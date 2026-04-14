#!/usr/bin/env python3
"""
Lightweight SRE monitor for the Roderick ecosystem.

Reads the SQLite DB and the local Docker API directly — no agent interference.
Emits a plaintext report to stdout and optionally writes to logs/monitor.log.

Usage:
  py scripts/monitor.py                   # one-shot report
  py scripts/monitor.py --watch 60        # poll every 60 seconds
  py scripts/monitor.py --json            # emit JSON instead of text
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# ── Config ────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = os.environ.get("RODERICK_DB", str(ROOT / "data" / "roderick.db"))
LOG_PATH = ROOT / "logs" / "monitor.log"
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
API_HOST = os.environ.get("RODERICK_API", "http://localhost:8000")
STUCK_THRESHOLD_MINUTES = int(os.environ.get("STUCK_THRESHOLD_MINUTES", "10"))

# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _minutes_ago(ts_str: str | None) -> float | None:
    if not ts_str:
        return None
    try:
        ts = datetime.fromisoformat(ts_str.rstrip("Z").replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds() / 60
    except Exception:
        return None


# ── Checks ────────────────────────────────────────────────────────────────────

def check_docker_health() -> dict:
    """Report container up/down and restart counts."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            cwd=str(ROOT), capture_output=True, timeout=10
        )
        if result.returncode != 0:
            return {"status": "error", "detail": result.stderr.decode("utf-8", errors="replace").strip()}
        stdout = result.stdout.decode("utf-8", errors="replace")
        containers = []
        for line in stdout.strip().splitlines():
            try:
                c = json.loads(line)
                containers.append({
                    "name": c.get("Name", "?"),
                    "service": c.get("Service", "?"),
                    "state": c.get("State", "?"),
                    "status": c.get("Status", "?"),
                    "health": c.get("Health", ""),
                })
            except json.JSONDecodeError:
                pass
        unhealthy = [c for c in containers if c["state"] not in ("running", "restarting")]
        return {
            "status": "ok" if not unhealthy else "warn",
            "containers": containers,
            "unhealthy": unhealthy,
        }
    except FileNotFoundError:
        return {"status": "skip", "detail": "docker not in PATH"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def check_agent_heartbeats() -> dict:
    """Identify agents that haven't heartbeated recently."""
    if not Path(DB_PATH).exists():
        return {"status": "skip", "detail": "DB not found"}
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT name, display_name, status, last_heartbeat FROM agent_registry"
        ).fetchall()
        agents = []
        stale = []
        for r in rows:
            age = _minutes_ago(r["last_heartbeat"])
            entry = {
                "name": r["name"],
                "status": r["status"],
                "last_heartbeat": r["last_heartbeat"],
                "minutes_ago": round(age, 1) if age is not None else None,
            }
            agents.append(entry)
            if age is None or age > 5:
                stale.append(entry)
        return {
            "status": "ok" if not stale else "warn",
            "agents": agents,
            "stale": stale,
        }
    finally:
        conn.close()


def check_stuck_tasks() -> dict:
    """Find tasks stuck in_progress longer than threshold."""
    if not Path(DB_PATH).exists():
        return {"status": "skip", "detail": "DB not found"}
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT id, to_agent, task_type, description, updated_at "
            "FROM tasks WHERE status='in_progress'"
        ).fetchall()
        stuck = []
        for r in rows:
            age = _minutes_ago(r["updated_at"])
            if age is not None and age > STUCK_THRESHOLD_MINUTES:
                stuck.append({
                    "id": r["id"],
                    "agent": r["to_agent"],
                    "type": r["task_type"],
                    "desc": r["description"][:80],
                    "minutes_ago": round(age, 1),
                })
        return {
            "status": "ok" if not stuck else "warn",
            "in_progress_count": len(rows),
            "stuck": stuck,
            "threshold_minutes": STUCK_THRESHOLD_MINUTES,
        }
    finally:
        conn.close()


def check_task_latency() -> dict:
    """Average and p95 completion time for tasks completed in last 24h."""
    if not Path(DB_PATH).exists():
        return {"status": "skip", "detail": "DB not found"}
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT to_agent, task_type, created_at, updated_at "
            "FROM tasks WHERE status='completed' AND updated_at > datetime('now', '-24 hours')"
        ).fetchall()
        if not rows:
            return {"status": "ok", "completed_24h": 0, "avg_minutes": None, "p95_minutes": None}
        durations = []
        by_agent: dict[str, list[float]] = {}
        for r in rows:
            try:
                start = datetime.fromisoformat(r["created_at"])
                end = datetime.fromisoformat(r["updated_at"])
                if start.tzinfo is None:
                    start = start.replace(tzinfo=timezone.utc)
                if end.tzinfo is None:
                    end = end.replace(tzinfo=timezone.utc)
                mins = (end - start).total_seconds() / 60
                if mins >= 0:
                    durations.append(mins)
                    by_agent.setdefault(r["to_agent"], []).append(mins)
            except Exception:
                pass
        durations.sort()
        p95_idx = max(0, int(len(durations) * 0.95) - 1)
        avg = round(sum(durations) / len(durations), 1) if durations else None
        p95 = round(durations[p95_idx], 1) if durations else None
        agent_avg = {
            a: round(sum(v) / len(v), 1) for a, v in by_agent.items()
        }
        return {
            "status": "ok",
            "completed_24h": len(rows),
            "avg_minutes": avg,
            "p95_minutes": p95,
            "by_agent": agent_avg,
        }
    finally:
        conn.close()


def check_ollama_models() -> dict:
    """Check which models are loaded in Ollama."""
    try:
        import urllib.request
        with urllib.request.urlopen(f"{OLLAMA_HOST}/api/ps", timeout=5) as resp:
            data = json.loads(resp.read())
        models = data.get("models", [])
        return {
            "status": "ok",
            "loaded_count": len(models),
            "models": [{"name": m.get("name"), "size_vram": m.get("size_vram")} for m in models],
        }
    except Exception as e:
        return {"status": "skip", "detail": str(e)}


def check_api_health() -> dict:
    """Ping the API and measure response time."""
    import urllib.request
    results = {}
    for path in ["/health", "/system/stats"]:
        t0 = time.monotonic()
        try:
            with urllib.request.urlopen(f"{API_HOST}{path}", timeout=5) as resp:
                resp.read()
                results[path] = {
                    "status": resp.status,
                    "ms": round((time.monotonic() - t0) * 1000),
                }
        except Exception as e:
            results[path] = {"status": "error", "detail": str(e)}
    overall = "ok" if all(v.get("status") == 200 for v in results.values()) else "warn"
    return {"status": overall, "endpoints": results}


def check_forge_sentinel_loop() -> dict:
    """Check whether the Forge→Sentinel improvement loop is flowing."""
    if not Path(DB_PATH).exists():
        return {"status": "skip", "detail": "DB not found"}
    conn = _connect()
    try:
        # Last improvement task
        last_improve = conn.execute(
            "SELECT id, status, updated_at FROM tasks "
            "WHERE task_type='system_improvement' ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        # Last sentinel validation
        last_validate = conn.execute(
            "SELECT id, status, updated_at FROM tasks "
            "WHERE task_type='validate_system_improvement' ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        # Blocked improvements (proposed, stuck)
        blocked = conn.execute(
            "SELECT COUNT(*) as n FROM system_improvements WHERE status NOT IN ('completed','rejected')"
        ).fetchone()
        return {
            "status": "ok",
            "last_improvement": dict(last_improve) if last_improve else None,
            "last_validation": dict(last_validate) if last_validate else None,
            "pipeline_blocked": blocked["n"] if blocked else 0,
        }
    except sqlite3.OperationalError:
        return {"status": "skip", "detail": "system_improvements table not yet created"}
    finally:
        conn.close()


def check_zuko_cadence() -> dict:
    """Check Zuko's last scan and whether it's running on schedule."""
    if not Path(DB_PATH).exists():
        return {"status": "skip", "detail": "DB not found"}
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT id, status, updated_at FROM tasks "
            "WHERE to_agent='zuko' ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return {"status": "warn", "detail": "No Zuko tasks found"}
        age = _minutes_ago(row["updated_at"])
        expected_hours = 3
        ok = age is not None and age < expected_hours * 60 * 1.5
        return {
            "status": "ok" if ok else "warn",
            "last_task_id": row["id"],
            "last_task_status": row["status"],
            "minutes_since_last": round(age, 1) if age else None,
            "expected_interval_hours": expected_hours,
        }
    finally:
        conn.close()


def check_pending_approvals() -> dict:
    """Count pending approvals and flag if stale (>2h)."""
    if not Path(DB_PATH).exists():
        return {"status": "skip", "detail": "DB not found"}
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT id, request_type, created_at FROM approval_requests WHERE status='pending'"
        ).fetchall()
        stale = []
        for r in rows:
            age = _minutes_ago(r["created_at"])
            if age and age > 120:
                stale.append({"id": r["id"], "type": r["request_type"], "hours": round(age / 60, 1)})
        return {
            "status": "ok" if not rows else ("warn" if stale else "info"),
            "pending_count": len(rows),
            "stale_over_2h": stale,
        }
    finally:
        conn.close()


# ── Report ────────────────────────────────────────────────────────────────────

CHECKS = [
    ("docker",       check_docker_health),
    ("heartbeats",   check_agent_heartbeats),
    ("stuck_tasks",  check_stuck_tasks),
    ("task_latency", check_task_latency),
    ("ollama",       check_ollama_models),
    ("api",          check_api_health),
    ("forge_loop",   check_forge_sentinel_loop),
    ("zuko",         check_zuko_cadence),
    ("approvals",    check_pending_approvals),
]

STATUS_ICON = {"ok": "[ok]", "warn": "[!!]", "error": "[XX]", "skip": "[--]", "info": "[ii]"}


def run_all() -> dict:
    results = {}
    for name, fn in CHECKS:
        try:
            results[name] = fn()
        except Exception as e:
            results[name] = {"status": "error", "detail": str(e)}
    return results


def format_report(results: dict) -> str:
    lines = [f"Roderick Monitor  {_now()}", "=" * 60]
    overall = "ok"
    for name, r in results.items():
        icon = STATUS_ICON.get(r.get("status", "?"), "?")
        status = r.get("status", "?")
        if status == "warn" and overall == "ok":
            overall = "warn"
        if status == "error":
            overall = "error"
        lines.append(f"  {icon}  {name:<18} {status}")

        # Per-check detail
        if status in ("warn", "error"):
            detail = r.get("detail", "")
            if detail:
                lines.append(f"         {detail}")

        if name == "heartbeats" and r.get("stale"):
            for a in r["stale"]:
                lines.append(f"         stale: {a['name']} ({a['minutes_ago']} min ago)")

        if name == "stuck_tasks" and r.get("stuck"):
            for t in r["stuck"]:
                lines.append(f"         stuck #{t['id']} {t['agent']} ({t['minutes_ago']} min) {t['desc']}")

        if name == "task_latency" and r.get("avg_minutes") is not None:
            lines.append(f"         avg {r['avg_minutes']} min · p95 {r['p95_minutes']} min · {r['completed_24h']} done/24h")
            for agent, avg in (r.get("by_agent") or {}).items():
                lines.append(f"         {agent:<14} {avg} min avg")

        if name == "ollama":
            for m in r.get("models", []):
                vram = f" ({round(m['size_vram']/1024/1024/1024, 1)} GB VRAM)" if m.get("size_vram") else ""
                lines.append(f"         loaded: {m['name']}{vram}")

        if name == "api" and r.get("endpoints"):
            for path, v in r["endpoints"].items():
                ms = v.get("ms", "?")
                st = v.get("status", "?")
                lines.append(f"         {path:<24} {st}  {ms}ms")

        if name == "forge_loop":
            last_i = r.get("last_improvement")
            last_v = r.get("last_validation")
            blocked = r.get("pipeline_blocked", 0)
            if last_i:
                lines.append(f"         last improvement: #{last_i['id']} {last_i['status']} @ {last_i['updated_at']}")
            if last_v:
                lines.append(f"         last validation:  #{last_v['id']} {last_v['status']} @ {last_v['updated_at']}")
            if blocked:
                lines.append(f"         pipeline blocked: {blocked} improvement(s)")

        if name == "zuko":
            lines.append(f"         last task #{r.get('last_task_id')} {r.get('last_task_status')} · {r.get('minutes_since_last')} min ago")

        if name == "approvals" and r.get("stale_over_2h"):
            for a in r["stale_over_2h"]:
                lines.append(f"         stale #{a['id']} {a['type']} ({a['hours']}h)")

    lines.append("=" * 60)
    overall_icon = STATUS_ICON.get(overall, "?")
    lines.append(f"  {overall_icon}  OVERALL: {overall.upper()}")
    return "\n".join(lines)


def main() -> None:
    # Ensure UTF-8 output on Windows
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Roderick ecosystem monitor")
    parser.add_argument("--watch", type=int, metavar="SECONDS", help="Poll interval in seconds")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    parser.add_argument("--log", action="store_true", help="Append to logs/monitor.log")
    args = parser.parse_args()

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    def run_once() -> None:
        results = run_all()
        if args.json:
            output = json.dumps({"ts": _now(), "checks": results}, indent=2)
        else:
            output = format_report(results)
        print(output)
        if args.log:
            with open(LOG_PATH, "a", encoding="utf-8") as f:
                f.write(output + "\n\n")

    if args.watch:
        try:
            while True:
                run_once()
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        run_once()


if __name__ == "__main__":
    main()
