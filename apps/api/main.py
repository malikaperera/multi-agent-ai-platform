"""
FastAPI backend for the Roderick dashboard.

This is intentionally thin: it reads and writes the same SQLite database and
context files used by Roderick instead of introducing a separate service
contract. That keeps Phase 5 aligned with the architecture in tasks/plan_v2.md.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import subprocess
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

import psutil

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from apps.roderick.core.agent_registry import AgentRegistryManager
from apps.roderick.core.memory import MemoryManager
from apps.roderick.core.orchestrator import Orchestrator
from apps.roderick.core.presence import PresenceManager, VALID_MODES
from shared.db.approvals import create_approval, resolve_approval
from shared.db.artifacts import get_forge_artifact, list_forge_artifacts
from shared.db.behavior import BehaviorPolicy, apply_policy, upsert_policy
from shared.db.connection import connect_sqlite
from shared.db.events import emit_event, get_unprocessed_events, mark_processed
from shared.db.improvements import (
    Improvement,
    advance_improvement,
    get_improvement,
    list_improvements,
    upsert_improvement,
)
from shared.db.messages import send_agent_message
from shared.db.schema import get_db_path, init_db, seed_db_if_needed
from shared.db.tasks import enqueue_task, update_task_status
from shared.graph.builder import build_memory_graph
from shared.llm.factory import build_llm
from shared.memory.founder import OwnerMemory
from shared.schemas.approval import ApprovalRequest
from shared.schemas.task import Task


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "config" / "roderick.json"


def _load_base_config() -> dict:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    data_dir = os.environ.get("DATA_DIR", config.get("data_dir", "data"))
    data_path = Path(data_dir)
    if not data_path.is_absolute():
        data_path = REPO_ROOT / data_path
    data_path.mkdir(parents=True, exist_ok=True)
    config["data_dir"] = str(data_path)
    db_dir = os.environ.get("DB_DIR", config.get("db_dir", config["data_dir"]))
    db_path = Path(db_dir)
    if not db_path.is_absolute():
        db_path = REPO_ROOT / db_path
    db_path.mkdir(parents=True, exist_ok=True)
    config["db_dir"] = str(db_path)
    memory_dir = os.environ.get("MEMORY_DIR", config.get("memory_dir", "memory"))
    memory_path = Path(memory_dir)
    if not memory_path.is_absolute():
        memory_path = REPO_ROOT / memory_path
    config["memory_dir"] = str(memory_path)
    return config


CONFIG = _load_base_config()
DB_PATH = get_db_path(CONFIG)
seed_db_if_needed(DB_PATH, os.environ.get("DB_SEED_PATH"))
init_db(DB_PATH)
_ORCHESTRATOR: Optional[Orchestrator] = None
_OPERATOR_LLM = None
_ZUKO_LLM = None
_OWNER_MEMORY: Optional[OwnerMemory] = None

app = FastAPI(title="Roderick Operator API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1|(?:\d{1,3}\.){3}\d{1,3})(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TaskCreate(BaseModel):
    to_agent: str
    task_type: str
    description: str
    from_agent: str = "dashboard"
    status: str = "pending"
    priority: str = "normal"
    urgency: str = "this_week"
    domain: str = "operations"
    payload: dict[str, Any] = Field(default_factory=dict)
    approval_required: bool = False


class PresenceUpdate(BaseModel):
    mode: str


class RoderickMessage(BaseModel):
    message: str = Field(min_length=1, max_length=4000)


class ApprovalAction(BaseModel):
    action: str


class AtlasLearningEntry(BaseModel):
    topic: str = Field(min_length=1, max_length=300)
    status: str = "completed"
    type: str = "linkedin_learning"
    note: str = ""
    linkedin_certificate_url: str = ""
    linkedin_learning_url: str = ""
    shareable_outcome: str = ""


class ControlAction(BaseModel):
    action: str
    task_id: Optional[int] = None
    artifact_id: Optional[int] = None
    agent: Optional[str] = None
    service: Optional[str] = None
    reason: str = ""


class AgentChatMessage(BaseModel):
    message: str = Field(min_length=1, max_length=4000)


def _connect() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = connect_sqlite(DB_PATH, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    return conn


def _json(value: Optional[str], fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _timestamp(value: Any) -> str:
    return str(value or "")


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def _row(row: sqlite3.Row) -> dict:
    data = dict(row)
    for key, fallback in (
        ("payload", {}),
        ("result", None),
        ("metadata", {}),
        ("config", {}),
        ("task_types_accepted", []),
        ("report_types_produced", []),
    ):
        if key in data:
            data[key] = _json(data[key], fallback)
    return data


def _task_stage(task: dict) -> str:
    status = str(task.get("status") or "unknown")
    result = task.get("result") or {}
    deployment = result.get("deployment") if isinstance(result.get("deployment"), dict) else {}
    if status == "live" and deployment.get("state") == "deploying":
        return "deploying_live"
    if status == "live" and deployment.get("state") == "deployed":
        return "deployed_live"
    mapping = {
        "pending": "awaiting_first_approval",
        "approved": "planning",
        "plan_ready": "awaiting_plan_approval",
        "plan_approved": "implementing",
        "in_progress": "active",
        "awaiting_validation": "awaiting_sentinel",
        "completed": "completed",
        "live": "promoted_live",
        "failed": "blocked",
        "rejected": "rejected",
        "cancelled": "cancelled",
    }
    return mapping.get(status, status)


def _forge_mode(task: dict) -> str:
    task_type = str(task.get("task_type") or "")
    payload = task.get("payload") or {}
    if task_type == "system_improvement":
        return "repo_patch"
    if payload.get("approval_policy") == "markdown_artifact_auto":
        return "artifact_markdown"
    if task_type in {"build", "build_feature"}:
        return "artifact_build"
    return "general"


def _task_output_summary(task: dict) -> dict:
    result = task.get("result") or {}
    payload = task.get("payload") or {}
    files_created = result.get("files_created") or payload.get("files_created") or []
    patches_applied = result.get("patches_applied") or payload.get("patches_applied") or []
    deployment = result.get("deployment") if isinstance(result.get("deployment"), dict) else {}
    return {
        "stage": _task_stage(task),
        "forge_mode": _forge_mode(task) if task.get("to_agent") == "forge" else None,
        "files_created": files_created,
        "patches_applied": patches_applied,
        "sentinel_task_id": result.get("sentinel_task_id") or payload.get("sentinel_task_id"),
        "artifact_root": result.get("_artifact_root") or payload.get("_artifact_root"),
        "artifact_files_dir": result.get("_artifact_files_dir") or payload.get("_artifact_files_dir"),
        "validation_state": "awaiting_validation" if task.get("status") == "awaiting_validation" else result.get("validation_state"),
        "deployment": deployment,
    }


def _agent_chat_thread(agent: str, limit: int = 40) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """SELECT id, from_agent, to_agent, message, priority, read, created_at
               FROM agent_messages
               WHERE (from_agent=? AND to_agent='dashboard')
                  OR (from_agent='dashboard' AND to_agent=?)
               ORDER BY created_at DESC LIMIT ?""",
            (agent, agent, limit),
        ).fetchall()
    thread = [dict(row) for row in rows]
    thread.reverse()
    return thread


def _load_task_report(agent: str, task_id: int) -> Optional[dict]:
    reports_dir = Path(CONFIG["data_dir"]) / "reports"
    candidates = [
        reports_dir / f"{agent}_{task_id}.json",
        reports_dir / f"{agent}_{task_id}_plan.json",
        reports_dir / f"{agent}_{task_id}_sysimprovement_plan.json",
    ]
    for path in candidates:
        if path.exists():
            try:
                return {
                    "path": str(path),
                    "content": json.loads(path.read_text(encoding="utf-8")),
                    "updated_at": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
                }
            except Exception as exc:
                return {
                    "path": str(path),
                    "content": {"error": str(exc)},
                    "updated_at": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
                }
    return None


def _open_merlin_diagnostic(conn: sqlite3.Connection, agent: str, source_task_id: Optional[int]) -> Optional[dict]:
    if source_task_id is not None:
        row = conn.execute(
            """SELECT * FROM tasks
               WHERE to_agent='merlin'
                 AND task_type='agent_diagnostics'
                 AND status IN ('pending','in_progress','approved')
                 AND payload LIKE ?
               ORDER BY created_at DESC LIMIT 1""",
            (f'%"task_id": {source_task_id}%',),
        ).fetchone()
        if row:
            return _row(row)
        return None
    row = conn.execute(
        """SELECT * FROM tasks
           WHERE to_agent='merlin'
             AND task_type='agent_diagnostics'
             AND status IN ('pending','in_progress','approved')
             AND payload LIKE ?
           ORDER BY created_at DESC LIMIT 1""",
        (f'%"agent": "{agent}"%',),
    ).fetchone()
    return _row(row) if row else None


def _read_text_tail(path: Path, max_chars: int = 6000) -> str:
    if not path.exists() or not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[-max_chars:]


def _summary_from_report(report: Any) -> str:
    if isinstance(report, dict):
        for key in (
            "summary",
            "opportunity_summary",
            "title",
            "recommendation",
            "status",
            "error",
        ):
            value = report.get(key)
            if value:
                return str(value)[:240]
        if report:
            return json.dumps(report, ensure_ascii=True)[:240]
    if isinstance(report, str):
        return report[:240]
    return ""


def _recent_agent_reports(agent_name: str, limit: int = 8) -> list[dict]:
    reports_dir = Path(CONFIG["data_dir"]) / "reports"
    if not reports_dir.exists():
        return []
    reports: list[dict] = []
    for path in sorted(reports_dir.glob(f"{agent_name}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            report = {"error": str(exc)}
        reports.append({
            "file": path.name,
            "path": str(path),
            "updated_at": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
            "summary": _summary_from_report(report),
        })
    return reports


def _system_snapshot() -> dict:
    proc = psutil.Process()
    mem = proc.memory_info()
    vm = psutil.virtual_memory()
    disk = psutil.disk_usage("/app/data") if Path("/app/data").exists() else psutil.disk_usage("/")
    return {
        "scope": "api_container_host_visible",
        "process_cpu_percent": round(proc.cpu_percent(interval=0.0), 1),
        "process_memory_rss_mb": round(mem.rss / 1024 / 1024, 1),
        "process_threads": proc.num_threads(),
        "host_cpu_percent": round(psutil.cpu_percent(interval=0.1), 1),
        "host_memory_percent": round(vm.percent, 1),
        "disk_percent": round(disk.percent, 1),
        "evidence": "verified from psutil inside the API container",
    }


def _query_ollama_ps() -> dict:
    base_url = os.environ.get("OLLAMA_BASE_URL", "http://host.docker.internal:11434").rstrip("/")
    try:
        req = Request(f"{base_url}/api/ps", headers={"Accept": "application/json"})
        with urlopen(req, timeout=4) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        models = data.get("models", []) if isinstance(data, dict) else []
        return {
            "status": "verified",
            "base_url": base_url,
            "loaded_models": [
                {
                    "name": m.get("name") or m.get("model"),
                    "size_vram": m.get("size_vram"),
                    "size": m.get("size"),
                    "processor": m.get("processor") or _ollama_processor_hint(m),
                    "expires_at": m.get("expires_at"),
                }
                for m in models
            ],
        }
    except Exception as exc:
        return {
            "status": "unknown",
            "base_url": base_url,
            "error": str(exc)[:240],
            "loaded_models": [],
        }


def _ollama_processor_hint(model: dict) -> str:
    size = model.get("size") or 0
    size_vram = model.get("size_vram") or 0
    try:
        if not size or not size_vram:
            return "processor unknown"
        gpu_pct = round(float(size_vram) / float(size) * 100)
        return f"estimated {100 - gpu_pct}%/{gpu_pct}% CPU/GPU"
    except Exception:
        return "processor unknown"


def _query_gpu_snapshot() -> dict:
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout or "nvidia-smi unavailable").strip())
        gpus = []
        for line in result.stdout.splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 6:
                continue
            name, util, mem_used, mem_total, temp, power = parts[:6]
            mem_used_f = float(mem_used)
            mem_total_f = float(mem_total) if float(mem_total) else 0.0
            gpus.append({
                "name": name,
                "utilization_percent": float(util),
                "memory_used_mb": mem_used_f,
                "memory_total_mb": mem_total_f,
                "memory_percent": round((mem_used_f / mem_total_f * 100), 1) if mem_total_f else 0,
                "temperature_c": float(temp),
                "power_draw_w": None if power in {"[N/A]", "N/A"} else float(power),
            })
        return {
            "status": "verified" if gpus else "unknown",
            "evidence": "nvidia-smi inside the API runtime",
            "gpus": gpus,
        }
    except Exception as exc:
        return {
            "status": "unknown",
            "evidence": "nvidia-smi was not available inside the API runtime",
            "error": str(exc)[:240],
            "gpus": [],
        }


def _ollama_gpu_residency(ollama: dict) -> Optional[dict]:
    models = ollama.get("loaded_models") or []
    estimates: list[dict] = []
    for model in models:
        size = model.get("size") or 0
        size_vram = model.get("size_vram") or 0
        try:
            if not size or not size_vram:
                continue
            gpu_pct = round(float(size_vram) / float(size) * 100, 1)
            estimates.append({
                "model": model.get("name") or model.get("model") or "unknown",
                "gpu_residency_percent": gpu_pct,
                "cpu_residency_percent": round(100 - gpu_pct, 1),
            })
        except Exception:
            continue
    if not estimates:
        return None
    return {
        "status": "inferred",
        "residency_percent": max(item["gpu_residency_percent"] for item in estimates),
        "evidence": "inferred from Ollama loaded model size_vram/size; this is GPU residency, not live utilization",
        "models": estimates,
    }


def _set_agent_runtime_state(agent: str, state: str, reason: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    policy = upsert_policy(DB_PATH, BehaviorPolicy(
        agent=agent,
        policy_key="runtime_state",
        policy_value=state,
        description=f"Dashboard operator set {agent} runtime state to {state}.",
        status="applied",
        origin="dashboard",
        changed_by="dashboard",
        requires_approval=False,
        approved_by="dashboard",
        applied_at=now,
        audit_notes=reason,
    ))
    apply_policy(DB_PATH, agent, "runtime_state", approved_by="dashboard")
    emit_event(DB_PATH, "dashboard_agent_runtime_state_changed", agent, {
        "agent": agent,
        "runtime_state": state,
        "reason": reason,
        "policy_id": policy.id,
    })
    return {
        "status": "done",
        "message": f"{agent} runtime state set to {state}. Workers will honor this before taking new tasks.",
        "evidence": f"agent_behavior_policies#{policy.id}",
    }


def _enrich_approval(row: sqlite3.Row, conn: sqlite3.Connection) -> dict:
    approval = _row(row)
    task = None
    if approval.get("task_id"):
        task_row = conn.execute("SELECT * FROM tasks WHERE id=?", (approval["task_id"],)).fetchone()
        task = _row(task_row) if task_row else None
    payload = approval.get("payload") or {}
    improvement = None
    if payload.get("improvement_id"):
        imp_row = conn.execute("SELECT * FROM improvements WHERE id=?", (payload["improvement_id"],)).fetchone()
        improvement = _row(imp_row) if imp_row else None
    events = []
    if task:
        events = [
            {**dict(r), "payload": _json(r["payload"], {})}
            for r in conn.execute(
                """SELECT id, event_type, agent, payload, created_at
                   FROM events
                   WHERE payload LIKE ?
                   ORDER BY created_at DESC LIMIT 8""",
                (f'%"task_id": {task["id"]}%',),
            ).fetchall()
        ]
    approval["task"] = task
    approval["improvement"] = improvement
    approval["evidence_events"] = events
    approval["decision_packet"] = _approval_decision_packet(approval, task, improvement, events)
    return approval


def _approval_decision_packet(
    approval: dict,
    task: Optional[dict],
    improvement: Optional[dict],
    events: list[dict],
) -> dict:
    payload = approval.get("payload") or {}
    risks: list[str] = []
    checks: list[str] = []
    if approval.get("request_type") == "task_approval":
        checks.append("Approving moves the linked task into the approved queue.")
        risks.append("Declining leaves the linked work unstarted unless another agent proposes a better route.")
    if approval.get("request_type") == "plan_approval":
        checks.append("Approving allows Forge to implement the approved plan.")
        risks.append("Review target paths and scope before approving implementation.")
        risks.append("Declining keeps the current issue unresolved, but avoids applying a plan you do not trust.")
    if approval.get("request_type") == "sentinel_approval":
        checks.append("Sentinel validation is the recorded promotion gate.")
        risks.append("Declining prevents promotion/deployment of the validated change.")
    if improvement:
        risks.append(f"Improvement risk: {improvement.get('risk_level', 'unknown')}")
    if task and task.get("approval_required"):
        checks.append("Task is marked approval_required in the task table.")
    if payload:
        checks.append("Approval has structured payload evidence.")
    unknowns = [
        item for item in (
            "No linked task row was found." if approval.get("task_id") and not task else "",
            "No Sentinel evidence is attached yet." if approval.get("request_type") != "sentinel_approval" else "",
        ) if item
    ]
    return {
        "why": _approval_why(approval, task, improvement),
        "if_declined": _approval_if_declined(approval, task, improvement),
        "verified": [
            item for item in (
                f"Approval row #{approval.get('id')} is pending.",
                f"Linked task #{task.get('id')} exists." if task else "",
                f"Linked improvement #{improvement.get('id')} exists." if improvement else "",
                f"{len(events)} task-related event(s) found." if events else "",
            ) if item
        ],
        "risks": risks,
        "checks": checks,
        "unknowns": unknowns,
    }


def _approval_why(approval: dict, task: Optional[dict], improvement: Optional[dict]) -> str:
    if improvement:
        return f"Improvement requested: {improvement.get('title') or improvement.get('origin_signal') or 'system improvement'}."
    if task:
        return f"{task.get('from_agent', 'an agent')} is asking to move task #{task.get('id')} ({task.get('task_type')}) forward."
    payload = approval.get("payload") or {}
    if payload.get("reason"):
        return str(payload["reason"])[:300]
    return "An agent requested a gated action that requires your approval."


def _approval_if_declined(approval: dict, task: Optional[dict], improvement: Optional[dict]) -> str:
    request_type = approval.get("request_type", "")
    if request_type == "plan_approval":
        return "Forge will not implement the plan; the underlying problem will remain until a revised plan is proposed."
    if request_type == "sentinel_approval":
        return "The validated change will not be promoted; the current system state remains as-is."
    if task:
        return f"Task #{task.get('id')} will not advance from its current status ({task.get('status', 'unknown')})."
    if improvement:
        return "The improvement stays unapproved and will not be applied automatically."
    return "The requested action will not be performed."


def _get_orchestrator() -> Orchestrator:
    global _ORCHESTRATOR
    if _ORCHESTRATOR is None:
        registry = AgentRegistryManager(DB_PATH, str(REPO_ROOT / "config" / "agents.json"))
        registry.sync_from_config()
        roderick_cfg = CONFIG.get("roderick", {})
        control_model = roderick_cfg.get("control_model", CONFIG.get("llm", {}).get("ollama_model", "qwen3:14b"))
        coordinator_model = roderick_cfg.get("coordinator_model", control_model)
        _ORCHESTRATOR = Orchestrator(
            llm=build_llm(CONFIG, model=control_model),
            db_path=DB_PATH,
            memory=MemoryManager(CONFIG["data_dir"]),
            registry=registry,
            config=CONFIG,
            owner_memory=OwnerMemory(CONFIG["memory_dir"]),
            coordinator_llm=build_llm(CONFIG, model=coordinator_model),
        )
    return _ORCHESTRATOR


def _get_owner_memory() -> OwnerMemory:
    global _OWNER_MEMORY
    if _OWNER_MEMORY is None:
        _OWNER_MEMORY = OwnerMemory(CONFIG["memory_dir"])
    return _OWNER_MEMORY


def _get_operator_llm():
    global _OPERATOR_LLM
    if _OPERATOR_LLM is None:
        operator_cfg = CONFIG.get("operator", {})
        model = operator_cfg.get("model") or CONFIG.get("llm", {}).get("ollama_model", "qwen3:14b")
        _OPERATOR_LLM = build_llm(CONFIG, model=model)
    return _OPERATOR_LLM


def _get_zuko_llm():
    global _ZUKO_LLM
    if _ZUKO_LLM is None:
        zuko_cfg = CONFIG.get("zuko", {})
        model = zuko_cfg.get("model") or CONFIG.get("llm", {}).get("ollama_model", "qwen3:14b")
        _ZUKO_LLM = build_llm(CONFIG, model=model)
    return _ZUKO_LLM


def _recent_agent_reports(agent: str, limit: int = 3) -> list[dict]:
    reports_dir = Path(CONFIG["data_dir"]) / "reports"
    if not reports_dir.exists():
        return []
    out: list[dict] = []
    for path in sorted(reports_dir.glob(f"{agent}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        try:
            out.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            continue
    return out


def _respond_as_operator(message: str) -> str:
    owner_memory = _get_owner_memory()
    prompt = (
        "You are Operator, the owner's business execution operator.\n"
        "Reply in clean, direct prose with short sections if helpful.\n"
        "Do not return JSON. Do not act like a generic assistant.\n"
        "Acknowledge what is already in motion, note any approvals/blockers, and name the next concrete action.\n\n"
        f"Owner context:\n{owner_memory.get_context()[:2500]}\n\n"
        f"Business context:\n{owner_memory.get_business_context()[:2500]}\n\n"
        f"Dashboard message:\n{message}"
    )
    try:
        response = _get_operator_llm().complete(
            messages=[{"role": "user", "content": prompt}],
            system=(
                "You are Operator. Reply in plain English only. "
                "Use concise execution language, mention approvals if needed, and never return JSON."
            ),
            name="operator_dashboard_chat",
        )
    except Exception:
        return "Operator logged that, but the live reply path hit an internal error. The request is still preserved in the dashboard thread."
    cleaned = (response or "").strip()
    return cleaned or "Operator logged that and is ready to move it into the execution lane when you want."


def _respond_as_zuko(message: str) -> str:
    owner_memory = _get_owner_memory()
    recent_reports = _recent_agent_reports("zuko", limit=2)
    prompt = (
        "You are Zuko, the owner's job-search and application-tracking agent.\n"
        "Reply in clean, direct prose with short sections if helpful.\n"
        "Do not return JSON.\n"
        "Be concrete: shortlist, application status, recruiter signal, next step, and what Merlin should look into if relevant.\n\n"
        f"Owner context:\n{owner_memory.get_context()[:2200]}\n\n"
        f"Job market context:\n{owner_memory.get_job_market_context()[:2200]}\n\n"
        f"Recent Zuko reports:\n{json.dumps(recent_reports, ensure_ascii=False)[:2200]}\n\n"
        f"Dashboard message:\n{message}"
    )
    try:
        response = _get_zuko_llm().complete(
            messages=[{"role": "user", "content": prompt}],
            system=(
                "You are Zuko. Reply in plain English only. "
                "Be concrete and career-action oriented. "
                "Never return JSON. Mention shortlist/application tracking when useful."
            ),
            name="zuko_dashboard_chat",
        )
    except Exception:
        return "Zuko logged that, but the live reply path hit an internal error. The message is still preserved in the dashboard thread."
    cleaned = (response or "").strip()
    return cleaned or "Zuko logged that and is ready to tighten the shortlist, applications, or recruiter follow-up."


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/roderick/message")
async def send_roderick_message(payload: RoderickMessage) -> dict:
    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")

    emit_event(DB_PATH, "dashboard_message_received", "dashboard", {"message": message[:500]})
    try:
        response = await asyncio.wait_for(_get_orchestrator().handle(message), timeout=120)
    except asyncio.TimeoutError:
        response = (
            "<b>Still working.</b>\n\n"
            "Roderick did not finish within the dashboard timeout. "
            "Check the dashboard tasks and activity log for verified progress."
        )
        emit_event(DB_PATH, "dashboard_message_timeout", "roderick", {"message": message[:500]})
    except Exception as exc:
        emit_event(DB_PATH, "dashboard_message_error", "roderick", {"message": message[:500], "error": str(exc)[:500]})
        raise HTTPException(status_code=500, detail=str(exc))

    emit_event(DB_PATH, "dashboard_message_response", "roderick", {"message": message[:300], "response": response[:1000]})
    return {
        "message": message,
        "response": response,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/agents")
def list_agents() -> list[dict]:
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM agent_registry ORDER BY name ASC").fetchall()
        active_rows = conn.execute(
            "SELECT to_agent, id, task_type, description, status, updated_at FROM tasks WHERE status='in_progress'"
        ).fetchall()
    active_by_agent = {r["to_agent"]: dict(r) for r in active_rows}
    agents = [_row(row) for row in rows]
    for agent in agents:
        active = active_by_agent.get(agent["name"])
        agent["active_task"] = active
        if not active and agent.get("current_task_id"):
            agent["stale_current_task_id"] = agent["current_task_id"]
            agent["current_task_id"] = None
            if agent.get("status") == "busy":
                agent["status"] = "stale_busy"
                agent["state_evidence"] = "inferred: registry was busy but no in_progress task row exists"
    return agents


@app.get("/agents/{name}")
def get_agent(name: str) -> dict:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM agent_registry WHERE name=?", (name,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="agent not found")
    agent = _row(row)
    # Attach current in-progress task
    with _connect() as conn:
        active = conn.execute(
            "SELECT id, task_type, description, status, created_at, updated_at "
            "FROM tasks WHERE to_agent=? AND status='in_progress' ORDER BY updated_at DESC LIMIT 1",
            (name,),
        ).fetchone()
        recent = conn.execute(
            "SELECT id, task_type, description, status, priority, urgency, domain, created_at, updated_at "
            "FROM tasks WHERE to_agent=? ORDER BY created_at DESC LIMIT 10",
            (name,),
        ).fetchall()
        counts = conn.execute(
            "SELECT status, COUNT(*) as n FROM tasks WHERE to_agent=? GROUP BY status",
            (name,),
        ).fetchall()
        intentions = conn.execute(
            """SELECT id, task_type, description, status, priority, urgency, domain, created_at, updated_at
               FROM tasks
               WHERE to_agent=? AND status IN ('pending','approved','plan_ready','plan_approved','awaiting_validation')
               ORDER BY
                 CASE status
                   WHEN 'plan_approved' THEN 0
                   WHEN 'approved' THEN 1
                   WHEN 'pending' THEN 2
                   ELSE 3
                 END,
                 created_at DESC
               LIMIT 12""",
            (name,),
        ).fetchall()
        events = conn.execute(
            "SELECT id, event_type, agent, payload, created_at FROM events WHERE agent=? ORDER BY created_at DESC LIMIT 25",
            (name,),
        ).fetchall()
        messages = conn.execute(
            """SELECT id, from_agent, to_agent, message, priority, read, created_at
               FROM agent_messages
               WHERE from_agent=? OR to_agent=?
               ORDER BY created_at DESC LIMIT 25""",
            (name, name),
        ).fetchall()
    agent["active_task"] = dict(active) if active else None
    if not active and agent.get("current_task_id"):
        agent["stale_current_task_id"] = agent["current_task_id"]
        agent["current_task_id"] = None
    agent["recent_tasks"] = [dict(r) for r in recent]
    agent["task_counts"] = {r["status"]: r["n"] for r in counts}
    agent["intentions"] = [dict(r) for r in intentions]
    agent["recent_events"] = [
        {**dict(r), "payload": _json(r["payload"], {})}
        for r in events
    ]
    agent["recent_messages"] = [dict(r) for r in messages]
    agent["recent_reports"] = _recent_agent_reports(name)
    agent["learning_note"] = _read_text_tail(Path(CONFIG["data_dir"]) / "agent_learning" / f"{name}.md")
    agent["resource_snapshot"] = _system_snapshot()
    if active:
        agent["stage"] = f"{active['task_type']} / {active['status']}"
        agent["state_evidence"] = "verified: active task row exists"
    elif agent.get("status") == "busy":
        agent["stage"] = "busy / active task unknown"
        agent["state_evidence"] = "inferred: registry heartbeat says busy but no in_progress task row was found"
    else:
        agent["stage"] = agent.get("status") or "unknown"
        agent["state_evidence"] = "verified: registry row and no active task row"
    # Inject per-call model routing info from agents.json where configured.
    if name in {"forge", "merlin", "venture"}:
        try:
            agents_data = json.loads(
                (Path(__file__).resolve().parents[2] / "config" / "agents.json").read_text(encoding="utf-8")
            )
            for a in agents_data.get("agents", []):
                if a["name"] != name:
                    continue
                if name == "forge":
                    agent["planner_model"] = a.get("planner_model", a.get("model_used", ""))
                    agent["coder_model"] = a.get("coder_model", "")
                if name == "merlin":
                    agent["research_model"] = a.get("research_model", a.get("model_used", ""))
                    agent["diagnostic_model"] = a.get("diagnostic_model", "")
                if name == "venture":
                    agent["deep_model"] = a.get("deep_model", a.get("model_used", ""))
                    agent["routine_model"] = a.get("routine_model", "")
                break
        except Exception:
            pass
    return agent


@app.get("/system/stats")
def system_stats() -> dict:
    """DB-level stats: task counts, event counts, agent health summary."""
    with _connect() as conn:
        task_by_status = {
            r["status"]: r["n"]
            for r in conn.execute("SELECT status, COUNT(*) as n FROM tasks GROUP BY status").fetchall()
        }
        task_by_agent = {
            r["to_agent"]: r["n"]
            for r in conn.execute("SELECT to_agent, COUNT(*) as n FROM tasks GROUP BY to_agent").fetchall()
        }
        event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        pending_approvals = conn.execute(
            "SELECT COUNT(*) FROM approval_requests WHERE status='pending'"
        ).fetchone()[0]
        agents_busy = conn.execute(
            "SELECT COUNT(*) FROM agent_registry WHERE status='busy'"
        ).fetchone()[0]
        agents_idle = conn.execute(
            "SELECT COUNT(*) FROM agent_registry WHERE status='idle'"
        ).fetchone()[0]
    return {
        "tasks_by_status": task_by_status,
        "tasks_by_agent": task_by_agent,
        "total_events": event_count,
        "pending_approvals": pending_approvals,
        "agents_busy": agents_busy,
        "agents_idle": agents_idle,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/memory/graph")
def memory_graph(
    window_hours: int = Query(24, ge=1, le=168),
    agent: Optional[str] = None,
    limit: int = Query(200, ge=25, le=500),
) -> dict:
    """Evidence-backed graph of agents, tasks, reports, approvals, policies, and artifacts."""
    return build_memory_graph(
        DB_PATH,
        CONFIG["data_dir"],
        window_hours=window_hours,
        agent_filter=agent,
        limit=limit,
    )


@app.get("/operator/recommendations")
def operator_recommendations(limit: int = Query(8, ge=1, le=20)) -> dict:
    """Deterministic control-plane recommendations from verified local state."""
    now = datetime.now(timezone.utc)
    recs: list[dict] = []
    with _connect() as conn:
        stuck = conn.execute(
            """SELECT * FROM tasks
               WHERE status='in_progress'
               ORDER BY updated_at ASC LIMIT 20"""
        ).fetchall()
        for row in stuck:
            task = _row(row)
            updated = _parse_datetime(task.get("updated_at"))
            age_min = int((now - updated).total_seconds() / 60) if updated else None
            if age_min is None or age_min < 10:
                continue
            existing_diag = _open_merlin_diagnostic(conn, task["to_agent"], task["id"])
            recs.append({
                "id": f"stuck_task_{task['id']}",
                "severity": "warn" if age_min < 45 else "danger",
                "title": f"Task #{task['id']} may be stuck",
                "summary": f"{task['to_agent']} has been in progress for about {age_min} minutes.",
                "why": "The task is still in_progress and has not updated recently.",
                "action_label": f"View Merlin task #{existing_diag['id']}" if existing_diag else "Ask Merlin to diagnose",
                "action": {"action": "open_tab", "tab": "Tasks"} if existing_diag else {"action": "diagnose_agent", "agent": task["to_agent"], "task_id": task["id"]},
                "evidence": f"tasks#{task['id']}",
            })

        for row in conn.execute(
            "SELECT * FROM agent_registry WHERE status IN ('failed','offline') ORDER BY updated_at DESC LIMIT 10"
        ).fetchall():
            agent = _row(row)
            recs.append({
                "id": f"agent_{agent['name']}_{agent['status']}",
                "severity": "danger",
                "title": f"{agent['display_name']} is {agent['status']}",
                "summary": agent.get("last_error") or agent.get("last_message") or "No recent detail recorded.",
                "why": "Agent registry marks this agent as not healthy.",
                "action_label": "Run Sentinel check",
                "action": {"action": "sentinel_check", "agent": agent["name"]},
                "evidence": f"agent_registry#{agent['id']}",
            })

        pending = conn.execute("SELECT COUNT(*) FROM approval_requests WHERE status='pending'").fetchone()[0]
        if pending:
            recs.append({
                "id": "pending_approvals",
                "severity": "info",
                "title": f"{pending} approval request(s) need review",
                "summary": "Review the approval queue before Forge/Sentinel promotion work can continue.",
                "why": "Pending approval rows are waiting for human decision.",
                "action_label": "Open Approvals",
                "action": {"action": "open_tab", "tab": "Approvals"},
                "evidence": "approval_requests status=pending",
            })

        proposed = conn.execute("SELECT COUNT(*) FROM agent_behavior_policies WHERE status='proposed'").fetchone()[0]
        if proposed:
            recs.append({
                "id": "proposed_behaviors",
                "severity": "info",
                "title": f"{proposed} behavior policy proposal(s)",
                "summary": "Roderick has proposed runtime behavior changes that need review.",
                "why": "Proposed agent_behavior_policies rows are waiting for action.",
                "action_label": "Open Behaviors",
                "action": {"action": "open_tab", "tab": "Behaviors"},
                "evidence": "agent_behavior_policies status=proposed",
            })

    try:
        disk = psutil.disk_usage(str(Path(CONFIG["data_dir"]).anchor or REPO_ROOT))
        if disk.percent > 85:
            recs.append({
                "id": "disk_pressure",
                "severity": "danger",
                "title": f"Disk pressure: {round(disk.percent)}%",
                "summary": "Storage pressure can break SQLite, Docker builds, model downloads, and logs.",
                "why": "Host disk usage is above the operational threshold.",
                "action_label": "Ask Sentinel to inspect storage",
                "action": {"action": "sentinel_check", "agent": "sentinel", "reason": "storage pressure"},
                "evidence": "psutil.disk_usage",
            })
    except Exception:
        pass

    return {
        "items": recs[:limit],
        "generated_at": now.isoformat(),
        "evidence": "verified from SQLite agent/task/approval/policy state plus host metrics where available",
    }


@app.get("/operator/initiatives")
def operator_initiatives() -> dict:
    """
    Return structured initiative list from memory/initiatives.md and recent operator reports.
    """
    memory_dir = Path(CONFIG.get("memory_dir", "memory"))
    initiatives_path = memory_dir / "initiatives.md"
    business_path = memory_dir / "business_ops.md"

    def _read_md(path: Path) -> str:
        try:
            content = path.read_text(encoding="utf-8")
            # Strip YAML front-matter
            if content.startswith("---"):
                end = content.find("---", 3)
                if end != -1:
                    content = content[end + 3:].strip()
            return content.strip()
        except FileNotFoundError:
            return ""
        except Exception:
            return ""

    initiatives_raw = _read_md(initiatives_path)
    business_ops_raw = _read_md(business_path)

    # Parse initiative blocks: lines starting with "## INITIATIVE-"
    initiatives = []
    if initiatives_raw:
        current: dict = {}
        for line in initiatives_raw.splitlines():
            if line.startswith("## INITIATIVE-") or line.startswith("## INITIATIVE BACKLOG"):
                if current:
                    initiatives.append(current)
                if "BACKLOG" in line:
                    break
                title = line[3:].strip()
                current = {"title": title, "status": "", "priority": "", "next_actions": [], "blockers": []}
            elif current:
                ls = line.strip()
                if ls.startswith("**Status:**"):
                    current["status"] = ls.split("**Status:**", 1)[1].strip()
                elif ls.startswith("**Priority:**"):
                    current["priority"] = ls.split("**Priority:**", 1)[1].strip()
                elif ls.startswith("**Objective**"):
                    pass
                elif ls.startswith("- [ ]") or ls.startswith("- [x]"):
                    pass  # checklist items — skip for summary
        if current and "title" in current:
            initiatives.append(current)

    # Enrich with recent operator reports
    data_dir = Path(CONFIG["data_dir"])
    reports_dir = data_dir / "reports"
    recent_reports = []
    if reports_dir.exists():
        op_reports = sorted(
            reports_dir.glob("operator_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )[:5]
        for rp in op_reports:
            try:
                rdata = json.loads(rp.read_text(encoding="utf-8"))
                recent_reports.append({
                    "task_id": rdata.get("_task_id"),
                    "task_type": rdata.get("_task_type"),
                    "initiative": rdata.get("initiative", ""),
                    "status": rdata.get("status", ""),
                    "task_summary": rdata.get("task_summary", "")[:200],
                    "approval_required": rdata.get("approval_required", False),
                    "executed_at": rdata.get("_executed_at", ""),
                })
            except Exception:
                pass

    return {
        "initiatives": initiatives,
        "recent_reports": recent_reports,
        "business_ops_summary": business_ops_raw[:2000] if business_ops_raw else "",
        "source": "memory/initiatives.md + data/reports/operator_*.json",
    }


@app.get("/operator/pending")
def operator_pending() -> dict:
    """Return pending approvals and blocked operator tasks."""
    with _connect() as conn:
        pending_approvals = [
            _enrich_approval(row, conn)
            for row in conn.execute(
                """SELECT * FROM approval_requests
                   WHERE status='pending' AND request_type LIKE '%operator%'
                   ORDER BY created_at DESC LIMIT 20"""
            ).fetchall()
        ]
        blocked_tasks = [
            _row(row)
            for row in conn.execute(
                """SELECT * FROM tasks
                   WHERE to_agent='operator' AND status IN ('pending','in_progress','blocked')
                   ORDER BY created_at DESC LIMIT 20"""
            ).fetchall()
        ]
    return {
        "pending_approvals": pending_approvals,
        "blocked_tasks": blocked_tasks,
        "source": "approval_requests + tasks tables",
    }


@app.get("/agents/{agent}/messages")
def agent_messages(agent: str, limit: int = Query(40, ge=1, le=200)) -> dict:
    agent = agent.strip().lower()
    return {
        "agent": agent,
        "messages": _agent_chat_thread(agent, limit),
        "evidence": "verified from agent_messages table",
    }


@app.post("/agents/{agent}/message")
def create_agent_message(agent: str, payload: AgentChatMessage) -> dict:
    agent = agent.strip().lower()
    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    msg = send_agent_message(DB_PATH, "dashboard", agent, message, priority="high")
    emit_event(DB_PATH, "dashboard_agent_message_sent", "dashboard", {"agent": agent, "message": message[:500]})
    if agent == "operator":
        response = _respond_as_operator(message)
        send_agent_message(DB_PATH, "operator", "dashboard", response, priority="normal")
        emit_event(DB_PATH, "operator_dashboard_reply", "operator", {"summary": response[:300]})
        return {
            "status": "queued",
            "message": response,
            "agent_message_id": msg.id,
        }
    if agent == "zuko":
        response = _respond_as_zuko(message)
        send_agent_message(DB_PATH, "zuko", "dashboard", response, priority="normal")
        emit_event(DB_PATH, "zuko_dashboard_reply", "zuko", {"summary": response[:300]})
        return {
            "status": "queued",
            "message": response,
            "agent_message_id": msg.id,
        }
    return {
        "status": "queued",
        "message": f"{agent} received the dashboard message.",
        "agent_message_id": msg.id,
    }


@app.post("/operator/task", status_code=201)
def create_operator_task(payload: TaskCreate) -> dict:
    """Create an operator task. Convenience endpoint — validates to_agent=operator."""
    if payload.to_agent != "operator":
        raise HTTPException(status_code=400, detail="Use POST /tasks for non-operator agents")
    task = enqueue_task(
        DB_PATH,
        Task(
            from_agent=payload.from_agent,
            to_agent="operator",
            task_type=payload.task_type,
            description=payload.description,
            status=payload.status,
            priority=payload.priority,
            urgency=payload.urgency,
            domain=payload.domain,
            payload=payload.payload,
            approval_required=payload.approval_required,
        ),
    )
    return {"id": task.id, "status": task.status, "to_agent": task.to_agent}


@app.post("/control/actions")
def control_action(payload: ControlAction) -> dict:
    """Execute safe dashboard actions or create audited work for agents."""
    action = payload.action.lower().strip()
    reason = payload.reason or "dashboard operator action"

    if action in {"pause_agent", "start_agent", "resume_agent", "stop_agent", "restart_agent"}:
        agent = (payload.agent or "").strip().lower()
        if not agent:
            raise HTTPException(status_code=400, detail="agent required")
        with _connect() as conn:
            exists = conn.execute("SELECT 1 FROM agent_registry WHERE name=?", (agent,)).fetchone()
        if not exists:
            raise HTTPException(status_code=400, detail=f"unknown agent: {agent}")
        if action == "pause_agent":
            return _set_agent_runtime_state(agent, "paused", reason)
        if action in {"start_agent", "resume_agent"}:
            return _set_agent_runtime_state(agent, "active", reason)
        if action == "stop_agent":
            return _set_agent_runtime_state(agent, "stopped", reason)
        if action == "restart_agent":
            # Soft restart: resume task pickup and recover abandoned in-progress rows.
            from shared.db.tasks import requeue_in_progress_tasks
            _set_agent_runtime_state(agent, "active", reason)
            recovered = requeue_in_progress_tasks(DB_PATH, agent)
            emit_event(DB_PATH, "dashboard_agent_soft_restart", agent, {
                "agent": agent,
                "requeued_in_progress_tasks": recovered,
                "reason": reason,
                "truthfulness": "soft restart only; in-process worker/container was not killed by the API",
            })
            return {
                "status": "done",
                "message": f"{agent} soft restart requested. Requeued {recovered} in-progress task(s); worker/container was not killed.",
                "evidence": f"agent={agent}; requeued={recovered}",
            }

    if action == "requeue_task":
        if not payload.task_id:
            raise HTTPException(status_code=400, detail="task_id required")
        update_task_status(DB_PATH, payload.task_id, "pending", {"requeued_by": "dashboard", "reason": reason})
        emit_event(DB_PATH, "dashboard_task_requeued", "roderick", {"task_id": payload.task_id, "reason": reason})
        return {"status": "done", "message": f"Task #{payload.task_id} requeued.", "evidence": f"tasks#{payload.task_id}"}

    if action == "cancel_task":
        if not payload.task_id:
            raise HTTPException(status_code=400, detail="task_id required")
        update_task_status(DB_PATH, payload.task_id, "cancelled", {"cancelled_by": "dashboard", "reason": reason})
        emit_event(DB_PATH, "dashboard_task_cancelled", "roderick", {"task_id": payload.task_id, "reason": reason})
        return {"status": "done", "message": f"Task #{payload.task_id} cancelled.", "evidence": f"tasks#{payload.task_id}"}

    if action == "diagnose_agent":
        agent = (payload.agent or "system").lower()
        with _connect() as conn:
            existing = _open_merlin_diagnostic(conn, agent, payload.task_id)
        if existing:
            return {
                "status": "already_queued",
                "message": f"Merlin is already queued for this diagnosis as task #{existing['id']}.",
                "task_id": existing["id"],
                "evidence": f"tasks#{existing['id']}",
            }
        desc = f"Diagnose {agent} from dashboard: {reason}"
        task = enqueue_task(DB_PATH, Task(
            from_agent="dashboard",
            to_agent="merlin",
            task_type="agent_diagnostics",
            description=desc,
            status="pending",
            priority="high",
            urgency="today",
            domain="operations",
            payload={"agent": agent, "source": "dashboard_control", "task_id": payload.task_id},
        ))
        emit_event(DB_PATH, "dashboard_merlin_diagnostic_requested", "merlin", {"task_id": task.id, "agent": agent})
        return {"status": "queued", "message": f"Merlin diagnostic queued as task #{task.id}.", "task_id": task.id}

    if action == "sentinel_check":
        agent = (payload.agent or "system").lower()
        task = enqueue_task(DB_PATH, Task(
            from_agent="dashboard",
            to_agent="sentinel",
            task_type="health_check",
            description=f"Dashboard requested Sentinel check for {agent}: {reason}",
            status="pending",
            priority="high",
            urgency="today",
            domain="security",
            payload={"agent": agent, "source": "dashboard_control"},
        ))
        emit_event(DB_PATH, "dashboard_sentinel_check_requested", "sentinel", {"task_id": task.id, "agent": agent})
        return {"status": "queued", "message": f"Sentinel check queued as task #{task.id}.", "task_id": task.id}

    if action == "validate_forge_artifact":
        if not payload.artifact_id:
            raise HTTPException(status_code=400, detail="artifact_id required")
        artifact = get_forge_artifact(DB_PATH, payload.artifact_id)
        if not artifact:
            raise HTTPException(status_code=404, detail="artifact not found")
        try:
            resolved_path = Path(artifact.path).resolve()
            resolved_root = Path(artifact.artifact_root).resolve()
            resolved_path.relative_to(resolved_root)
        except Exception:
            raise HTTPException(status_code=403, detail="artifact path is outside managed Forge workspace")
        task = enqueue_task(DB_PATH, Task(
            from_agent="dashboard",
            to_agent="sentinel",
            task_type="validate_build",
            description=f"Validate Forge artifact #{artifact.id}: {artifact.relative_path or artifact.path}",
            status="pending",
            priority="high",
            urgency="today",
            domain="validation",
            payload={
                "source": "dashboard_forge_artifact_validation",
                "artifact_id": artifact.id,
                "forge_task_id": artifact.task_id,
                "files_created": [str(resolved_path)],
                "project_dir": str(resolved_root),
                "project_path": str(resolved_root),
                "artifact_path": str(resolved_path),
                "artifact_root": str(resolved_root),
                "reason": reason,
            },
        ))
        emit_event(DB_PATH, "dashboard_forge_artifact_validation_requested", "sentinel", {
            "task_id": task.id,
            "artifact_id": artifact.id,
            "forge_task_id": artifact.task_id,
            "path": str(resolved_path),
            "reason": reason,
        })
        return {
            "status": "queued",
            "message": f"Sentinel artifact validation queued as task #{task.id}.",
            "task_id": task.id,
            "evidence": f"forge_artifacts#{artifact.id}; tasks#{task.id}",
        }

    if action == "atlas_focus":
        topic = reason.strip() or "Review what Merlin and Zuko found recently."
        task = enqueue_task(DB_PATH, Task(
            from_agent="dashboard",
            to_agent="atlas",
            task_type="skill_lesson",
            description=topic,
            status="pending",
            priority="normal",
            urgency="today",
            domain="learning",
            payload={"source": "dashboard_control"},
        ))
        emit_event(DB_PATH, "dashboard_atlas_focus_requested", "atlas", {"task_id": task.id, "topic": topic})
        return {"status": "queued", "message": f"Atlas lesson/focus task queued as #{task.id}.", "task_id": task.id}

    if action == "restart_service":
        service = (payload.service or "").strip().lower()
        if service not in {"roderick", "api", "dashboard", "zuko", "ollama"}:
            raise HTTPException(status_code=400, detail="service must be one of roderick, api, dashboard, zuko, ollama")
        approval = create_approval(DB_PATH, ApprovalRequest(
            request_type="control_action_approval",
            description=f"Restart {service} service from dashboard control plane.",
            payload={
                "action": "restart_service",
                "service": service,
                "reason": reason,
                "executor_status": "not_configured_in_api",
                "truthfulness": "Approval is recorded, but automatic restart execution is not wired in the API runtime yet.",
            },
        ))
        emit_event(DB_PATH, "dashboard_restart_approval_requested", "roderick", {"approval_id": approval.id, "service": service})
        return {
            "status": "approval_required",
            "message": f"Restart approval #{approval.id} created. Automatic execution is not wired yet.",
            "approval_id": approval.id,
        }

    raise HTTPException(status_code=400, detail="unsupported action")


@app.get("/system/metrics")
def system_metrics() -> dict:
    """Process-level CPU, memory, disk metrics from the API container."""
    proc = psutil.Process()
    mem = proc.memory_info()
    cpu_percent = psutil.cpu_percent(interval=0.0)
    vm = psutil.virtual_memory()
    disk = psutil.disk_usage("/app/data") if Path("/app/data").exists() else psutil.disk_usage("/")
    gpu = _query_gpu_snapshot()
    ollama = _query_ollama_ps()
    residency = _ollama_gpu_residency(ollama)
    if residency:
        gpu["ollama_residency"] = residency
        if not gpu.get("gpus"):
            gpu["status"] = "inferred"
            gpu["evidence"] = residency["evidence"]
    return {
        "process": {
            "pid": proc.pid,
            "cpu_percent": round(proc.cpu_percent(interval=0.0), 1),
            "memory_rss_mb": round(mem.rss / 1024 / 1024, 1),
            "memory_vms_mb": round(mem.vms / 1024 / 1024, 1),
            "threads": proc.num_threads(),
        },
        "host": {
            "cpu_percent": round(cpu_percent, 1),
            "memory_total_mb": round(vm.total / 1024 / 1024),
            "memory_used_mb": round(vm.used / 1024 / 1024),
            "memory_percent": round(vm.percent, 1),
            "disk_total_gb": round(disk.total / 1024 / 1024 / 1024, 1),
            "disk_used_gb": round(disk.used / 1024 / 1024 / 1024, 1),
            "disk_percent": round(disk.percent, 1),
        },
        "gpu": gpu,
        "ollama": ollama,
        "langfuse": {
            "status": "configured" if os.environ.get("LANGFUSE_PUBLIC_KEY") and os.environ.get("LANGFUSE_SECRET_KEY") else "disabled",
            "host": os.environ.get("LANGFUSE_HOST", "http://langfuse:3000"),
            "public_key_configured": bool(os.environ.get("LANGFUSE_PUBLIC_KEY")),
            "secret_key_configured": bool(os.environ.get("LANGFUSE_SECRET_KEY")),
            "ui": "http://localhost:3001",
            "evidence": "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY environment variables",
        },
        "db_path": DB_PATH,
        "db_size_mb": round(Path(DB_PATH).stat().st_size / 1024 / 1024, 2) if Path(DB_PATH).exists() else 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/sentinel/status")
def sentinel_status() -> dict:
    with _connect() as conn:
        agent_row = conn.execute("SELECT * FROM agent_registry WHERE name='sentinel'").fetchone()
        active = conn.execute(
            "SELECT * FROM tasks WHERE to_agent='sentinel' AND status='in_progress' ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        queued = conn.execute(
            """SELECT * FROM tasks
               WHERE to_agent='sentinel' AND status IN ('pending','approved')
               ORDER BY created_at DESC LIMIT 8"""
        ).fetchall()
        events = conn.execute(
            "SELECT id, event_type, agent, payload, created_at FROM events WHERE agent='sentinel' ORDER BY created_at DESC LIMIT 12"
        ).fetchall()
        merlin_msgs = conn.execute(
            """SELECT id, from_agent, to_agent, message, priority, read, created_at
               FROM agent_messages
               WHERE (from_agent='sentinel' AND to_agent='merlin') OR (from_agent='merlin' AND to_agent='sentinel')
               ORDER BY created_at DESC LIMIT 8"""
        ).fetchall()
    reports = _recent_agent_reports("sentinel", limit=5)
    latest_summary = reports[0]["summary"] if reports else ""
    checks = [
        "Forge build validation",
        "System improvement sanity checks",
        "Security finding review",
        "Health-check risk triage",
        "Merlin escalation for research-heavy findings",
    ]
    return {
        "agent": _row(agent_row) if agent_row else None,
        "active_task": _row(active) if active else None,
        "queued_tasks": [_row(r) for r in queued],
        "recent_events": [{**dict(r), "payload": _json(r["payload"], {})} for r in events],
        "merlin_research_messages": [dict(r) for r in merlin_msgs],
        "recent_reports": reports,
        "latest_summary": latest_summary,
        "checks": checks,
        "evidence": "verified from agent_registry, tasks, events, agent_messages, and report files",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/tasks")
def list_tasks(
    to_agent: Optional[str] = None,
    status: Optional[str] = None,
    domain: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
) -> list[dict]:
    query = "SELECT * FROM tasks WHERE 1=1"
    params: list[Any] = []
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
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [_row(row) for row in rows]


@app.get("/tasks/{task_id}")
def get_task_detail(task_id: int) -> dict:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="task not found")
        task = _row(row)

        events = [
            {**dict(r), "payload": _json(r["payload"], {})}
            for r in conn.execute(
                """SELECT id, event_type, agent, payload, created_at
                   FROM events
                   WHERE payload LIKE ? OR payload LIKE ? OR agent=?
                   ORDER BY created_at DESC LIMIT 30""",
                (f'%"task_id": {task_id}%', f'%"forge_task_id": {task_id}%', task["to_agent"]),
            ).fetchall()
        ]
        approvals = [
            _enrich_approval(r, conn)
            for r in conn.execute(
                "SELECT * FROM approval_requests WHERE task_id=? ORDER BY created_at DESC LIMIT 12",
                (task_id,),
            ).fetchall()
        ]
        artifacts = [
            _row(r)
            for r in conn.execute(
                "SELECT * FROM forge_artifacts WHERE task_id=? ORDER BY created_at DESC LIMIT 40",
                (task_id,),
            ).fetchall()
        ]
        messages = [
            _row(r)
            for r in conn.execute(
                """SELECT * FROM agent_messages
                   WHERE (from_agent=? OR to_agent=?)
                   ORDER BY created_at DESC LIMIT 20""",
                (task["to_agent"], task["to_agent"]),
            ).fetchall()
        ]

        improvement = None
        payload = task.get("payload") or {}
        imp_id = payload.get("improvement_id")
        if imp_id:
            imp_row = conn.execute("SELECT * FROM improvements WHERE id=?", (imp_id,)).fetchone()
            improvement = _row(imp_row) if imp_row else None
        elif task["to_agent"] == "forge":
            imp_row = conn.execute("SELECT * FROM improvements WHERE forge_task_id=?", (task_id,)).fetchone()
            improvement = _row(imp_row) if imp_row else None

        related_sentinel = None
        if task["to_agent"] == "forge":
            sentinel_row = conn.execute(
                """SELECT * FROM tasks
                   WHERE to_agent='sentinel'
                     AND (payload LIKE ? OR payload LIKE ?)
                   ORDER BY created_at DESC LIMIT 1""",
                (f'%"forge_task_id": {task_id}%', f'%"task_id": {task_id}%'),
            ).fetchone()
            related_sentinel = _row(sentinel_row) if sentinel_row else None

    return {
        "task": task,
        "stage": _task_stage(task),
        "output": _task_output_summary(task),
        "improvement": improvement,
        "related_sentinel_task": related_sentinel,
        "events": events,
        "approvals": approvals,
        "artifacts": artifacts,
        "messages": messages,
        "report": _load_task_report(task["to_agent"], task_id),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "evidence": "verified from tasks, events, approval_requests, forge_artifacts, agent_messages, and report files",
    }


@app.post("/tasks", status_code=201)
def create_task(payload: TaskCreate) -> dict:
    task = enqueue_task(
        DB_PATH,
        Task(
            to_agent=payload.to_agent,
            from_agent=payload.from_agent,
            task_type=payload.task_type,
            description=payload.description,
            status=payload.status,
            priority=payload.priority,
            urgency=payload.urgency,
            domain=payload.domain,
            payload=payload.payload,
            approval_required=payload.approval_required,
        ),
    )
    return task.__dict__


@app.get("/approvals")
def list_approvals(
    status: Optional[str] = "pending",
    limit: int = Query(100, ge=1, le=500),
) -> list[dict]:
    query = "SELECT * FROM approval_requests WHERE 1=1"
    params: list[Any] = []
    if status:
        query += " AND status=?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
        return [_enrich_approval(row, conn) for row in rows]


@app.post("/approvals/{approval_id}/resolve")
def resolve_approval_action(approval_id: int, payload: ApprovalAction) -> dict:
    action = payload.action.lower().strip()
    if action not in {"approve", "reject", "defer"}:
        raise HTTPException(status_code=400, detail="action must be approve, reject, or defer")

    approval = _get_pending_approval(approval_id)
    if not approval:
        raise HTTPException(status_code=404, detail="pending approval not found")

    status = {"approve": "approved", "reject": "rejected", "defer": "deferred"}[action]
    resolve_approval(DB_PATH, approval_id, status)
    _apply_approval_task_transition(approval, action)
    return {"id": approval_id, "status": status}


@app.get("/presence")
def get_presence() -> dict:
    manager = PresenceManager(CONFIG["data_dir"])
    return {"mode": manager.get_mode(), "valid_modes": sorted(VALID_MODES)}


@app.post("/presence")
def update_presence(payload: PresenceUpdate) -> dict:
    manager = PresenceManager(CONFIG["data_dir"])
    manager.set_mode(payload.mode)
    return {"mode": manager.get_mode(), "valid_modes": sorted(VALID_MODES)}


@app.get("/atlas/skills")
def get_atlas_skills() -> dict:
    path = Path(CONFIG["data_dir"]) / "atlas" / "skills.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/atlas/today")
def get_atlas_today() -> dict:
    lessons_dir = Path(CONFIG["data_dir"]) / "atlas" / "lessons"
    today = date.today().isoformat()
    path = lessons_dir / f"{today}.json"
    if not path.exists():
        return {}
    lesson = json.loads(path.read_text(encoding="utf-8"))
    _enrich_atlas_lesson(lesson)
    return lesson


def _enrich_atlas_lesson(lesson: dict) -> None:
    topic = str(lesson.get("topic") or "").strip()
    query = str(
        lesson.get("linkedin_learning_search_query")
        or lesson.get("linkedin_query")
        or (f"{_safe_atlas_learning_query(topic)} practical" if topic else "")
    ).strip()
    query = _safe_atlas_learning_query(query)
    if query:
        lesson["linkedin_learning_search_query"] = query
        lesson["linkedin_learning_search_url"] = f"https://www.linkedin.com/learning/search?keywords={quote_plus(query)}"
    lesson.setdefault(
        "shareable_outcome",
        f"Explain and demonstrate {_safe_atlas_learning_query(topic)} using a real Roderick ecosystem example.",
    )
    lesson.setdefault(
        "portfolio_receipt",
        "After completing a relevant LinkedIn Learning course or learning path, save the certificate/share link in Atlas.",
    )
    lesson.setdefault(
        "recruiter_visibility",
        "Atlas tracks completion links you provide; it does not claim completion without a receipt.",
    )


def _safe_atlas_learning_query(text: str) -> str:
    cleaned = str(text or "").replace("$", "").replace("AUD", "")
    cleaned = re.sub(r"\([^)]*(?:cost|price|\d+\s*certification|certification)[^)]*\)", "", cleaned, flags=re.IGNORECASE)
    for phrase in ("Cost-Effective", "cost-effective"):
        cleaned = cleaned.replace(phrase, "")
    while "  " in cleaned:
        cleaned = cleaned.replace("  ", " ")
    return cleaned.strip(" -()")


@app.get("/atlas/learning")
def get_atlas_learning() -> dict:
    atlas_dir = Path(CONFIG["data_dir"]) / "atlas"
    log_path = atlas_dir / "learning_log.json"
    status_path = atlas_dir / "lesson_status.json"
    lesson = get_atlas_today()
    entries = []
    if log_path.exists():
        try:
            data = json.loads(log_path.read_text(encoding="utf-8"))
            entries = data if isinstance(data, list) else []
        except Exception:
            entries = []
    status = {}
    if status_path.exists():
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
        except Exception:
            status = {}
    return {
        "status": status,
        "today": lesson,
        "entries": entries[:100],
        "coaching_recommendations": _atlas_coaching_recommendations(lesson, entries, status),
        "linkedin_learning_note": (
            "LinkedIn Learning course/path certificates can be added or shared after completion. "
            "Atlas records links you provide but will not claim completion without a recorded receipt."
        ),
    }


def _atlas_coaching_recommendations(lesson: dict, entries: list, status: dict) -> list[dict]:
    topic = str(lesson.get("topic") or "current focus")
    recs = [
        {
            "title": "Teach-back check",
            "summary": f"After studying {topic}, explain it back to Atlas in 5 bullets and ask for corrections.",
            "evidence": "atlas/today lesson topic",
        },
        {
            "title": "Proof of learning",
            "summary": "Attach a LinkedIn Learning completion link, GitHub demo, or short note when you finish.",
            "evidence": "atlas learning receipt workflow",
        },
    ]
    if not entries:
        recs.append({
            "title": "Start receipt history",
            "summary": "No learning receipts are recorded yet; add the first proof after your next lesson.",
            "evidence": "atlas learning_log.json is empty or missing",
        })
    if status.get("postponed_until"):
        recs.append({
            "title": "Postponed lesson",
            "summary": f"Atlas has a postponement recorded until {status.get('postponed_until')}. Resume when ready.",
            "evidence": "atlas lesson_status.json",
        })
    return recs


@app.post("/atlas/learning", status_code=201)
def create_atlas_learning_entry(payload: AtlasLearningEntry) -> dict:
    atlas_dir = Path(CONFIG["data_dir"]) / "atlas"
    atlas_dir.mkdir(parents=True, exist_ok=True)
    log_path = atlas_dir / "learning_log.json"
    try:
        entries = json.loads(log_path.read_text(encoding="utf-8")) if log_path.exists() else []
        if not isinstance(entries, list):
            entries = []
    except Exception:
        entries = []
    entry = payload.dict()
    entry["created_at"] = datetime.now(timezone.utc).isoformat()
    entry["id"] = f"atlas_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    entries.insert(0, entry)
    log_path.write_text(json.dumps(entries[:200], indent=2, ensure_ascii=False), encoding="utf-8")
    emit_event(DB_PATH, "atlas_learning_recorded", "atlas", {"topic": entry["topic"], "type": entry["type"]})
    return entry


@app.get("/opportunities")
def get_opportunities() -> dict:
    founder = OwnerMemory(CONFIG["memory_dir"])
    reports = []
    for path in sorted((Path(CONFIG["data_dir"]) / "reports").glob("venture_*.json")):
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            _normalize_opportunity_report(report)
            report["_report_path"] = str(path)
            reports.append(report)
        except Exception:
            continue
    recent_reports = sorted(
        reports,
        key=lambda r: str(r.get("_researched_at") or r.get("_generated_at") or ""),
        reverse=True,
    )
    recent = [
        {
            "date": str(report.get("_researched_at") or "")[:10] or date.today().isoformat(),
            "title": report.get("opportunity_summary") or "Opportunity",
            "category": report.get("category") or "other",
            "capital": report.get("capital_required"),
            "risk": report.get("risk_level") or "unknown",
        }
        for report in recent_reports[:12]
    ]
    return {
        "recent": recent or founder.get_recent_opportunities(days=30),
        "log": founder.get_opportunity_log(),
        "reports": reports,
    }


def _normalize_opportunity_report(report: dict) -> None:
    aliases = {
        "opportunity_summary": ["opportunity", "opportunity_name", "name", "title"],
        "market_problem": ["problem", "problem_solved"],
        "proposed_solution": ["solution"],
        "revenue_model": ["recurring_revenue_model", "monetization"],
        "time_to_first_revenue": ["time_to_revenue", "revenue_timeline"],
        "revenue_potential": ["recurring_revenue_potential", "revenue"],
        "competition": ["competition_analysis"],
        "automation_potential": ["automation", "automation_level", "automation_alignment"],
    }
    for canonical, keys in aliases.items():
        if not str(report.get(canonical) or "").strip():
            for key in keys:
                if str(report.get(key) or "").strip():
                    report[canonical] = report[key]
                    break


@app.get("/events")
def list_events(limit: int = Query(50, ge=1, le=200), unprocessed_only: bool = True) -> list[dict]:
    """Unprocessed events for n8n polling. Call POST /events/ack to mark as processed."""
    if unprocessed_only:
        return get_unprocessed_events(DB_PATH, limit=limit)
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [_row(row) for row in rows]


@app.get("/logs")
def get_logs(limit: int = Query(100, ge=1, le=500)) -> list[dict]:
    """Unified activity log — events + task transitions, newest first."""
    entries: list[dict] = []

    with _connect() as conn:
        # Events (agent-emitted structured events)
        evts = conn.execute(
            "SELECT id, event_type, agent, payload, created_at FROM events ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        for e in evts:
            payload = _json(e["payload"], {})
            entries.append({
                "kind": "event",
                "id": f"evt-{e['id']}",
                "agent": e["agent"],
                "type": e["event_type"],
                "message": _event_label(e["event_type"], payload),
                "detail": payload,
                "ts": _timestamp(e["created_at"]),
            })

        # Task status changes — completed, failed, in_progress
        tasks = conn.execute(
            """SELECT id, from_agent, to_agent, task_type, description, status, result, created_at, updated_at
               FROM tasks
               WHERE status IN ('completed','failed','in_progress','live','rolled_back')
               ORDER BY updated_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        for t in tasks:
            result = _json(t["result"], None)
            snippet = ""
            if isinstance(result, dict):
                snippet = str(result.get("opportunity_summary") or result.get("summary") or result.get("error") or "")[:120]
            elif isinstance(result, str):
                snippet = result[:120]
            entries.append({
                "kind": "task",
                "id": f"task-{t['id']}",
                "agent": t["to_agent"],
                "type": t["task_type"],
                "message": f"Task #{t['id']} {t['status']}: {t['description'][:80]}",
                "detail": {"status": t["status"], "snippet": snippet, "from": t["from_agent"]},
                "ts": _timestamp(t["updated_at"] or t["created_at"]),
            })

        # Agent messages (inter-agent comms)
        try:
            msgs = conn.execute(
                "SELECT id, from_agent, to_agent, message, created_at FROM agent_messages ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            for m in msgs:
                entries.append({
                    "kind": "message",
                    "id": f"msg-{m['id']}",
                    "agent": m["from_agent"],
                    "type": "agent_message",
                    "message": f"{m['from_agent']} → {m['to_agent']}: {m['message'][:100]}",
                    "detail": {"to": m["to_agent"], "full": m["message"]},
                    "ts": _timestamp(m["created_at"]),
                })
        except Exception:
            pass  # agent_messages may not have data yet

    # Sort by timestamp descending, cap at limit
    entries.sort(key=lambda x: x.get("ts") or "", reverse=True)
    return entries[:limit]


def _event_label(event_type: str, payload: dict) -> str:
    labels = {
        "task_completed":            lambda p: f"Task completed by {p.get('agent', '?')}",
        "task_failed":               lambda p: f"Task failed: {p.get('error', '')[:60]}",
        "opportunity_discovered":    lambda p: f"Opportunity found: {p.get('summary', '')[:60]}",
        "forge_awaiting_validation": lambda p: f"Forge waiting for Sentinel validation",
        "skill_learned":             lambda p: f"Atlas: skill updated — {p.get('skill', '')}",
        "research_complete":         lambda p: f"Merlin: research done — {p.get('title', '')[:60]}",
    }
    fn = labels.get(event_type)
    if fn:
        try:
            return fn(payload)
        except Exception:
            pass
    return event_type.replace("_", " ")


@app.post("/events/ack")
def ack_events(payload: dict) -> dict:
    """Mark events as processed. Body: {\"ids\": [1, 2, 3]}"""
    ids = payload.get("ids", [])
    if not isinstance(ids, list) or not all(isinstance(i, int) for i in ids):
        raise HTTPException(status_code=400, detail="ids must be a list of integers")
    mark_processed(DB_PATH, ids)
    return {"acked": len(ids)}


@app.get("/improvements")
def list_improvements_endpoint(
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
) -> list[dict]:
    """List improvement pipeline entries."""
    improvements = list_improvements(DB_PATH, status=status, limit=limit)
    return [imp.__dict__ for imp in improvements]


@app.get("/improvements/{imp_id}")
def get_improvement_endpoint(imp_id: int) -> dict:
    imp = get_improvement(DB_PATH, imp_id)
    if not imp:
        raise HTTPException(status_code=404, detail="improvement not found")
    return imp.__dict__


class ImprovementAdvance(BaseModel):
    new_status: str
    evidence_update: dict[str, Any] = Field(default_factory=dict)


@app.post("/improvements/{imp_id}/advance")
def advance_improvement_endpoint(imp_id: int, payload: ImprovementAdvance) -> dict:
    valid_statuses = {
        "signal", "investigating", "proposed", "approved", "implementing",
        "validating", "complete", "rejected", "failed", "rolled_back",
    }
    if payload.new_status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"invalid status: {payload.new_status}")
    imp = advance_improvement(DB_PATH, imp_id, payload.new_status, payload.evidence_update or None)
    if not imp:
        raise HTTPException(status_code=404, detail="improvement not found")
    return imp.__dict__


@app.get("/pipeline")
def get_pipeline() -> dict:
    """Improvement pipeline summary: counts by status + active items."""
    improvements = list_improvements(DB_PATH, limit=200)
    by_status: dict[str, list] = {}
    for imp in improvements:
        by_status.setdefault(imp.status, []).append({
            "id": imp.id,
            "title": imp.title,
            "origin_agent": imp.origin_agent,
            "origin_signal": imp.origin_signal,
            "priority": imp.priority,
            "risk_level": imp.risk_level,
            "forge_recommended": imp.forge_recommended,
            "created_at": imp.created_at,
            "updated_at": imp.updated_at,
        })
    terminal = {"complete", "rejected", "failed", "rolled_back"}
    active_count = sum(len(v) for k, v in by_status.items() if k not in terminal)
    return {
        "by_status": by_status,
        "active_count": active_count,
        "total_count": len(improvements),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/forge/artifacts")
def list_forge_artifacts_endpoint(
    task_id: Optional[int] = None,
    limit: int = Query(100, ge=1, le=500),
) -> list[dict]:
    """List Forge-created artifact records for dashboard review."""
    artifacts = list_forge_artifacts(DB_PATH, task_id=task_id, limit=limit)
    return [artifact.__dict__ for artifact in artifacts]


@app.get("/forge/artifacts/{artifact_id}")
def get_forge_artifact_endpoint(artifact_id: int) -> dict:
    artifact = get_forge_artifact(DB_PATH, artifact_id)
    if not artifact:
        raise HTTPException(status_code=404, detail="artifact not found")
    return artifact.__dict__


@app.get("/forge/artifacts/{artifact_id}/content")
def get_forge_artifact_content(artifact_id: int) -> dict:
    artifact = get_forge_artifact(DB_PATH, artifact_id)
    if not artifact:
        raise HTTPException(status_code=404, detail="artifact not found")
    path = Path(artifact.path)
    root = Path(artifact.artifact_root)
    try:
        resolved_path = path.resolve()
        resolved_root = root.resolve()
        resolved_path.relative_to(resolved_root)
    except Exception:
        raise HTTPException(status_code=403, detail="artifact path is outside managed root")
    if not resolved_path.exists() or not resolved_path.is_file():
        raise HTTPException(status_code=404, detail="artifact file not found")
    if resolved_path.stat().st_size > 512_000:
        raise HTTPException(status_code=413, detail="artifact is too large to preview")
    return {
        "artifact": artifact.__dict__,
        "content": resolved_path.read_text(encoding="utf-8", errors="replace"),
    }


@app.get("/forge/workflow")
def get_forge_workflow() -> dict:
    """Truthful CI/CD workflow readiness for Forge-managed changes."""
    local_git_enabled = (REPO_ROOT / ".git").exists()
    github_repo = os.environ.get("GITHUB_REPOSITORY", "")
    github_url = os.environ.get("GITHUB_REPOSITORY_URL", "")
    github_branch = os.environ.get("GITHUB_DEFAULT_BRANCH", "main")
    github_workflow = os.environ.get("GITHUB_ACTIONS_WORKFLOW", "Sentinel Gate")
    github_connected = bool(github_repo or github_url)
    workflow_path = REPO_ROOT / ".github" / "workflows" / "sentinel-gate.yml"
    workflow_configured = workflow_path.exists() or github_connected
    artifact_root = Path(CONFIG["data_dir"]) / "forge_artifacts"
    return {
        "git_enabled": local_git_enabled or github_connected,
        "local_git_enabled": local_git_enabled,
        "github_connected": github_connected,
        "workflow_configured": workflow_configured,
        "status": "configured" if (github_connected and workflow_configured) else ("local_only" if local_git_enabled else "not_configured"),
        "repo_root": str(REPO_ROOT),
        "artifact_root": str(artifact_root),
        "github_repository": github_repo,
        "github_url": github_url,
        "github_branch": github_branch,
        "github_workflow": github_workflow,
        "source_of_truth": "github_private_repo" if github_connected else ("local_git_repo" if local_git_enabled else "local_workspace_without_git_metadata"),
        "promotion_flow": [
            "Forge stages generated output in the managed artifact workspace.",
            "Roderick creates or links the approval request.",
            "Forge applies approved code/config changes only after approval.",
            "Sentinel runs compile, config, security, and service sanity checks.",
            "GitHub Actions runs the Sentinel Gate on pushed commits and pull requests.",
            "Phone approval can happen through GitHub mobile by reviewing the run or PR and then approving the matching Roderick request.",
            "Docker services are rebuilt/refreshed from the verified local workspace after approval.",
        ],
        "sentinel_gate": [
            "python -m compileall apps shared",
            "configuration JSON sanity checks",
            "secret leakage and dangerous shell pattern scan",
            "dashboard build when dashboard files changed",
            "docker compose config sanity check with placeholder CI env",
            "local Sentinel validation remains the required live-system promotion gate",
        ],
        "phone_approval_flow": [
            "Open the GitHub Actions run or pull request on the phone.",
            "Confirm the Sentinel Gate is green and inspect changed files if needed.",
            "Approve the matching Roderick dashboard or Telegram approval.",
            "Roderick/Forge/Sentinel can then promote locally and refresh Docker.",
        ],
        "truthfulness": (
            "GitHub private repo and Sentinel Gate workflow are configured; local promotion still requires Sentinel validation and user approval."
            if github_connected
            else (
                "Local Git metadata is present, but GitHub CI/CD is not configured in the API environment."
                if local_git_enabled
                else "Git promotion is not active in this runtime until GitHub repo settings are configured."
            )
        ),
    }


@app.get("/reports/{agent}/{task_id}")
def get_report(agent: str, task_id: int) -> dict:
    reports_dir = Path(CONFIG["data_dir"]) / "reports"
    candidates = [
        reports_dir / f"{agent}_{task_id}.json",
        reports_dir / f"{agent}_{task_id}_plan.json",
    ]
    for path in candidates:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    raise HTTPException(status_code=404, detail="report not found")


@app.get("/behaviors")
def list_behaviors(
    agent: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
) -> list[dict]:
    """List agent behavior policies."""
    from shared.db.behavior import list_policies
    policies = list_policies(DB_PATH, agent=agent, status=status, limit=limit)
    return [p.__dict__ for p in policies]


@app.get("/behaviors/{agent}/effective")
def get_effective_behaviors(agent: str) -> dict:
    """Get the currently applied (effective) behavior policies for an agent."""
    from shared.db.behavior import get_effective_policies
    return {"agent": agent, "policies": get_effective_policies(DB_PATH, agent)}


class BehaviorAction(BaseModel):
    action: str  # approve | reject | rollback


@app.post("/behaviors/{agent}/{policy_key}/action")
def behavior_action(agent: str, policy_key: str, payload: BehaviorAction) -> dict:
    """Approve, reject, or roll back a behavior policy."""
    from shared.db.behavior import apply_policy, reject_policy, rollback_policy, get_policy
    action = payload.action.lower().strip()
    if action == "approve":
        pol = apply_policy(DB_PATH, agent, policy_key, approved_by="dashboard")
    elif action == "reject":
        reject_policy(DB_PATH, agent, policy_key)
        pol = get_policy(DB_PATH, agent, policy_key)
    elif action == "rollback":
        rollback_policy(DB_PATH, agent, policy_key)
        pol = get_policy(DB_PATH, agent, policy_key)
    else:
        raise HTTPException(status_code=400, detail="action must be approve, reject, or rollback")
    if not pol:
        raise HTTPException(status_code=404, detail="policy not found")
    return pol.__dict__


def _get_pending_approval(approval_id: int) -> Optional[dict]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM approval_requests WHERE id=? AND status='pending'",
            (approval_id,),
        ).fetchone()
    return _row(row) if row else None


def _apply_approval_task_transition(approval: dict, action: str) -> None:
    task_id = approval.get("task_id")
    if not task_id:
        return
    request_type = approval["request_type"]
    payload = approval.get("payload") or {}

    if action == "approve":
        if request_type == "task_approval":
            update_task_status(DB_PATH, task_id, "approved")
            _advance_improvement_from_approval(approval, "approved")
        elif request_type == "plan_approval":
            update_task_status(DB_PATH, task_id, "plan_approved")
            _advance_improvement_from_approval(approval, "implementing")
        elif request_type == "sentinel_approval":
            forge_task_id = payload.get("forge_task_id") or task_id
            update_task_status(DB_PATH, forge_task_id, "live")
            update_task_status(DB_PATH, task_id, "completed")
            _advance_improvement_from_approval(approval, "complete")
        return

    if action == "reject":
        if request_type == "sentinel_approval":
            forge_task_id = payload.get("forge_task_id") or task_id
            update_task_status(DB_PATH, forge_task_id, "rolled_back")
            update_task_status(DB_PATH, task_id, "rolled_back")
            _advance_improvement_from_approval(approval, "rolled_back")
        else:
            update_task_status(DB_PATH, task_id, "rejected")
            _advance_improvement_from_approval(approval, "rejected")


def _advance_improvement_from_approval(approval: dict, status: str) -> None:
    improvement_id = (approval.get("payload") or {}).get("improvement_id")
    if not improvement_id:
        return
    try:
        advance_improvement(DB_PATH, int(improvement_id), status)
    except Exception:
        return


# ── Jobs (Zuko applications.db) ──────────────────────────────────────────────

def _jobs_db_path() -> str:
    return str(Path(CONFIG["data_dir"]) / "applications.db")


def _jobs_connect():
    path = _jobs_db_path()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/jobs")
def list_jobs(
    status: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = Query(200, ge=1, le=1000),
) -> dict:
    """List job applications from Zuko's applications.db."""
    path = _jobs_db_path()
    if not Path(path).exists():
        return {"applications": [], "stats": {}, "total": 0}

    with _jobs_connect() as conn:
        query = "SELECT * FROM applications WHERE 1=1"
        params: list[Any] = []
        if status:
            query += " AND status=?"
            params.append(status)
        if source:
            query += " AND source=?"
            params.append(source)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        apps = [dict(r) for r in rows]

        # stats: count by status
        stat_rows = conn.execute(
            "SELECT status, COUNT(*) as n FROM applications GROUP BY status"
        ).fetchall()
        stats = {r["status"]: r["n"] for r in stat_rows}
        total = conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0]

    return {"applications": apps, "stats": stats, "total": total}


@app.post("/zuko/scan", status_code=202)
def trigger_zuko_scan() -> dict:
    """Enqueue a job_search task for Zuko. Returns immediately."""
    task = enqueue_task(
        DB_PATH,
        Task(
            to_agent="zuko",
            from_agent="dashboard",
            task_type="job_search",
            description="Manual scan triggered from dashboard",
            status="pending",
            priority="high",
            urgency="immediate",
            domain="career",
            payload={"trigger": "dashboard_scan"},
            approval_required=False,
        ),
    )
    emit_event(DB_PATH, "zuko_scan_triggered", "dashboard", {"task_id": task.id, "source": "dashboard"})
    return {"queued": True, "task_id": task.id}
