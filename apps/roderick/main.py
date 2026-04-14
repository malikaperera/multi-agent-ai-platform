"""
Roderick — entrypoint.

Startup sequence:
  1. Logging
  2. Config (from config/roderick.json + env)
  3. DB init
  4. Per-agent LLM instances (each agent gets its own model)
  5. Memory + agent registry
  6. Orchestrator
  7. Telegram Application
  8. post_init: scheduler + agent workers + "Roderick online." message
  9. run_polling

Agent → Model mapping (configured in config/agents.json "model_used" field):
  roderick  → qwen3:14b        (orchestrator reasoning)
  merlin    → qwen2.5-coder:14b (research / technical analysis)
  venture   → qwen3:14b        (business exploration)
  atlas     → qwen3:14b        (skill tutoring — configurable domain)
  sentinel  → qwen2.5-coder:14b (tests / security / debugging)
  forge     → qwen3:14b        (planning; optional manual Claude handoff for execution)
  zuko      → qwen3:14b        (job search — runs in its own container)
"""
import asyncio
import json
import logging
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from telegram.ext import Application

from apps.atlas.agent import AtlasAgent
from apps.atlas.bot import build_atlas_application
from apps.forge.agent import ForgeAgent
from apps.merlin.agent import MerlinAgent
from apps.operator.agent import OperatorAgent
from apps.operator.bot import build_operator_application
from apps.roderick.bot.handlers import build_application
from apps.roderick.core.agent_registry import AgentRegistryManager
from apps.roderick.core.memory import MemoryManager
from apps.roderick.core.orchestrator import Orchestrator
from apps.roderick.core.presence import PresenceManager
from apps.roderick.core.scheduler import RoderickScheduler
from apps.sentinel.agent import SentinelAgent
from apps.venture.agent import VentureAgent
from shared.db.schema import get_db_path, init_db, seed_db_if_needed
from shared.llm.factory import build_llm
from shared.memory.founder import OwnerMemory
from shared.utils.config import agents_config_path, load_config
from shared.utils.logging_setup import setup_logging

logger = logging.getLogger(__name__)

# Fallback model if an agent's config entry has no model_used
_DEFAULT_MODEL = "qwen3:14b"


def _classify_main_telegram_priority(text: str) -> str:
    """Best-effort priority classification for main-chat Telegram noise control."""
    lowered = (text or "").lower()
    if any(token in lowered for token in (
        "critical", "🚨", "blocked promotion", "deployment failed", " failed",
    )):
        return "critical"
    if any(token in lowered for token in (
        "⚠️", "blocked", "capital approval required", "promotion approval", "security:",
        "revision queued", "patch deployed", "system improvement failed",
    )):
        return "high"
    if any(token in lowered for token in (
        "approval digest", "pending approval",
    )):
        return "high"
    return "normal"


def _should_send_main_telegram(text: str) -> tuple[bool, str]:
    """
    Decide whether a main-chat Telegram message is worth sending.
    Telegram should be sparse: approvals + high-signal alerts, not dashboard chatter.
    """
    lowered = (text or "").lower()
    priority = _classify_main_telegram_priority(text)

    suppress_markers = (
        "research complete",
        "investigation complete",
        "opportunity found",
        "sentinel report",
        "system improvement validation",
        "health check",
        "plan ready",
        "markdown artifact queued",
        "scaffold generated",
        "revision feedback held",
        "dashboard reply sent",
        "roderick online",
    )
    if any(marker in lowered for marker in suppress_markers):
        return False, priority

    return True, priority


def _wait_for_ollama_ready(config: dict, *, max_wait_seconds: int = 90, poll_seconds: int = 3) -> bool:
    """Wait briefly for Ollama to become responsive before starting workers."""
    host = (
        os.environ.get("OLLAMA_HOST")
        or config.get("llm", {}).get("ollama_host")
        or "http://host.docker.internal:11434"
    ).rstrip("/")
    deadline = time.time() + max_wait_seconds
    attempt = 0
    last_error = ""
    while time.time() < deadline:
        attempt += 1
        try:
            with urllib.request.urlopen(f"{host}/api/version", timeout=5) as resp:
                if resp.status == 200:
                    logger.info("Ollama ready at %s after %s attempt(s)", host, attempt)
                    return True
        except Exception as e:
            last_error = str(e)
        logger.info("Waiting for Ollama at %s (attempt %s, last=%s)", host, attempt, last_error[:120])
        time.sleep(poll_seconds)
    logger.warning("Ollama did not become ready within %ss: %s", max_wait_seconds, last_error)
    return False


def _load_agent_models(agents_path: str) -> dict[str, str]:
    """Read model_used for each agent from config/agents.json."""
    try:
        data = json.loads(Path(agents_path).read_text(encoding="utf-8"))
        return {a["name"]: a.get("model_used", _DEFAULT_MODEL) for a in data.get("agents", [])}
    except Exception as e:
        logger.warning("Could not load agent models from config: %s", e)
        return {}


def _load_forge_dual_models(agents_path: str) -> tuple[str, str]:
    """Return (planner_model, coder_model) for Forge from agents.json."""
    try:
        data = json.loads(Path(agents_path).read_text(encoding="utf-8"))
        for a in data.get("agents", []):
            if a["name"] == "forge":
                return (
                    a.get("planner_model", a.get("model_used", _DEFAULT_MODEL)),
                    a.get("coder_model", "qwen2.5-coder:14b"),
                )
    except Exception as e:
        logger.warning("Could not load Forge dual-model config: %s", e)
    return ("qwen3:30b", "qwen2.5-coder:14b")


def _load_merlin_dual_models(agents_path: str) -> tuple[str, str]:
    """Return (research_model, diagnostic_model) for Merlin from agents.json."""
    try:
        data = json.loads(Path(agents_path).read_text(encoding="utf-8"))
        for a in data.get("agents", []):
            if a["name"] == "merlin":
                return (
                    a.get("research_model", a.get("model_used", "qwen3:30b")),
                    a.get("diagnostic_model", "qwen3:4b"),
                )
    except Exception as e:
        logger.warning("Could not load Merlin dual-model config: %s", e)
    return ("qwen3:30b", "qwen3:4b")


def _load_venture_dual_models(agents_path: str) -> tuple[str, str]:
    """Return (deep_model, routine_model) for Venture from agents.json."""
    try:
        data = json.loads(Path(agents_path).read_text(encoding="utf-8"))
        for a in data.get("agents", []):
            if a["name"] == "venture":
                return (
                    a.get("deep_model", a.get("model_used", "qwen3:30b")),
                    a.get("routine_model", "qwen3:14b"),
                )
    except Exception as e:
        logger.warning("Could not load Venture dual-model config: %s", e)
    return ("qwen3:30b", "qwen3:14b")


def _load_roderick_dual_models(agents_path: str, config: dict) -> tuple[str, str]:
    """Return (coordinator_model, control_model) for Roderick from config."""
    roderick_cfg = config.get("roderick", {})
    fallback_control = roderick_cfg.get("control_model", _DEFAULT_MODEL)
    fallback_coordinator = roderick_cfg.get("coordinator_model", fallback_control)
    try:
        data = json.loads(Path(agents_path).read_text(encoding="utf-8"))
        for a in data.get("agents", []):
            if a["name"] == "roderick":
                control = a.get("control_model", a.get("model_used", fallback_control))
                coordinator = a.get("coordinator_model", control)
                return (coordinator, control)
    except Exception as e:
        logger.warning("Could not load Roderick dual-model config: %s", e)
    return (fallback_coordinator, fallback_control)


def main() -> None:
    config = load_config()
    setup_logging(config["data_dir"])
    logger.info("Starting Roderick…")

    db_path = get_db_path(config)
    seed_db_if_needed(db_path, os.environ.get("DB_SEED_PATH"))
    init_db(db_path)
    logger.info("DB ready: %s", db_path)

    agents_path = str(agents_config_path())
    agent_models = _load_agent_models(agents_path)
    logger.info("Agent models: %s", agent_models)

    def llm_for(agent: str):
        model = agent_models.get(agent, _DEFAULT_MODEL)
        return build_llm(config, model=model)

    _wait_for_ollama_ready(
        config,
        max_wait_seconds=int(os.environ.get("OLLAMA_STARTUP_WAIT_SECONDS", "90")),
        poll_seconds=int(os.environ.get("OLLAMA_STARTUP_POLL_SECONDS", "3")),
    )

    # Roderick uses a small coordinator model for routing and the 14B model for deeper control.
    roderick_coordinator_model, roderick_control_model = _load_roderick_dual_models(agents_path, config)
    logger.info(
        "Roderick models: coordinator=%s control=%s",
        roderick_coordinator_model,
        roderick_control_model,
    )
    orchestrator_llm = build_llm(config, model=roderick_control_model)
    orchestrator_coordinator_llm = build_llm(config, model=roderick_coordinator_model)

    memory  = MemoryManager(config["data_dir"])
    registry = AgentRegistryManager(db_path, agents_path)
    registry.sync_from_config()

    owner_memory = OwnerMemory(config.get("memory_dir", "memory"))
    presence = PresenceManager(config["data_dir"])

    orchestrator = Orchestrator(
        orchestrator_llm,
        db_path,
        memory,
        registry,
        config,
        owner_memory,
        coordinator_llm=orchestrator_coordinator_llm,
    )

    app = build_application(config, orchestrator, db_path)

    # Workers — each gets its own LLM instance with the right model
    merlin_research_model, merlin_diagnostic_model = _load_merlin_dual_models(agents_path)
    logger.info(
        "Merlin models: research=%s diagnostic=%s",
        merlin_research_model,
        merlin_diagnostic_model,
    )
    merlin = MerlinAgent(
        build_llm(config, model=merlin_research_model),
        db_path,
        config["data_dir"],
        config,
        owner_memory,
        diagnostic_llm=build_llm(config, model=merlin_diagnostic_model),
        research_model=merlin_research_model,
        diagnostic_model=merlin_diagnostic_model,
    )
    forge_planner_model, forge_coder_model = _load_forge_dual_models(agents_path)
    forge    = ForgeAgent(
        llm_for("forge"),
        db_path, config["data_dir"], config, owner_memory,
        coder_llm=build_llm(config, model=forge_coder_model),
        planner_model=forge_planner_model,
        coder_model=forge_coder_model,
    )
    venture_deep_model, venture_routine_model = _load_venture_dual_models(agents_path)
    logger.info(
        "Venture models: deep=%s routine=%s",
        venture_deep_model,
        venture_routine_model,
    )
    venture  = VentureAgent(
        build_llm(config, model=venture_deep_model),
        db_path,
        config["data_dir"],
        config,
        owner_memory,
        routine_llm=build_llm(config, model=venture_routine_model),
        deep_model=venture_deep_model,
        routine_model=venture_routine_model,
    )
    atlas    = AtlasAgent(   llm_for("atlas"),    db_path, config["data_dir"], config, owner_memory)
    sentinel = SentinelAgent(llm_for("sentinel"), db_path, config["data_dir"], config, owner_memory)
    operator = OperatorAgent(llm_for("operator"), db_path, config["data_dir"], config, owner_memory)

    atlas_chat_id_raw = os.environ.get("ATLAS_TELEGRAM_CHAT_ID") or os.environ.get("ATLAS_CHAT_ID")
    atlas_chat_id = int(atlas_chat_id_raw) if atlas_chat_id_raw else None
    atlas.atlas_chat_id = atlas_chat_id
    atlas.set_presence(presence.get_mode)

    operator_bot_token = os.environ.get("OPERATOR_TELEGRAM_BOT_TOKEN", "").strip()
    operator_chat_id_raw = os.environ.get("OPERATOR_TELEGRAM_CHAT_ID", "").strip()
    operator_chat_id = int(operator_chat_id_raw) if operator_chat_id_raw else None

    async def post_init(application: Application) -> None:
        main_chat_id = config["authorized_chat_id"]

        async def send_fn(text: str) -> None:
            should_send, inferred_priority = _should_send_main_telegram(text)
            if not should_send:
                logger.info("Suppressed routine main Telegram update (priority=%s): %s", inferred_priority, text[:160])
                return
            if not presence.should_send_telegram(inferred_priority):
                logger.info("Presence muted main Telegram update (mode=%s priority=%s): %s", presence.get_mode(), inferred_priority, text[:160])
                return
            await application.bot.send_message(
                chat_id=main_chat_id, text=text, parse_mode="HTML"
            )

        # Atlas uses only its own Telegram bot/chat when configured.
        atlas_bot_token = os.environ.get("ATLAS_TELEGRAM_BOT_TOKEN", "")
        atlas_chat_app = None
        if atlas_bot_token:
            if atlas_chat_id is not None:
                atlas_chat_app = build_atlas_application(atlas_bot_token, atlas_chat_id, atlas)
                _atlas_bot = atlas_chat_app.bot
            else:
                _atlas_bot = None
        else:
            _atlas_bot = None

        async def atlas_send_fn(text: str) -> None:
            if _atlas_bot is None or atlas_chat_id is None:
                logger.warning(
                    "Atlas Telegram delivery skipped: ATLAS_TELEGRAM_BOT_TOKEN and ATLAS_TELEGRAM_CHAT_ID are required"
                )
                return
            await _atlas_bot.send_message(
                chat_id=atlas_chat_id, text=text, parse_mode="HTML"
            )

        # Operator — optional dedicated Telegram chat (similar to Atlas pattern).
        operator_dedicated = bool(
            operator_bot_token
            and operator_chat_id is not None
        )
        operator_chat_app = None
        if operator_dedicated:
            from shared.db.agents import get_agent as _get_agent
            from shared.db.tasks import enqueue_task as _enqueue_task

            def _operator_initiatives_summary() -> str:
                try:
                    content = owner_memory._read(
                        owner_memory.memory_dir / "initiatives.md"
                    )
                    return f"<b>Active Initiatives</b>\n\n{content[:3000]}" if content else "No initiatives loaded."
                except Exception:
                    return "Could not load initiatives.md."

            operator_chat_app = build_operator_application(
                operator_bot_token,
                operator_chat_id,
                enqueue_task=_enqueue_task,
                db_path=db_path,
                get_agent=_get_agent,
                get_initiatives_summary=_operator_initiatives_summary,
            )
            _operator_bot = operator_chat_app.bot
        else:
            if not operator_bot_token or operator_chat_id is None:
                logger.warning(
                    "Operator dedicated chat disabled: set OPERATOR_TELEGRAM_BOT_TOKEN and "
                    "OPERATOR_TELEGRAM_CHAT_ID with a dedicated bot/chat to enable."
                )
            _operator_bot = None

        async def operator_send_fn(text: str) -> None:
            if _operator_bot is not None and operator_chat_id is not None:
                await _operator_bot.send_message(
                    chat_id=operator_chat_id, text=text, parse_mode="HTML"
                )
            else:
                logger.warning("Operator Telegram delivery skipped: dedicated Operator bot/chat is not configured")

        async def _send_approval(
            description: str, task_id: int, request_type: str, payload: dict = None
        ) -> None:
            from apps.roderick.bot.approvals import send_approval_request
            await send_approval_request(
                db_path=db_path,
                bot=application.bot,
                chat_id=main_chat_id,
                description=description,
                task_id=task_id,
                request_type=request_type,
                payload=payload or {},
            )

        merlin.set_notify(send_fn)
        merlin.set_approval_sender(_send_approval)
        forge.set_notify(send_fn)
        forge.set_approval_sender(lambda desc, tid: _send_approval(desc, tid, "plan_approval"))
        venture.set_notify(send_fn)
        venture.set_approval_sender(_send_approval)
        atlas.set_notify(atlas_send_fn)
        sentinel.set_notify(send_fn)
        sentinel.set_approval_sender(_send_approval)
        operator.set_notify(operator_send_fn)
        operator.set_approval_sender(_send_approval)

        async def _start_atlas_chat() -> None:
            if atlas_chat_app is None:
                return
            try:
                await atlas_chat_app.initialize()
                await atlas_chat_app.start()
                await atlas_chat_app.updater.start_polling(allowed_updates=["message"])
                logger.info("Atlas chat polling started")
            except Exception as e:
                logger.error("Atlas chat polling failed: %s", e, exc_info=True)

        async def _start_operator_chat() -> None:
            if operator_chat_app is None:
                return
            try:
                await operator_chat_app.initialize()
                await operator_chat_app.start()
                await operator_chat_app.updater.start_polling(allowed_updates=["message"])
                logger.info("Operator chat polling started")
            except Exception as e:
                logger.error("Operator chat polling failed: %s", e, exc_info=True)

        asyncio.create_task(_start_atlas_chat())
        asyncio.create_task(_start_operator_chat())
        asyncio.create_task(merlin.run())
        asyncio.create_task(forge.run())
        asyncio.create_task(venture.run())
        asyncio.create_task(atlas.run())
        asyncio.create_task(sentinel.run())
        asyncio.create_task(operator.run())

        # Give workers a brief head start to recover abandoned tasks before
        # the scheduler enqueues proactive startup work.
        await asyncio.sleep(2)
        scheduler = RoderickScheduler(
            config=config,
            briefing_fn=orchestrator.morning_briefing_text,
            send_fn=send_fn,
            atlas_daily_lesson_fn=atlas.deliver_daily_lesson,
            ecosystem_council_fn=orchestrator.ecosystem_council_text,
            owner_memory=owner_memory,
            db_path=db_path,
        )
        scheduler.start()

        online_text = "<b>Roderick online.</b> Ready."
        if presence.should_send_telegram("high"):
            await application.bot.send_message(
                chat_id=main_chat_id,
                text=online_text,
                parse_mode="HTML",
            )
        else:
            logger.info("Suppressed startup Telegram ping in mode=%s", presence.get_mode())
        logger.info("Roderick online — chat_id=%s", main_chat_id)

    app.post_init = post_init
    logger.info("Starting Telegram polling…")
    app.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()
