import json
import os
from pathlib import Path


def load_config() -> dict:
    """Load config/roderick.json and apply environment overrides. Startup-only — not hot-reloaded."""
    config_path = _find_config_file()
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    # Environment overrides
    if os.environ.get("CLAUDE_MODEL"):
        config.setdefault("llm", {})["model"] = os.environ["CLAUDE_MODEL"]
    if os.environ.get("MAX_HISTORY"):
        config.setdefault("llm", {})["max_history"] = int(os.environ["MAX_HISTORY"])
    if os.environ.get("DEVOPS_ROOT"):
        config["devops_root"] = os.environ["DEVOPS_ROOT"]
    if os.environ.get("DATA_DIR"):
        config["data_dir"] = os.environ["DATA_DIR"]
    if os.environ.get("DB_DIR"):
        config["db_dir"] = os.environ["DB_DIR"]

    # Always set from env — these are secrets, not in JSON
    config["telegram_bot_token"] = os.environ["TELEGRAM_BOT_TOKEN"]
    config["anthropic_api_key"] = os.environ.get("ANTHROPIC_API_KEY", "")  # optional — only needed for LLM_PROVIDER=anthropic
    config["authorized_chat_id"] = int(os.environ["AUTHORIZED_CHAT_ID"])

    if os.environ.get("MEMORY_DIR"):
        config["memory_dir"] = os.environ["MEMORY_DIR"]

    # Resolve data_dir to absolute path and ensure it exists
    config["data_dir"] = str(_resolve_dir(config.get("data_dir", "data")))
    config["db_dir"] = str(_resolve_dir(config.get("db_dir", config["data_dir"])))

    return config


def _find_config_file() -> Path:
    candidates = [
        Path.cwd() / "config" / "roderick.json",
        Path(__file__).parent.parent.parent / "config" / "roderick.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        f"config/roderick.json not found. Searched: {[str(p) for p in candidates]}"
    )


def _resolve_dir(data_dir: str) -> Path:
    p = Path(data_dir)
    if not p.is_absolute():
        repo_root = Path(__file__).parent.parent.parent
        p = repo_root / p
    p.mkdir(parents=True, exist_ok=True)
    return p


def agents_config_path() -> Path:
    """Return path to config/agents.json."""
    candidates = [
        Path.cwd() / "config" / "agents.json",
        Path(__file__).parent.parent.parent / "config" / "agents.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("config/agents.json not found.")
