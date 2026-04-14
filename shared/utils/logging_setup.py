import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(data_dir: str, level: int = logging.INFO) -> None:
    """File + stdout rolling logger. Call once at startup."""
    log_dir = Path(data_dir).parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    stdout = logging.StreamHandler()
    stdout.setFormatter(fmt)
    root.addHandler(stdout)

    file_handler = RotatingFileHandler(
        str(log_dir / "roderick.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    # Quiet noisy third-party loggers
    for name in ("httpx", "httpcore", "telegram", "apscheduler"):
        logging.getLogger(name).setLevel(logging.WARNING)
