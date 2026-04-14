import sqlite3
import time
from pathlib import Path


def connect_sqlite(db_path: str, *, timeout: int = 30, row_factory=None, attempts: int = 3, backoff_seconds: float = 0.5) -> sqlite3.Connection:
    """Open SQLite with a small retry window for transient Windows/Docker bind-mount failures."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    last_error: sqlite3.OperationalError | None = None
    for attempt in range(1, attempts + 1):
        try:
            conn = sqlite3.connect(db_path, timeout=timeout)
            if row_factory is not None:
                conn.row_factory = row_factory
            return conn
        except sqlite3.OperationalError as exc:
            last_error = exc
            if "unable to open database file" not in str(exc).lower() or attempt == attempts:
                raise
            time.sleep(backoff_seconds * attempt)
    if last_error:
        raise last_error
    raise sqlite3.OperationalError("unable to open database file")
