import shutil
import sqlite3
import time
from pathlib import Path


def get_db_path(config: dict) -> str:
    return str(Path(config.get("db_dir", config["data_dir"])) / "roderick.db")


def seed_db_if_needed(db_path: str, seed_path: str | None = None) -> None:
    """Seed a fresh runtime DB from an existing host-side copy when available."""
    target = Path(db_path)
    if target.exists() and target.stat().st_size > 0:
        return
    if not seed_path:
        return
    source = Path(seed_path)
    if not source.exists() or not source.is_file() or source.resolve() == target.resolve():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def init_db(db_path: str) -> None:
    """Create all tables and run migrations. Idempotent — safe on every startup."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    attempts = 5
    last_error: sqlite3.OperationalError | None = None
    for attempt in range(1, attempts + 1):
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.OperationalError:
                # Some Docker bind mounts cannot create SQLite WAL sidecar files.
                # Fall back to the simpler rollback journal instead of crashing startup.
                conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS reminders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                text        TEXT NOT NULL,
                due         TEXT,
                category    TEXT DEFAULT 'personal',
                done        INTEGER DEFAULT 0,
                recurring   TEXT,
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                from_agent        TEXT NOT NULL DEFAULT 'roderick',
                to_agent          TEXT NOT NULL,
                task_type         TEXT NOT NULL,
                description       TEXT NOT NULL,
                status            TEXT NOT NULL DEFAULT 'pending',
                priority          TEXT DEFAULT 'normal',
                urgency           TEXT DEFAULT 'this_week',
                domain            TEXT DEFAULT 'operations',
                payload           TEXT DEFAULT '{}',
                result            TEXT,
                approval_required INTEGER DEFAULT 0,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS agent_registry (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                name                 TEXT UNIQUE NOT NULL,
                display_name         TEXT NOT NULL,
                purpose              TEXT,
                status               TEXT DEFAULT 'unknown',
                autonomy_level       TEXT DEFAULT 'supervised',
                model_used           TEXT DEFAULT 'claude-sonnet-4-6',
                task_types_accepted  TEXT DEFAULT '[]',
                report_types_produced TEXT DEFAULT '[]',
                last_run             TEXT,
                last_heartbeat       TEXT,
                last_success         TEXT,
                last_error           TEXT,
                last_message         TEXT,
                last_report          TEXT,
                config               TEXT DEFAULT '{}',
                updated_at           TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS approval_requests (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id             INTEGER,
                request_type        TEXT NOT NULL,
                description         TEXT NOT NULL,
                payload             TEXT DEFAULT '{}',
                status              TEXT DEFAULT 'pending',
                telegram_message_id INTEGER,
                callback_data       TEXT UNIQUE,
                created_at          TEXT NOT NULL,
                resolved_at         TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks(id)
            );

            CREATE TABLE IF NOT EXISTS agent_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                from_agent  TEXT NOT NULL,
                to_agent    TEXT NOT NULL,
                message     TEXT NOT NULL,
                priority    TEXT DEFAULT 'normal',
                read        INTEGER DEFAULT 0,
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type  TEXT NOT NULL,
                agent       TEXT NOT NULL,
                payload     TEXT DEFAULT '{}',
                processed   INTEGER DEFAULT 0,
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS improvements (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                title                TEXT NOT NULL,
                description          TEXT NOT NULL DEFAULT '',
                origin_agent         TEXT NOT NULL DEFAULT '',
                origin_signal        TEXT NOT NULL DEFAULT 'manual',
                status               TEXT NOT NULL DEFAULT 'signal',
                evidence             TEXT DEFAULT '{}',
                merlin_task_id       INTEGER,
                forge_task_id        INTEGER,
                sentinel_task_id     INTEGER,
                priority             TEXT DEFAULT 'normal',
                risk_level           TEXT DEFAULT 'unknown',
                affected_components  TEXT DEFAULT '[]',
                forge_recommended    INTEGER DEFAULT 0,
                created_at           TEXT NOT NULL,
                updated_at           TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS agent_behavior_policies (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                agent         TEXT NOT NULL,
                policy_key    TEXT NOT NULL,
                policy_value  TEXT NOT NULL,
                description   TEXT NOT NULL DEFAULT '',
                status        TEXT NOT NULL DEFAULT 'proposed',
                origin        TEXT NOT NULL DEFAULT 'user',
                changed_by    TEXT NOT NULL DEFAULT 'roderick',
                requires_approval INTEGER NOT NULL DEFAULT 0,
                approved_by   TEXT,
                applied_at    TEXT,
                expires_at    TEXT,
                audit_notes   TEXT DEFAULT '',
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL,
                UNIQUE(agent, policy_key)
            );

            CREATE TABLE IF NOT EXISTS forge_artifacts (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id          INTEGER NOT NULL,
                artifact_type    TEXT NOT NULL,
                artifact_root    TEXT NOT NULL DEFAULT '',
                relative_path    TEXT NOT NULL DEFAULT '',
                path             TEXT NOT NULL,
                summary          TEXT NOT NULL DEFAULT '',
                approval_state   TEXT NOT NULL DEFAULT 'unknown',
                validation_state TEXT NOT NULL DEFAULT 'pending',
                metadata         TEXT DEFAULT '{}',
                created_at       TEXT NOT NULL,
                FOREIGN KEY (task_id) REFERENCES tasks(id)
            );
            """)
            _migrate(conn)
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            last_error = exc
            conn.close()
            if "unable to open database file" not in str(exc).lower():
                raise
            if attempt == attempts:
                raise
            time.sleep(min(5, attempt))
        else:
            conn.close()
    if last_error:
        raise last_error


def _migrate(conn: sqlite3.Connection) -> None:
    """
    Add columns that were introduced after the initial schema.
    Safe to run against existing databases — skips columns that already exist.
    """
    # tasks: urgency + domain (added in v2)
    _add_col(conn, "tasks",         "urgency", "TEXT DEFAULT 'this_week'")
    _add_col(conn, "tasks",         "domain",  "TEXT DEFAULT 'operations'")

    # agent_registry: extended fields (added in v2)
    _add_col(conn, "agent_registry", "autonomy_level",        "TEXT DEFAULT 'supervised'")
    _add_col(conn, "agent_registry", "model_used",            "TEXT DEFAULT 'claude-sonnet-4-6'")
    _add_col(conn, "agent_registry", "task_types_accepted",   "TEXT DEFAULT '[]'")
    _add_col(conn, "agent_registry", "report_types_produced", "TEXT DEFAULT '[]'")
    _add_col(conn, "agent_registry", "last_heartbeat",        "TEXT")
    _add_col(conn, "agent_registry", "last_success",          "TEXT")
    _add_col(conn, "agent_registry", "last_error",            "TEXT")
    # agent introspection fields (added in v3)
    _add_col(conn, "agent_registry", "current_task_id",       "INTEGER")
    _add_col(conn, "agent_registry", "current_model",         "TEXT")
    _add_col(conn, "agent_registry", "state_confidence",      "REAL DEFAULT 1.0")


def _add_col(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
