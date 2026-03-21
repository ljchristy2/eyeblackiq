"""
EyeBlackIQ — db_migrate.py
Safe, idempotent migrations for eyeblackiq.db.
Run after pulling new code that adds columns or tables.

Usage:
  python pipeline/db_migrate.py
"""
import sqlite3
import logging
from pathlib import Path

DB_PATH = Path(__file__).parent / "db" / "eyeblackiq.db"
logger = logging.getLogger(__name__)
logging.basicConfig(level="INFO", format="%(levelname)-7s %(message)s")


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def run_migrations():
    if not DB_PATH.exists():
        logger.error(f"DB not found at {DB_PATH} — run db_init.py first")
        return

    with sqlite3.connect(DB_PATH) as conn:
        # ── v1: pick_source + b2b_flag on signals ────────────────────────────
        if not _column_exists(conn, "signals", "pick_source"):
            conn.execute(
                "ALTER TABLE signals ADD COLUMN pick_source TEXT DEFAULT 'SPORTSBOOK'"
            )
            logger.info("signals: added pick_source (default SPORTSBOOK)")
        else:
            logger.info("signals.pick_source — already exists, skip")

        if not _column_exists(conn, "signals", "b2b_flag"):
            conn.execute(
                "ALTER TABLE signals ADD COLUMN b2b_flag TEXT"
            )
            logger.info("signals: added b2b_flag (nullable)")
        else:
            logger.info("signals.b2b_flag — already exists, skip")

        conn.commit()

    logger.info("All migrations complete.")


if __name__ == "__main__":
    run_migrations()
