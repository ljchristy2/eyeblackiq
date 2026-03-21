"""
EyeBlackIQ — db_init.py
Initializes eyeblackiq.db with all tables per spec §4.
Run once: python pipeline/db_init.py
"""
import sqlite3
import os
from pathlib import Path

DB_PATH = Path(__file__).parent / "db" / "eyeblackiq.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

SCHEMA = """
-- ─────────────────────────────────────────────
--  TABLE: signals
--  One row per model-generated pick, pre-result
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_date      TEXT    NOT NULL,
    sport            TEXT    NOT NULL,   -- NCAA_BASEBALL | NHL | MLB | SOCCER | HANDBALL
    game             TEXT    NOT NULL,   -- "Away @ Home" or "Home vs Away"
    game_time        TEXT,
    bet_type         TEXT    NOT NULL,   -- ML | F5 | PROP | TOTAL | PARLAY
    side             TEXT    NOT NULL,   -- team name or player name + direction
    market           TEXT,              -- e.g. "Strikeouts O3.5" or "ML"
    odds             INTEGER,           -- American odds
    model_prob       REAL    NOT NULL,  -- 0.0–1.0
    no_vig_prob      REAL,
    edge             REAL    NOT NULL,  -- model_prob - no_vig_prob
    ev               REAL,
    tier             TEXT,              -- FILTHY | WHEELHOUSE | SCOUT | BALK (or sport equiv)
    units            REAL    DEFAULT 1.0,
    is_pod           INTEGER DEFAULT 0, -- 1 if POD pick
    pod_sport        TEXT,
    correlated_parlay_id INTEGER,       -- FK to parlays.id if part of parlay
    gate1_pyth       TEXT    DEFAULT 'PASS',
    gate2_edge       TEXT    DEFAULT 'PASS',
    gate3_model_agree TEXT   DEFAULT 'PASS',
    gate4_line_move  TEXT    DEFAULT 'PASS',
    gate5_etl_fresh  TEXT    DEFAULT 'PASS',
    notes            TEXT,
    created_at       TEXT    NOT NULL
);

-- ─────────────────────────────────────────────
--  TABLE: results
--  Filled in by grade.py after games complete
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS results (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id        INTEGER NOT NULL REFERENCES signals(id),
    signal_date      TEXT    NOT NULL,
    sport            TEXT    NOT NULL,
    game             TEXT    NOT NULL,
    side             TEXT    NOT NULL,
    market           TEXT,
    odds             INTEGER,
    units            REAL,
    result           TEXT    NOT NULL,  -- WIN | LOSS | PUSH | VOID | PENDING
    units_net        REAL,              -- +/- units after result
    actual_val       TEXT,              -- actual score/stat for props
    closing_line     INTEGER,           -- Pinnacle no-vig closing ML
    clv              REAL,              -- closing line value (positive = beat close)
    notes            TEXT,
    graded_at        TEXT
);

-- ─────────────────────────────────────────────
--  TABLE: parlays
--  Correlated parlays tracked as a unit
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS parlays (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    parlay_date      TEXT    NOT NULL,
    sport            TEXT,
    label            TEXT    NOT NULL,  -- "USC ML + Under 9.5"
    leg1_desc        TEXT    NOT NULL,
    leg2_desc        TEXT,
    leg3_desc        TEXT,
    parlay_odds      INTEGER NOT NULL,  -- American combined odds
    parlay_decimal   REAL    NOT NULL,
    model_prob       REAL    NOT NULL,  -- P(parlay)
    ev               REAL,
    units            REAL    DEFAULT 1.5,
    correlation_note TEXT,              -- "SP dominance → run suppression"
    result           TEXT    DEFAULT 'PENDING',
    units_net        REAL,
    created_at       TEXT    NOT NULL
);

-- ─────────────────────────────────────────────
--  TABLE: backtest_log
--  Walk-forward backtest results by model/season
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS backtest_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date         TEXT    NOT NULL,
    sport            TEXT    NOT NULL,
    model_version    TEXT    NOT NULL,  -- e.g. "v1.0"
    season_range     TEXT    NOT NULL,  -- "2021-2024"
    n_bets           INTEGER,
    roi_pct          REAL,
    clv_pct          REAL,              -- % of bets that beat closing line
    cal_max_err      REAL,              -- max calibration bin error
    go_live_cleared  INTEGER DEFAULT 0, -- 1 = all 4 thresholds cleared
    notes            TEXT,
    created_at       TEXT    NOT NULL
);

-- ─────────────────────────────────────────────
--  TABLE: pod_records
--  POD pick per sport per date, with tracking
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pod_records (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    date             TEXT    NOT NULL,
    sport            TEXT    NOT NULL,
    label            TEXT    NOT NULL,
    pick             TEXT    NOT NULL,
    line             REAL,
    side             TEXT,
    odds             INTEGER,
    model_prob       REAL,
    edge             REAL,
    ev               REAL,
    tier             TEXT,
    units            REAL    DEFAULT 2.0,
    game             TEXT,
    game_time        TEXT,
    result           TEXT    DEFAULT 'PENDING',
    actual_val       TEXT,
    units_net        REAL,
    created_at       TEXT    NOT NULL,
    UNIQUE(date, sport)
);

-- ─────────────────────────────────────────────
--  TABLE: etl_log
--  Tracks every data pull timestamp (Gate 5)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source           TEXT    NOT NULL,  -- "therundown" | "fdcu" | "moneypuck"
    table_name       TEXT    NOT NULL,
    rows_loaded      INTEGER,
    as_of_ts         TEXT,              -- data-as-of timestamp
    run_ts           TEXT    NOT NULL   -- when the ETL ran
);
"""

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(SCHEMA)
        conn.commit()

    # Verify tables
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r[0] for r in cur.fetchall()]

    print(f"[OK] eyeblackiq.db initialized at: {DB_PATH}")
    print(f"   Tables: {', '.join(tables)}")
    return tables

if __name__ == "__main__":
    tables = init_db()
    print(f"\n[OK] DB ready -- {len(tables)} tables created")
