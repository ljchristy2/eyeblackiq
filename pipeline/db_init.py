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
    pick_source      TEXT    DEFAULT 'SPORTSBOOK', -- SPORTSBOOK | PICKEM
    b2b_flag         TEXT,                          -- OPPONENT_B2B | PLAYER_B2B | NULL
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

-- ─────────────────────────────────────────────
--  HANDBALL TABLES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS handball_matches (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id          TEXT UNIQUE,
    date             TEXT NOT NULL,
    league_id        INTEGER,
    league_name      TEXT,
    season           TEXT,
    home_team        TEXT NOT NULL,
    away_team        TEXT NOT NULL,
    home_score       INTEGER,
    away_score       INTEGER,
    total_goals      INTEGER,
    home_possession  REAL,
    away_possession  REAL,
    home_shots       INTEGER,
    away_shots       INTEGER,
    home_shots_on_goal INTEGER,
    away_shots_on_goal INTEGER,
    home_odds        REAL,
    away_odds        REAL,
    draw_odds        REAL,
    total_line       REAL,
    over_odds        REAL,
    under_odds       REAL,
    game_time        TEXT,
    status           TEXT DEFAULT 'FT',
    source           TEXT DEFAULT 'API_SPORTS',
    created_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS handball_team_stats (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    season                      TEXT NOT NULL,
    league_id                   INTEGER,
    team_name                   TEXT NOT NULL,
    games_played                INTEGER DEFAULT 0,
    wins                        INTEGER DEFAULT 0,
    losses                      INTEGER DEFAULT 0,
    draws                       INTEGER DEFAULT 0,
    goals_for                   REAL DEFAULT 0,
    goals_against               REAL DEFAULT 0,
    possessions_per_game        REAL DEFAULT 52.0,
    shots_per_game              REAL DEFAULT 30.0,
    shot_efficiency             REAL DEFAULT 0.568,
    def_goals_allowed_per_shot  REAL DEFAULT 0.568,
    elo_rating                  REAL DEFAULT 1500,
    last_updated                TEXT,
    UNIQUE(season, league_id, team_name)
);

CREATE TABLE IF NOT EXISTS handball_odds (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id          TEXT,
    bookmaker        TEXT,
    market           TEXT,
    home_odds        REAL,
    draw_odds        REAL,
    away_odds        REAL,
    total_line       REAL,
    over_odds        REAL,
    under_odds       REAL,
    recorded_at      TEXT,
    source           TEXT DEFAULT 'API_SPORTS',
    UNIQUE(game_id, bookmaker, market)
);

-- ─────────────────────────────────────────────
--  CRICKET TABLES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cricket_matches (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id       TEXT UNIQUE,
    date             TEXT NOT NULL,
    league_id        INTEGER,
    league_name      TEXT,
    format           TEXT NOT NULL DEFAULT 'T20',
    season           TEXT,
    home_team        TEXT NOT NULL,
    away_team        TEXT NOT NULL,
    home_score       INTEGER,
    away_score       INTEGER,
    venue            TEXT,
    toss_winner      TEXT,
    toss_decision    TEXT,
    result           TEXT,
    winner           TEXT,
    margin_runs      INTEGER,
    margin_wickets   INTEGER,
    home_odds        REAL,
    away_odds        REAL,
    total_line       REAL,
    over_odds        REAL,
    under_odds       REAL,
    game_time        TEXT,
    status           TEXT DEFAULT 'FT',
    source           TEXT DEFAULT 'CRICSHEET',
    created_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cricket_innings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id       TEXT,
    innings_number   INTEGER,
    batting_team     TEXT,
    bowling_team     TEXT,
    runs             INTEGER,
    wickets          INTEGER,
    overs            REAL,
    extras           INTEGER,
    run_rate         REAL,
    source           TEXT DEFAULT 'CRICSHEET'
);

CREATE TABLE IF NOT EXISTS cricket_team_stats (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    season                      TEXT NOT NULL,
    league_id                   INTEGER DEFAULT 0,
    format                      TEXT NOT NULL DEFAULT 'T20',
    team_name                   TEXT NOT NULL,
    games_played                INTEGER DEFAULT 0,
    wins                        INTEGER DEFAULT 0,
    losses                      INTEGER DEFAULT 0,
    avg_score_batting_first     REAL DEFAULT 165.0,
    avg_score_batting_second    REAL DEFAULT 155.0,
    avg_runs_conceded           REAL DEFAULT 165.0,
    win_pct_batting_first       REAL DEFAULT 0.5,
    win_pct_batting_second      REAL DEFAULT 0.5,
    toss_win_pct                REAL DEFAULT 0.5,
    elo_rating                  REAL DEFAULT 1500,
    last_updated                TEXT,
    UNIQUE(season, league_id, format, team_name)
);

CREATE TABLE IF NOT EXISTS cricket_venue_stats (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    venue                   TEXT UNIQUE,
    format                  TEXT DEFAULT 'T20',
    avg_first_innings_score REAL DEFAULT 165.0,
    std_first_innings_score REAL DEFAULT 22.0,
    avg_total_runs          REAL DEFAULT 330.0,
    matches_played          INTEGER DEFAULT 0,
    pace_friendly           REAL DEFAULT 0.5,
    boundary_percentage     REAL DEFAULT 0.0,
    last_updated            TEXT
);

CREATE TABLE IF NOT EXISTS cricket_players (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    player_name      TEXT NOT NULL,
    team             TEXT,
    season           TEXT,
    format           TEXT DEFAULT 'T20',
    matches          INTEGER DEFAULT 0,
    innings          INTEGER DEFAULT 0,
    runs_total       INTEGER DEFAULT 0,
    avg_runs         REAL DEFAULT 0,
    strike_rate      REAL DEFAULT 0,
    batting_position INTEGER DEFAULT 5,
    balls_faced_avg  REAL DEFAULT 15,
    wicket_rate      REAL DEFAULT 0.045,
    last_updated     TEXT,
    UNIQUE(player_name, team, season, format)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_hm_date    ON handball_matches(date);
CREATE INDEX IF NOT EXISTS idx_hm_teams   ON handball_matches(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_hts_team   ON handball_team_stats(team_name, season);
CREATE INDEX IF NOT EXISTS idx_cm_date    ON cricket_matches(date, format);
CREATE INDEX IF NOT EXISTS idx_cm_teams   ON cricket_matches(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_cts_team   ON cricket_team_stats(team_name, format, season);
CREATE INDEX IF NOT EXISTS idx_cvs_venue  ON cricket_venue_stats(venue);
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
