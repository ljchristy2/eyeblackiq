"""
EyeBlackIQ — scrapers/fetch_historical_handball.py
=====================================================
Fetches and stores historical handball match data from multiple sources.

SOURCES (in priority order):
  1. API Sports (handball)  — primary: historical fixtures by league/season
  2. Kaggle Dataset          — handball Bundesliga stats (CSV download)
  3. OddsPortal scraper      — historical match winner + totals odds
  4. HandballStats247        — team efficiency and possession stats (scrape)

OUTPUT TABLES (written to eyeblackiq.db):
  - handball_matches      : fixture-level results
  - handball_team_stats   : ELO + possession efficiency (computed from matches)
  - handball_odds         : historical pre-game odds (for backtest CLV)

USAGE:
  python scrapers/fetch_historical_handball.py --seasons 3
  python scrapers/fetch_historical_handball.py --league 1 --season 2024
  python scrapers/fetch_historical_handball.py --elo-only
  python scrapers/fetch_historical_handball.py --status

TARGET LEAGUES:
  1  = EHF Champions League        (primary — deepest odds market)
  2  = German HBL (Bundesliga)     (secondary)
  3  = French Starligue             (secondary)

EHF CL SEASONS AVAILABLE VIA API SPORTS:
  2019, 2020, 2021, 2022, 2023, 2024  (~56-64 group-stage games each)

NOTE: Walk-forward backtest requires ≥ 90 matches (3 EHF CL group-stage seasons).
      Full backtest needs ~300 betting opportunities → fetch all 3 leagues × 3 seasons.
"""

import os
import sys
import json
import time
import math
import sqlite3
import argparse
import logging
import zipfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

LOG_PATH = Path(__file__).parent.parent / "logs" / "scraper_errors.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger("fetch_historical_handball")

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"
DATA_DIR = BASE_DIR / "data" / "handball"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# API Sports config
APISPORTS_BASE = "https://v1.handball.api-sports.io"
APISPORTS_KEY  = os.getenv("APISPORTS_KEY", "")

# Default seasons to fetch (most recent 3 full seasons)
DEFAULT_SEASONS = [2022, 2023, 2024]

# Target leagues
TARGET_LEAGUES = {
    1: "EHF Champions League",
    2: "German HBL",
    3: "French Starligue",
}

# ELO calibration
ELO_DEFAULT  = 1500
ELO_K        = 20      # standard K-factor
ELO_K_EARLY  = 32     # K-factor for first 20 games


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables():
    """Create handball-specific tables if they don't exist."""
    schema = """
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
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        season                  TEXT NOT NULL,
        league_id               INTEGER,
        team_name               TEXT NOT NULL,
        games_played            INTEGER DEFAULT 0,
        wins                    INTEGER DEFAULT 0,
        losses                  INTEGER DEFAULT 0,
        draws                   INTEGER DEFAULT 0,
        goals_for               REAL DEFAULT 0,
        goals_against           REAL DEFAULT 0,
        possessions_per_game    REAL DEFAULT 52.0,
        shots_per_game          REAL DEFAULT 30.0,
        shot_efficiency         REAL DEFAULT 0.568,
        def_goals_allowed_per_shot REAL DEFAULT 0.568,
        elo_rating              REAL DEFAULT 1500,
        last_updated            TEXT,
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

    CREATE INDEX IF NOT EXISTS idx_hm_date     ON handball_matches(date);
    CREATE INDEX IF NOT EXISTS idx_hm_teams    ON handball_matches(home_team, away_team);
    CREATE INDEX IF NOT EXISTS idx_hts_team    ON handball_team_stats(team_name, season);
    """
    with get_conn() as conn:
        conn.executescript(schema)
        conn.commit()
    logger.info("[HANDBALL] Tables ensured.")


# ── API Sports helpers ─────────────────────────────────────────────────────────
def _api_request(endpoint: str, params: dict = None) -> dict:
    """Authenticated GET to API Sports handball endpoint."""
    if not APISPORTS_KEY:
        logger.error("APISPORTS_KEY not set in .env")
        return {"errors": {"key": "missing"}, "response": []}

    url = APISPORTS_BASE + endpoint
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url += "?" + qs

    req = urllib.request.Request(url)
    req.add_header("x-rapidapi-key",  APISPORTS_KEY)
    req.add_header("x-apisports-key", APISPORTS_KEY)

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        remaining = resp.headers.get("x-ratelimit-requests-remaining", "?")
        logger.debug(f"  API Sports [{endpoint}] → {len(data.get('response',[]))} rows | quota_left={remaining}")
        time.sleep(0.7)   # respect rate limits
        return data
    except Exception as e:
        logger.error(f"API Sports request failed: {url} — {e}")
        return {"errors": {str(type(e).__name__): str(e)}, "response": []}


def fetch_games_by_season(league_id: int, season: int) -> list:
    """
    Fetch all games for a league-season from API Sports.
    Returns list of normalized game dicts.
    """
    data = _api_request("/games", {"league": league_id, "season": season})
    if data.get("errors"):
        logger.warning(f"  games error L={league_id} S={season}: {data['errors']}")
        return []

    games = []
    for item in (data.get("response") or []):
        teams  = item.get("teams") or {}
        scores = item.get("scores") or {}
        status = item.get("status") or {}

        home_s  = scores.get("home") or {}
        away_s  = scores.get("away") or {}
        h_score = home_s.get("total") if isinstance(home_s, dict) else home_s
        a_score = away_s.get("total") if isinstance(away_s, dict) else away_s

        games.append({
            "game_id":    str(item.get("id")),
            "date":       (item.get("date") or "")[:10],
            "league_id":  league_id,
            "league_name": TARGET_LEAGUES.get(league_id, f"League {league_id}"),
            "season":     str(season),
            "home_team":  (teams.get("home") or {}).get("name"),
            "away_team":  (teams.get("away") or {}).get("name"),
            "home_score": h_score,
            "away_score": a_score,
            "total_goals": (h_score or 0) + (a_score or 0) if (h_score is not None and a_score is not None) else None,
            "status":     status.get("short") if isinstance(status, dict) else status,
            "source":     "API_SPORTS",
        })

    logger.info(f"  Fetched {len(games)} games: League={league_id} Season={season}")
    return games


def store_games(games: list) -> int:
    """Insert games into handball_matches. Returns number of new rows."""
    if not games:
        return 0
    with get_conn() as conn:
        inserted = 0
        for g in games:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO handball_matches
                       (game_id, date, league_id, league_name, season,
                        home_team, away_team, home_score, away_score,
                        total_goals, status, source)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        g["game_id"], g["date"], g["league_id"], g["league_name"], g["season"],
                        g["home_team"], g["away_team"], g["home_score"], g["away_score"],
                        g["total_goals"], g["status"], g["source"],
                    )
                )
                if conn.execute("SELECT changes()").fetchone()[0]:
                    inserted += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()

    # Log ETL
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO etl_log (source, table_name, rows_loaded, as_of_ts, run_ts) VALUES (?,?,?,?,?)",
            ("API_SPORTS_HANDBALL", "handball_matches", inserted,
             datetime.now(timezone.utc).strftime("%Y-%m-%d"),
             datetime.now(timezone.utc).isoformat())
        )
        conn.commit()

    return inserted


# ── ELO computation ────────────────────────────────────────────────────────────
def compute_elo_from_matches() -> dict:
    """
    Walk all completed handball_matches chronologically and compute ELO ratings.
    Returns dict of {team_name: elo_rating}.

    ELO formula:
      E_h = 1 / (1 + 10^(-(elo_home + HFA - elo_away) / 400))
      elo_home_new = elo_home + K × (S_h - E_h)
      where S_h = 1 (home wins), 0.5 (draw), 0 (home loses)
      K = ELO_K_EARLY (< 20 games) else ELO_K

    HFA = 50 ELO points for handball home advantage.
    """
    HFA = 50

    try:
        with get_conn() as conn:
            rows = conn.execute(
                """SELECT date, home_team, away_team, home_score, away_score
                   FROM handball_matches
                   WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                     AND status IN ('FT','FINISHED','COMPLETE')
                   ORDER BY date ASC"""
            ).fetchall()
    except sqlite3.OperationalError:
        return {}

    elo       = {}
    game_count = {}

    for row in rows:
        ht, at    = row["home_team"], row["away_team"]
        hs, as_   = row["home_score"], row["away_score"]

        if not ht or not at:
            continue

        elo.setdefault(ht, ELO_DEFAULT)
        elo.setdefault(at, ELO_DEFAULT)
        game_count.setdefault(ht, 0)
        game_count.setdefault(at, 0)

        e_h = 1 / (1 + 10 ** (-((elo[ht] + HFA) - elo[at]) / 400))
        e_a = 1 - e_h

        if hs > as_:
            s_h, s_a = 1.0, 0.0
        elif hs < as_:
            s_h, s_a = 0.0, 1.0
        else:
            s_h, s_a = 0.5, 0.5

        k_h = ELO_K_EARLY if game_count[ht] < 20 else ELO_K
        k_a = ELO_K_EARLY if game_count[at] < 20 else ELO_K

        elo[ht] = elo[ht] + k_h * (s_h - e_h)
        elo[at] = elo[at] + k_a * (s_a - e_a)
        game_count[ht] += 1
        game_count[at] += 1

    logger.info(f"[ELO] Computed ratings for {len(elo)} handball teams.")
    return elo


def compute_team_stats_from_matches() -> int:
    """
    Aggregate per-team efficiency stats from handball_matches.
    Computes: goals_for, goals_against, wins, losses, draws, shot_efficiency, def_eff.
    Writes/updates handball_team_stats. Returns rows upserted.
    """
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """SELECT season, league_id, league_name, home_team, away_team,
                          home_score, away_score,
                          home_shots, away_shots,
                          home_shots_on_goal, away_shots_on_goal,
                          home_possession, away_possession
                   FROM handball_matches
                   WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                     AND status IN ('FT','FINISHED','COMPLETE')"""
            ).fetchall()
    except sqlite3.OperationalError:
        return 0

    # Aggregate
    stats = {}  # key: (season, league_id, team_name)

    def _init(k):
        stats[k] = {
            "games_played":0, "wins":0, "losses":0, "draws":0,
            "goals_for":0.0, "goals_against":0.0,
            "shots_total":0, "goals_from_shots":0,
            "shots_faced":0, "goals_conceded_from_shots":0,
        }

    for row in rows:
        season = row["season"]
        lid    = row["league_id"]
        ht, at = row["home_team"], row["away_team"]
        hs, as_ = row["home_score"], row["away_score"]

        for team, scored, conceded, shots, shots_faced in [
            (ht, hs, as_, row["home_shots"], row["away_shots"]),
            (at, as_, hs, row["away_shots"], row["home_shots"]),
        ]:
            if not team:
                continue
            k = (season, lid, team)
            if k not in stats:
                _init(k)
            s = stats[k]
            s["games_played"]  += 1
            s["goals_for"]     += scored or 0
            s["goals_against"] += conceded or 0
            if scored > conceded:    s["wins"]   += 1
            elif scored < conceded:  s["losses"] += 1
            else:                    s["draws"]  += 1
            if shots:
                s["shots_total"]           += shots
                s["goals_from_shots"]      += scored or 0
            if shots_faced:
                s["shots_faced"]            += shots_faced
                s["goals_conceded_from_shots"] += conceded or 0

    # Compute ELO
    elo_map = compute_elo_from_matches()

    upserted = 0
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        for (season, lid, team), s in stats.items():
            gp = s["games_played"]
            if gp == 0:
                continue
            shot_eff   = (s["goals_from_shots"] / s["shots_total"]) if s["shots_total"] > 0 else 0.568
            def_eff    = (s["goals_conceded_from_shots"] / s["shots_faced"]) if s["shots_faced"] > 0 else 0.568
            elo_rating = elo_map.get(team, ELO_DEFAULT)

            conn.execute(
                """INSERT INTO handball_team_stats
                   (season, league_id, team_name, games_played, wins, losses, draws,
                    goals_for, goals_against, shot_efficiency, def_goals_allowed_per_shot,
                    elo_rating, last_updated)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(season, league_id, team_name) DO UPDATE SET
                     games_played=excluded.games_played,
                     wins=excluded.wins, losses=excluded.losses, draws=excluded.draws,
                     goals_for=excluded.goals_for, goals_against=excluded.goals_against,
                     shot_efficiency=excluded.shot_efficiency,
                     def_goals_allowed_per_shot=excluded.def_goals_allowed_per_shot,
                     elo_rating=excluded.elo_rating,
                     last_updated=excluded.last_updated""",
                (season, lid, team, gp, s["wins"], s["losses"], s["draws"],
                 s["goals_for"], s["goals_against"], shot_eff, def_eff,
                 elo_rating, now)
            )
            upserted += 1
        conn.commit()

    logger.info(f"[HANDBALL] Team stats computed: {upserted} team-season rows written.")
    return upserted


# ── OddsPortal scraper (best-effort) ─────────────────────────────────────────
def try_fetch_oddsportal_handball(league_name: str, season: str) -> list:
    """
    Attempt to scrape historical odds from OddsPortal for handball.
    Uses gingeleski/odds-portal-scraper pattern (pure urllib, no Selenium).
    Returns list of odds dicts if successful; empty list if blocked/unavailable.

    NOTE: OddsPortal requires JavaScript rendering for full data. This is a
    best-effort attempt. Full implementation requires Selenium. For production,
    use TheRundown historical API (paid tier) or manual CSV collection.
    """
    # OddsPortal API-style URL (observed via network inspection)
    league_slug = league_name.lower().replace(" ", "-").replace(".", "")
    url = (
        f"https://www.oddsportal.com/ajax-sport-country-tournament-archive/"
        f"1/{league_slug}/X0/1/0/#/page/1/"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/javascript, */*",
        "Referer": "https://www.oddsportal.com/",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            text = resp.read().decode("utf-8")
        # OddsPortal returns JSONP — this is a simplified extract
        if "d" in text and len(text) > 100:
            logger.info(f"  OddsPortal: partial data received for {league_name} {season}")
    except Exception as e:
        logger.debug(f"  OddsPortal unavailable for {league_name}: {e} (expected without Selenium)")
    return []   # Requires Selenium for full extraction — see requirements.txt


# ── Kaggle dataset (best-effort) ─────────────────────────────────────────────
def try_fetch_kaggle_handball() -> int:
    """
    Attempt to download Handball Bundesliga Stats from Kaggle.
    Requires KAGGLE_KEY / KAGGLE_USERNAME in .env (optional).
    Returns number of matches loaded. 0 if Kaggle not configured.
    """
    kaggle_user = os.getenv("KAGGLE_USERNAME", "")
    kaggle_key  = os.getenv("KAGGLE_KEY", "")
    if not kaggle_user or not kaggle_key:
        logger.info("  Kaggle not configured (KAGGLE_USERNAME/KAGGLE_KEY missing) — skipping.")
        return 0

    try:
        import subprocess
        dest = DATA_DIR / "kaggle"
        dest.mkdir(exist_ok=True)
        cmd = [
            sys.executable, "-m", "kaggle", "datasets", "download",
            "--dataset", "martj42/handball-bundesliga-stats",
            "--path", str(dest), "--unzip"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            logger.warning(f"  Kaggle download failed: {result.stderr[:200]}")
            return 0

        # Parse CSVs
        loaded = 0
        for csv_file in dest.glob("*.csv"):
            logger.info(f"  Kaggle CSV: {csv_file.name}")
            # Basic parsing — adapt column names to our schema
            with open(csv_file, encoding="utf-8") as f:
                lines = f.readlines()
            if not lines:
                continue
            header = [h.strip().lower() for h in lines[0].split(",")]
            for line in lines[1:]:
                parts = line.split(",")
                if len(parts) < len(header):
                    continue
                row = dict(zip(header, [p.strip() for p in parts]))
                game = {
                    "game_id":    f"KAGGLE_{csv_file.stem}_{loaded}",
                    "date":       row.get("date", "")[:10],
                    "league_id":  2,
                    "league_name":"German HBL",
                    "season":     row.get("season", ""),
                    "home_team":  row.get("home_team") or row.get("home"),
                    "away_team":  row.get("away_team") or row.get("away"),
                    "home_score": int(row["home_score"]) if row.get("home_score","").isdigit() else None,
                    "away_score": int(row["away_score"]) if row.get("away_score","").isdigit() else None,
                    "status":     "FT",
                    "source":     "KAGGLE",
                }
                if game["home_team"] and game["away_team"] and game["date"]:
                    loaded += store_games([game])
        logger.info(f"  Kaggle: {loaded} handball matches loaded.")
        return loaded

    except Exception as e:
        logger.warning(f"  Kaggle fetch failed: {e}")
        return 0


# ── Main fetch orchestrator ────────────────────────────────────────────────────
def run_fetch(seasons: list = None, leagues: list = None, elo_only: bool = False) -> dict:
    """
    Main entry point: fetch all historical handball data.

    Args:
        seasons: list of season years (default: DEFAULT_SEASONS)
        leagues: list of league IDs (default: TARGET_LEAGUES.keys())
        elo_only: if True, only recompute ELO/team stats from existing DB data

    Returns summary dict.
    """
    ensure_tables()
    seasons = seasons or DEFAULT_SEASONS
    leagues = leagues or list(TARGET_LEAGUES.keys())

    summary = {
        "total_games_fetched": 0,
        "total_stored":        0,
        "leagues_processed":   [],
        "team_stats_rows":     0,
        "elo_teams":           0,
        "errors":              [],
    }

    if not elo_only:
        # ── API Sports: historical fixtures ──────────────────────────────────
        if APISPORTS_KEY:
            for lid in leagues:
                for season in seasons:
                    lg_name = TARGET_LEAGUES.get(lid, f"League {lid}")
                    logger.info(f"[HANDBALL] Fetching {lg_name} (L={lid}) Season {season}...")
                    games  = fetch_games_by_season(lid, season)
                    stored = store_games(games)
                    summary["total_games_fetched"] += len(games)
                    summary["total_stored"]        += stored
                    summary["leagues_processed"].append(f"{lg_name}/{season}: {len(games)} games, {stored} new")
        else:
            logger.warning("[HANDBALL] APISPORTS_KEY not set — skipping API Sports fetch.")
            summary["errors"].append("APISPORTS_KEY missing")

        # ── Kaggle: Bundesliga dataset ────────────────────────────────────────
        logger.info("[HANDBALL] Trying Kaggle dataset...")
        kaggle_n = try_fetch_kaggle_handball()
        summary["total_stored"] += kaggle_n

        # ── OddsPortal: historical odds (best-effort) ─────────────────────────
        logger.info("[HANDBALL] Trying OddsPortal (best-effort, no Selenium)...")
        try_fetch_oddsportal_handball("EHF Champions League", "2023")

    # ── Compute ELO + team stats from all stored matches ─────────────────────
    logger.info("[HANDBALL] Computing ELO + team efficiency stats...")
    upserted = compute_team_stats_from_matches()
    elo_map  = compute_elo_from_matches()
    summary["team_stats_rows"] = upserted
    summary["elo_teams"]       = len(elo_map)

    # ── Final status ──────────────────────────────────────────────────────────
    try:
        with get_conn() as conn:
            n = conn.execute("SELECT COUNT(*) FROM handball_matches").fetchone()[0]
    except Exception:
        n = 0

    summary["total_in_db"] = n
    summary["data_phase_cleared"] = n >= 90

    logger.info(
        f"\n{'='*60}\n"
        f"  HANDBALL DATA FETCH COMPLETE\n"
        f"  Total matches in DB : {n}\n"
        f"  Data phase cleared  : {'✓ YES' if n >= 90 else f'✗ NO ({n}/90 matches)'}\n"
        f"  Team stat rows      : {upserted}\n"
        f"  ELO teams           : {len(elo_map)}\n"
        f"{'='*60}"
    )

    if elo_map:
        # Print top 10 ELO ratings
        top10 = sorted(elo_map.items(), key=lambda x: x[1], reverse=True)[:10]
        logger.info("  TOP 10 ELO RATINGS:")
        for team, elo in top10:
            logger.info(f"    {team:35s} {elo:.0f}")

    return summary


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EyeBlackIQ handball historical data fetcher")
    parser.add_argument("--seasons", type=int, nargs="+",
                        default=DEFAULT_SEASONS, help="Season years to fetch")
    parser.add_argument("--league", type=int, nargs="*",
                        default=None, help="League IDs (default: all 3)")
    parser.add_argument("--elo-only", action="store_true",
                        help="Only recompute ELO from existing DB data")
    parser.add_argument("--status", action="store_true",
                        help="Print DB status and exit")
    args = parser.parse_args()

    if args.status:
        ensure_tables()
        with get_conn() as conn:
            n_matches = conn.execute("SELECT COUNT(*) FROM handball_matches").fetchone()[0]
            n_teams   = conn.execute("SELECT COUNT(DISTINCT team_name) FROM handball_team_stats").fetchone()[0]
        print(f"\nHandball DB Status:")
        print(f"  handball_matches  : {n_matches} rows")
        print(f"  teams tracked     : {n_teams}")
        print(f"  data_phase_cleared: {'YES' if n_matches >= 90 else f'NO ({n_matches}/90)'}")
        print(f"  APISPORTS_KEY     : {'SET' if APISPORTS_KEY else 'MISSING'}")
    else:
        result = run_fetch(
            seasons  = args.seasons,
            leagues  = args.league,
            elo_only = args.elo_only,
        )
        print(json.dumps({k: v for k, v in result.items() if k != "leagues_processed"}, indent=2))
        print("\nLeague breakdown:")
        for line in result.get("leagues_processed", []):
            print(f"  {line}")
