"""
EyeBlackIQ — fetch_odds.py
Fetches prop odds from OddsAPI (the-odds-api.com).
Supports pitcher strikeouts (MLB), player SOG (NHL), player SOT/goals (Soccer).

OddsAPI endpoints:
  GET /v4/sports/{sport_key}/odds → team ML + totals
  GET /v4/sports/{sport_key}/events/{event_id}/odds?markets={prop_markets} → player props
  GET /v4/historical/sports/{sport_key}/odds?date={iso_timestamp} → historical snapshot

Run daily before model signals:
  python scrapers/fetch_odds.py [--date YYYY-MM-DD] [--sport all|mlb|nhl|soccer]
"""
import os
import json
import sqlite3
import logging
import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

# Load from the main project .env
_ENV_PATH = Path(__file__).parent.parent.parent / "quant-betting" / "soccer" / ".claude" / "worktrees" / "admiring-allen" / ".env"
if _ENV_PATH.exists():
    load_dotenv(_ENV_PATH)
else:
    load_dotenv()  # fallback to local .env

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).parent.parent
DB_PATH    = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"
CACHE_DIR  = BASE_DIR / "scrapers" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
ODDS_API_URL = "https://api.the-odds-api.com/v4"

# Sport keys for OddsAPI
SPORT_KEYS = {
    "mlb":    "baseball_mlb",
    "nhl":    "icehockey_nhl",
    "soccer": "soccer_epl",      # Can expand to other leagues
    "ncaa":   "baseball_ncaa",
}

# Prop markets by sport
PROP_MARKETS = {
    "mlb":    "pitcher_strikeouts,batter_hits,batter_total_bases",
    "nhl":    "player_shots_on_goal,player_points,player_assists",
    "soccer": "player_shots_on_target,player_goals,player_assists",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_events(sport_key: str) -> list:
    """Fetch all events (game list) for a sport."""
    if not ODDS_API_KEY:
        logger.warning("ODDS_API_KEY not set — skipping OddsAPI fetch")
        return []

    url = f"{ODDS_API_URL}/sports/{sport_key}/events"
    params = {"apiKey": ODDS_API_KEY, "dateFormat": "iso"}
    resp = requests.get(url, params=params, timeout=15)

    remaining = resp.headers.get("x-requests-remaining", "?")
    logger.info(f"OddsAPI events [{sport_key}]: {resp.status_code}  remaining={remaining}")

    if resp.status_code == 401:
        logger.error("OddsAPI: 401 Unauthorized — check ODDS_API_KEY")
        return []
    resp.raise_for_status()
    return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_event_props(sport_key: str, event_id: str, markets: str) -> dict:
    """Fetch prop odds for a specific event."""
    if not ODDS_API_KEY:
        return {}

    url = f"{ODDS_API_URL}/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "apiKey":   ODDS_API_KEY,
        "markets":  markets,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    resp = requests.get(url, params=params, timeout=15)
    remaining = resp.headers.get("x-requests-remaining", "?")
    logger.debug(f"OddsAPI props [{event_id[:8]}]: {resp.status_code}  remaining={remaining}")

    if resp.status_code == 404:
        logger.debug(f"No props for event {event_id}")
        return {}
    resp.raise_for_status()
    return resp.json()


def parse_prop_rows(event_data: dict, sport: str) -> List[dict]:
    """
    Parse OddsAPI event props response into flat rows.
    Returns list of dicts ready for DB insertion.
    """
    rows = []
    event_id   = event_data.get("id", "")
    home_team  = event_data.get("home_team", "")
    away_team  = event_data.get("away_team", "")
    event_date = event_data.get("commence_time", "")[:10]

    for bm in event_data.get("bookmakers", []):
        book = bm.get("key", "")
        for market in bm.get("markets", []):
            market_key = market.get("key", "")
            for outcome in market.get("outcomes", []):
                player   = outcome.get("description", outcome.get("name", ""))
                side     = outcome.get("name", "")    # "Over" or "Under"
                line     = outcome.get("point")
                price    = outcome.get("price")       # American odds

                if price is None or line is None:
                    continue

                rows.append({
                    "event_id":   event_id,
                    "event_date": event_date,
                    "sport":      sport,
                    "home_team":  home_team,
                    "away_team":  away_team,
                    "market":     market_key,
                    "player":     player,
                    "side":       side,
                    "line":       float(line),
                    "price":      int(price),
                    "book":       book,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })
    return rows


def cache_response(sport: str, event_id: str, data: dict):
    fname = CACHE_DIR / f"odds_{sport}_{event_id[:8]}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f)


def log_etl(conn, source: str, table: str, rows: int, as_of_ts: str):
    conn.execute(
        "INSERT INTO etl_log (source, table_name, rows_loaded, as_of_ts, run_ts) VALUES (?,?,?,?,?)",
        (source, table, rows, as_of_ts, datetime.now(timezone.utc).isoformat())
    )


def fetch_and_store_props(sport: str, date_str: str) -> int:
    """
    Fetch all prop odds for a sport on a date.
    Writes raw data to cache and parsed rows to a JSON file for model consumption.
    Returns total prop rows fetched.
    """
    sport_key  = SPORT_KEYS.get(sport)
    if not sport_key:
        logger.warning(f"No sport_key for {sport}")
        return 0

    markets = PROP_MARKETS.get(sport, "")
    if not markets:
        return 0

    # Get event list
    try:
        events = fetch_events(sport_key)
    except Exception as e:
        logger.error(f"fetch_events failed [{sport}]: {e}")
        return 0

    # Filter to today's events
    day_events = [e for e in events if e.get("commence_time", "").startswith(date_str)]
    logger.info(f"{sport}: {len(day_events)} events on {date_str} (of {len(events)} total)")

    all_rows = []
    for ev in day_events:
        event_id = ev["id"]
        try:
            props_data = fetch_event_props(sport_key, event_id, markets)
            if props_data:
                cache_response(sport, event_id, props_data)
                rows = parse_prop_rows(props_data, sport)
                all_rows.extend(rows)
                logger.debug(f"  {ev.get('home_team','?')} vs {ev.get('away_team','?')}: {len(rows)} prop rows")
            time.sleep(0.3)  # Rate limit courtesy
        except Exception as e:
            logger.warning(f"fetch_event_props failed [{event_id[:8]}]: {e}")

    # Write to cache as JSON (models read from this)
    out_file = CACHE_DIR / f"props_{sport}_{date_str}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, indent=2)
    logger.info(f"Wrote {len(all_rows)} prop rows → {out_file.name}")

    # Log to DB etl_log
    if all_rows:
        with sqlite3.connect(DB_PATH) as conn:
            log_etl(conn, "oddsapi", f"props_{sport}", len(all_rows), date_str)
            conn.commit()

    return len(all_rows)


def fetch_team_odds(sport: str, date_str: str) -> list:
    """
    Fetch team ML + totals (no props) for a sport.
    Returns list of parsed game odds.
    """
    sport_key = SPORT_KEYS.get(sport)
    if not sport_key or not ODDS_API_KEY:
        return []

    url = f"{ODDS_API_URL}/sports/{sport_key}/odds"
    params = {
        "apiKey":      ODDS_API_KEY,
        "regions":     "us",
        "markets":     "h2h,totals",
        "oddsFormat":  "american",
        "dateFormat":  "iso",
        "commenceTimeFrom": f"{date_str}T00:00:00Z",
        "commenceTimeTo":   f"{date_str}T23:59:59Z",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"fetch_team_odds [{sport}]: {e}")
        return []

    # Cache
    out_file = CACHE_DIR / f"team_odds_{sport}_{date_str}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Team odds [{sport}]: {len(data)} games → {out_file.name}")
    return data


def run_fetch(sports: list, date_str: str) -> dict:
    """Fetch props + team odds for all sports."""
    results = {}
    for sport in sports:
        n_props = fetch_and_store_props(sport, date_str)
        team    = fetch_team_odds(sport, date_str)
        results[sport] = {"prop_rows": n_props, "team_games": len(team)}
        time.sleep(0.5)

    logger.info(f"fetch_odds complete: {results}")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch prop odds from OddsAPI")
    parser.add_argument("--date",  default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    parser.add_argument("--sport", default="all", help="all|mlb|nhl|soccer|ncaa")
    args = parser.parse_args()

    sports = list(SPORT_KEYS.keys()) if args.sport == "all" else [args.sport]
    results = run_fetch(sports, args.date)

    for sport, counts in results.items():
        print(f"  {sport}: {counts['prop_rows']} prop rows, {counts['team_games']} team games")
