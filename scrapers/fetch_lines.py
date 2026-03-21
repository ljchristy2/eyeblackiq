"""
EyeBlackIQ — fetch_lines.py
Fetches today's game lines from TheRundown API.
Writes raw snapshots to etl_log and parsed odds to lines cache.

TheRundown API (free, 20K pts/day):
  GET https://therundown.io/api/v2/sports/{sport_id}/events/{date}?market_ids=1,2,3
  sport_ids: NFL=1, MLB=3, NHL=4, Soccer=varies by league

Run daily before model signals:
  python scrapers/fetch_lines.py [--date YYYY-MM-DD] [--sport all|mlb|nhl|soccer|ncaa]
"""
import os
import json
import sqlite3
import logging
import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).parent.parent
DB_PATH    = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"
CACHE_DIR  = BASE_DIR / "scrapers" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

RUNDOWN_KEY = os.getenv("THERUNDOWN_API_KEY", "")
RUNDOWN_URL = "https://therundown.io/api/v2"

# TheRundown sport IDs
SPORT_IDS = {
    "nfl":    1,
    "mlb":    3,
    "nhl":    4,
    "ncaa":   5,    # NCAA Baseball (verify — may vary)
}

# Market IDs: 1=full game ML, 2=spread, 3=total, 8=1H/F5 ML
MARKET_IDS = "1,2,3,8"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_sport_events(sport_id: int, date_str: str) -> dict:
    """
    Fetch all events for a sport on a date from TheRundown.
    Returns raw API response dict.
    """
    url = f"{RUNDOWN_URL}/sports/{sport_id}/events/{date_str}"
    params = {"market_ids": MARKET_IDS}
    headers = {}
    if RUNDOWN_KEY:
        headers["x-rapidapi-key"] = RUNDOWN_KEY

    logger.info(f"Fetching TheRundown sport={sport_id} date={date_str}")
    resp = requests.get(url, params=params, headers=headers, timeout=15)

    if resp.status_code == 401:
        logger.warning("TheRundown: 401 Unauthorized — check THERUNDOWN_API_KEY")
        return {}
    if resp.status_code == 429:
        logger.warning("TheRundown: 429 Rate limited — backing off")
        time.sleep(30)
        resp = requests.get(url, params=params, headers=headers, timeout=15)

    resp.raise_for_status()
    return resp.json()


def parse_event(event: dict) -> Optional[dict]:
    """
    Parse a single TheRundown event into our standard format.
    Returns None if event is missing required fields.
    """
    try:
        event_id   = event.get("event_id", "")
        event_date = event.get("event_date", "")
        home_team  = event.get("teams_normalized", [{}])[0].get("name", "")
        away_team  = event.get("teams_normalized", [{}])[1].get("name", "") if len(event.get("teams_normalized", [])) > 1 else ""

        # Lines — find Pinnacle (affiliate_id varies; look by name) and best available
        lines_raw = event.get("lines", {})
        pinnacle_ml_home = None
        pinnacle_ml_away = None
        best_ml_home     = None
        best_ml_away     = None
        total_line       = None
        f5_ml_home       = None
        f5_ml_away       = None

        for book_id, book_data in lines_raw.items():
            book_name = book_data.get("affiliate", {}).get("name", "").lower()
            moneyline = book_data.get("moneyline", {})
            totals    = book_data.get("total", {})
            f5        = book_data.get("moneyline_1h", {})  # first half / F5

            ml_home = moneyline.get("moneyline_home")
            ml_away = moneyline.get("moneyline_away")
            total   = totals.get("total_over")

            if "pinnacle" in book_name and ml_home and ml_away:
                pinnacle_ml_home = ml_home
                pinnacle_ml_away = ml_away

            if ml_home and ml_away:
                best_ml_home = ml_home
                best_ml_away = ml_away

            if total and total_line is None:
                total_line = total

            if f5.get("moneyline_home") and f5_ml_home is None:
                f5_ml_home = f5["moneyline_home"]
                f5_ml_away = f5.get("moneyline_away")

        return {
            "event_id":       event_id,
            "event_date":     event_date,
            "home_team":      home_team,
            "away_team":      away_team,
            "ml_home":        pinnacle_ml_home or best_ml_home,
            "ml_away":        pinnacle_ml_away or best_ml_away,
            "ml_home_pinn":   pinnacle_ml_home,
            "ml_away_pinn":   pinnacle_ml_away,
            "total":          total_line,
            "f5_ml_home":     f5_ml_home,
            "f5_ml_away":     f5_ml_away,
            "fetched_at":     datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.warning(f"parse_event failed: {e} — {event.get('event_id','?')}")
        return None


def cache_response(sport: str, date_str: str, data: dict):
    """Write raw API response to cache file."""
    fname = CACHE_DIR / f"{sport}_{date_str}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.debug(f"Cached to {fname}")


def log_etl(conn, source: str, table: str, rows: int, as_of_ts: str):
    """Append ETL run to etl_log table."""
    conn.execute(
        "INSERT INTO etl_log (source, table_name, rows_loaded, as_of_ts, run_ts) VALUES (?,?,?,?,?)",
        (source, table, rows, as_of_ts, datetime.now(timezone.utc).isoformat())
    )


def fetch_and_store(sport: str, date_str: str) -> list:
    """
    Fetch lines for a sport+date, parse, cache, and return parsed events.
    """
    sport_id = SPORT_IDS.get(sport)
    if not sport_id:
        logger.warning(f"No sport_id for {sport}")
        return []

    try:
        raw = fetch_sport_events(sport_id, date_str)
    except Exception as e:
        logger.error(f"Fetch failed for {sport}: {e}")
        return []

    if not raw:
        return []

    # Cache raw response
    cache_response(sport, date_str, raw)

    events_raw = raw.get("events", [])
    parsed = []
    for ev in events_raw:
        p = parse_event(ev)
        if p:
            parsed.append(p)

    logger.info(f"{sport} {date_str}: {len(parsed)} events parsed")

    # Write to DB
    if parsed:
        with sqlite3.connect(DB_PATH) as conn:
            log_etl(conn, "therundown", f"lines_{sport}", len(parsed), date_str)
            conn.commit()

    return parsed


def run_fetch(sports: list, date_str: str) -> dict:
    """Fetch lines for multiple sports. Returns {sport: [events]}."""
    results = {}
    for sport in sports:
        results[sport] = fetch_and_store(sport, date_str)
        time.sleep(0.5)  # Rate limit courtesy

    total = sum(len(v) for v in results.values())
    logger.info(f"fetch_lines complete: {total} events across {len(sports)} sports")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch game lines from TheRundown")
    parser.add_argument("--date",  default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    parser.add_argument("--sport", default="all", help="all|mlb|nhl|soccer|ncaa")
    args = parser.parse_args()

    sports = list(SPORT_IDS.keys()) if args.sport == "all" else [args.sport]
    results = run_fetch(sports, args.date)

    for sport, events in results.items():
        print(f"  {sport}: {len(events)} events")
        for ev in events[:3]:
            print(f"    {ev['away_team']} @ {ev['home_team']}  ML: {ev['ml_away']}/{ev['ml_home']}  O/U: {ev['total']}")
