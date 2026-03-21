"""
EyeBlackIQ — scraper_cricket.py
Cricket data via API Sports (v1.cricket.api-sports.io).

Usage:
  python scrapers/scraper_cricket.py --date 2026-03-21 --mode schedule
  python scrapers/scraper_cricket.py --date 2026-03-21 --mode results
  python scrapers/scraper_cricket.py --mode leagues

Requires: APISPORTS_KEY in .env
Output: JSON to stdout
"""
import sys
import json
import time
import os
import argparse
import logging
from datetime import datetime
from pathlib import Path

try:
    import urllib.request
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

LOG_PATH = Path(__file__).parent.parent / "logs" / "scraper_errors.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger("scraper_cricket")

BASE_URL = "https://v1.cricket.api-sports.io"

# Tracked leagues: IPL=1, ICC T20 World Cup=9, ICC ODI World Cup=10, The Hundred=2
DEFAULT_LEAGUES = [1, 9, 10, 2]


def _get_key() -> str:
    key = os.getenv("APISPORTS_KEY", "")
    if not key:
        logger.error("APISPORTS_KEY not set in .env — cricket scraper requires API Sports key")
    return key


def _request(endpoint: str, params: dict = None) -> dict:
    """Make authenticated GET request to API Sports cricket API."""
    key = _get_key()
    if not key:
        return {"errors": {"key": "APISPORTS_KEY not set"}, "response": []}

    url = BASE_URL + endpoint
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"

    req = urllib.request.Request(url)
    req.add_header("X-Auth-Token", key)
    req.add_header("x-rapidapi-key", key)
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        time.sleep(0.5)
        return data
    except Exception as e:
        logger.error(f"API Sports cricket request failed: {url} — {e}")
        return {"errors": {str(type(e).__name__): str(e)}, "response": []}


def get_leagues() -> list:
    """Return all available cricket leagues from API Sports."""
    data = _request("/leagues")
    if data.get("errors"):
        logger.warning(f"Leagues error: {data['errors']}")
        return []
    return [
        {
            "id":      item.get("id") or (item.get("league") or {}).get("id"),
            "name":    item.get("name") or (item.get("league") or {}).get("name"),
            "country": (item.get("country") or {}).get("name"),
            "seasons": [s.get("year") or s.get("season") for s in (item.get("seasons") or [])],
        }
        for item in (data.get("response") or [])
    ]


def get_schedule(date_str: str, league_ids: list = None) -> list:
    """Fetch fixtures for a given date across target leagues."""
    leagues = league_ids or DEFAULT_LEAGUES
    fixtures = []
    for lid in leagues:
        data = _request("/fixtures", {"league": lid, "date": date_str})
        if data.get("errors"):
            logger.warning(f"Fixtures error league={lid}: {data['errors']}")
            continue
        for item in (data.get("response") or []):
            f = item.get("fixture") or item
            teams = item.get("teams") or {}
            scores = item.get("scores") or item.get("score") or {}
            fixtures.append({
                "fixture_id":  f.get("id"),
                "date":        f.get("date") or date_str,
                "status":      (f.get("status") or {}).get("short") or f.get("status"),
                "league_id":   lid,
                "home_team":   (teams.get("home") or {}).get("name"),
                "away_team":   (teams.get("away") or {}).get("name"),
                "home_score":  (scores.get("home") or {}).get("total") or scores.get("home"),
                "away_score":  (scores.get("away") or {}).get("total") or scores.get("away"),
            })
        time.sleep(0.5)
    return fixtures


def get_results(date_str: str, league_ids: list = None) -> list:
    """Fetch completed match results for a given date."""
    all_fix = get_schedule(date_str, league_ids)
    return [f for f in all_fix if (f.get("status") or "").upper() in ("FT", "FINISHED", "COMPLETE", "NS")]


def get_odds(fixture_id: int) -> list:
    """Fetch pre-game odds for a fixture (if available on plan)."""
    data = _request("/odds", {"fixture": fixture_id})
    if data.get("errors"):
        logger.warning(f"Odds error fixture={fixture_id}: {data['errors']}")
        return []
    books = []
    for item in (data.get("response") or []):
        for bk in (item.get("bookmakers") or []):
            for bet in (bk.get("bets") or []):
                books.append({
                    "fixture_id":    fixture_id,
                    "bookmaker":     bk.get("name"),
                    "market":        bet.get("name"),
                    "values":        bet.get("values") or [],
                })
    return books


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EyeBlackIQ cricket scraper — API Sports")
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"), help="Date YYYY-MM-DD")
    parser.add_argument("--mode",   default="schedule", choices=["schedule", "results", "props", "leagues"],
                        help="schedule | results | props | leagues")
    parser.add_argument("--league", type=int, nargs="*", help="League ID(s) to filter (default: all tracked)")
    args = parser.parse_args()

    if args.mode == "leagues":
        out = get_leagues()
    elif args.mode == "schedule":
        out = get_schedule(args.date, args.league)
    elif args.mode == "results":
        out = get_results(args.date, args.league)
    elif args.mode == "props":
        # Props requires fixture IDs — schedule first, then fetch odds
        fixtures = get_schedule(args.date, args.league)
        out = []
        for fx in fixtures:
            fid = fx.get("fixture_id")
            if fid:
                odds = get_odds(fid)
                out.extend(odds)
    else:
        out = []

    sys.stdout.reconfigure(encoding="utf-8")
    print(json.dumps(out, indent=2, ensure_ascii=False))
