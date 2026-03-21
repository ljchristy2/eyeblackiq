"""
EyeBlackIQ — scraper_handball.py
Handball data via API Sports (v1.handball.api-sports.io).

Usage:
  python scrapers/scraper_handball.py --date 2026-03-21 --mode schedule
  python scrapers/scraper_handball.py --date 2026-03-21 --mode results
  python scrapers/scraper_handball.py --mode leagues

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
logger = logging.getLogger("scraper_handball")

BASE_URL = "https://v1.handball.api-sports.io"

# Tracked leagues — discover more via --mode leagues
# EHF Champions League, German Handball Bundesliga, French Starligue
DEFAULT_LEAGUES = [1, 2, 3]


def _get_key() -> str:
    key = os.getenv("APISPORTS_KEY", "")
    if not key:
        logger.error("APISPORTS_KEY not set in .env — handball scraper requires API Sports key")
    return key


def _request(endpoint: str, params: dict = None) -> dict:
    """Make authenticated GET request to API Sports handball API."""
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
        logger.error(f"API Sports handball request failed: {url} — {e}")
        return {"errors": {str(type(e).__name__): str(e)}, "response": []}


def get_leagues() -> list:
    """Return all available handball leagues from API Sports."""
    data = _request("/leagues")
    if data.get("errors"):
        logger.warning(f"Leagues error: {data['errors']}")
        return []
    return [
        {
            "id":      item.get("id") or (item.get("league") or {}).get("id"),
            "name":    item.get("name") or (item.get("league") or {}).get("name"),
            "country": (item.get("country") or {}).get("name"),
            "seasons": [s.get("season") or s.get("year") for s in (item.get("seasons") or [])],
        }
        for item in (data.get("response") or [])
    ]


def get_schedule(date_str: str, league_ids: list = None) -> list:
    """Fetch games for a given date across target leagues."""
    leagues = league_ids or DEFAULT_LEAGUES
    games = []
    for lid in leagues:
        data = _request("/games", {"league": lid, "date": date_str})
        if data.get("errors"):
            logger.warning(f"Games error league={lid}: {data['errors']}")
            continue
        for item in (data.get("response") or []):
            teams = item.get("teams") or {}
            scores = item.get("scores") or {}
            status = item.get("status") or {}
            games.append({
                "game_id":    item.get("id"),
                "date":       item.get("date") or date_str,
                "status":     status.get("short") if isinstance(status, dict) else status,
                "league_id":  lid,
                "home_team":  (teams.get("home") or {}).get("name"),
                "away_team":  (teams.get("away") or {}).get("name"),
                "home_score": (scores.get("home") or {}).get("total") if isinstance(scores.get("home"), dict) else scores.get("home"),
                "away_score": (scores.get("away") or {}).get("total") if isinstance(scores.get("away"), dict) else scores.get("away"),
            })
        time.sleep(0.5)
    return games


def get_results(date_str: str, league_ids: list = None) -> list:
    """Fetch completed game results for a given date."""
    all_games = get_schedule(date_str, league_ids)
    return [g for g in all_games if (g.get("status") or "").upper() in ("FT", "FINISHED", "NS", "COMPLETE")]


def get_odds(game_id: int) -> list:
    """Fetch pre-game odds for a game (if available on plan)."""
    data = _request("/odds", {"game": game_id})
    if data.get("errors"):
        logger.warning(f"Odds error game={game_id}: {data['errors']}")
        return []
    books = []
    for item in (data.get("response") or []):
        for bk in (item.get("bookmakers") or []):
            for bet in (bk.get("bets") or []):
                books.append({
                    "game_id":    game_id,
                    "bookmaker":  bk.get("name"),
                    "market":     bet.get("name"),
                    "values":     bet.get("values") or [],
                })
    return books


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EyeBlackIQ handball scraper — API Sports")
    parser.add_argument("--date",   default=datetime.now().strftime("%Y-%m-%d"), help="Date YYYY-MM-DD")
    parser.add_argument("--mode",  default="schedule", choices=["schedule", "results", "props", "leagues"],
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
        games = get_schedule(args.date, args.league)
        out = []
        for g in games:
            gid = g.get("game_id")
            if gid:
                odds = get_odds(gid)
                out.extend(odds)
    else:
        out = []

    sys.stdout.reconfigure(encoding="utf-8")
    print(json.dumps(out, indent=2, ensure_ascii=False))
