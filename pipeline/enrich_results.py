"""
EyeBlackIQ — pipeline/enrich_results.py
Fetches ESPN box scores for results where actual_val is NULL.
Writes final score back to results.actual_val for loss analysis context.

Sports: NCAA_BASEBALL, NHL, MLB
ESPN: free public API, no key needed

Usage:
  python pipeline/enrich_results.py                  # last 7 days
  python pipeline/enrich_results.py --date 2026-03-21
  python pipeline/enrich_results.py --days 14
  python pipeline/enrich_results.py --all
"""
import sqlite3
import json
import time
import logging
import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"

ESPN_ENDPOINTS = {
    "NCAA_BASEBALL": "https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/scoreboard",
    "NHL":           "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "MLB":           "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "SOCCER":        "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json"}


def fetch_espn_games(sport: str, date_str: str) -> list:
    """Fetch all completed games from ESPN for a given sport + date."""
    url = ESPN_ENDPOINTS.get(sport.upper())
    if not url:
        return []
    try:
        r = requests.get(url, params={"dates": date_str.replace("-", "")},
                         headers=HEADERS, timeout=12)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.warning(f"[ESPN] {sport} {date_str}: {e}")
        return []

    games = []
    for ev in data.get("events", []):
        try:
            comp   = ev.get("competitions", [{}])[0]
            comps  = comp.get("competitors", [])
            status = ev.get("status", {}).get("type", {}).get("name", "")
            home   = next((c for c in comps if c.get("homeAway") == "home"), {})
            away   = next((c for c in comps if c.get("homeAway") == "away"), {})
            if not home or not away:
                continue
            ht = home.get("team", {})
            at = away.get("team", {})
            games.append({
                "id":         ev.get("id", ""),
                "home_abbr":  (ht.get("abbreviation") or "").upper(),
                "away_abbr":  (at.get("abbreviation") or "").upper(),
                "home_full":  (ht.get("displayName") or ht.get("name") or "").lower(),
                "away_full":  (at.get("displayName") or at.get("name") or "").lower(),
                "home_score": int(home.get("score") or 0),
                "away_score": int(away.get("score") or 0),
                "status":     status,
            })
        except Exception:
            continue
    logger.info(f"[ESPN] {sport} {date_str}: {len(games)} games fetched")
    return games


def _team_match(rg_part: str, abbr: str, full: str) -> bool:
    """Fuzzy team matching: check if rg_part matches team abbreviation or full name."""
    rg = rg_part.lower().strip()
    abbr_l = abbr.lower()
    if not rg:
        return False
    # Exact abbreviation match
    if rg == abbr_l:
        return True
    # Full name starts with or contains our string
    if full.startswith(rg) or rg in full:
        return True
    # Our string starts with or contains abbr
    if abbr_l in rg or rg in abbr_l:
        return True
    # Significant word overlap (words > 3 chars)
    rg_words = {w for w in rg.split() if len(w) > 3}
    full_words = set(full.split())
    if rg_words and rg_words & full_words:
        return True
    return False


def match_game(result_game: str, espn_games: list) -> dict | None:
    """Try to match a result game name like 'Away @ Home' to an ESPN game."""
    if not result_game or not espn_games:
        return None
    rg = result_game.strip()
    sep = " @ " if " @ " in rg else " vs " if " vs " in rg else None
    if not sep:
        return None
    rg_away, rg_home = rg.split(sep, 1)
    for eg in espn_games:
        hm = _team_match(rg_home, eg["home_abbr"], eg["home_full"])
        am = _team_match(rg_away, eg["away_abbr"], eg["away_full"])
        if hm and am:
            return eg
    return None


def build_score_string(eg: dict) -> str:
    """Build 'AWAY X–Y HOME (Final)' string from ESPN game dict."""
    return (f"{eg['away_abbr']} {eg['away_score']}\u2013{eg['home_score']} {eg['home_abbr']}"
            f" (Final)")


def enrich(date_str: str = None, days: int = 7, enrich_all: bool = False) -> int:
    """Fetch and write box scores for ungraded results. Returns count updated."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if enrich_all:
            q = """SELECT r.id, r.signal_date, r.sport, r.game, r.actual_val, r.result, r.side
                   FROM results r WHERE r.result NOT IN ('PENDING','VOID') AND r.actual_val IS NULL"""
            rows = [dict(r) for r in conn.execute(q)]
        elif date_str:
            q = """SELECT r.id, r.signal_date, r.sport, r.game, r.actual_val, r.result, r.side
                   FROM results r WHERE r.signal_date=? AND r.result NOT IN ('PENDING','VOID') AND r.actual_val IS NULL"""
            rows = [dict(r) for r in conn.execute(q, (date_str,))]
        else:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            q = """SELECT r.id, r.signal_date, r.sport, r.game, r.actual_val, r.result, r.side
                   FROM results r WHERE r.signal_date>=? AND r.result NOT IN ('PENDING','VOID') AND r.actual_val IS NULL"""
            rows = [dict(r) for r in conn.execute(q, (cutoff,))]

    logger.info(f"[Enrich] {len(rows)} results missing box score data")
    if not rows:
        return 0

    # Group by (date, sport) to minimize ESPN calls
    by_ds = {}
    for row in rows:
        key = (row["signal_date"], row["sport"])
        by_ds.setdefault(key, []).append(row)

    updates = []
    for (date, sport), result_rows in by_ds.items():
        if sport.upper() not in ESPN_ENDPOINTS:
            continue
        espn = fetch_espn_games(sport, date)
        time.sleep(0.3)

        for row in result_rows:
            eg = match_game(row["game"], espn)
            if not eg:
                logger.debug(f"[Enrich] No match: {row['game']} ({sport} {date})")
                continue
            score = build_score_string(eg)
            # If it's a total bet, append total runs/goals
            side = (row.get("side") or "").upper()
            if any(kw in side for kw in ("OVER", "UNDER", "O ", "U ", "O5", "U5", "TOTAL")):
                total = eg["home_score"] + eg["away_score"]
                score += f"  \u00b7  Total: {total}"
            logger.info(f"[Enrich] {row['game']} ({date}) \u2192 {score}")
            updates.append((score, row["id"]))

    if updates:
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany("UPDATE results SET actual_val=? WHERE id=?", updates)
            conn.commit()
        logger.info(f"[Enrich] Updated {len(updates)} results")
    return len(updates)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Enrich EyeBlackIQ results with ESPN box scores")
    ap.add_argument("--date",  help="Specific date YYYY-MM-DD")
    ap.add_argument("--days",  type=int, default=7, help="Look back N days (default 7)")
    ap.add_argument("--all",   action="store_true", help="Enrich all results missing actual_val")
    args = ap.parse_args()
    enrich(date_str=args.date, days=args.days, enrich_all=args.all)
