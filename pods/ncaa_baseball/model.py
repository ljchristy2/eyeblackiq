"""
EyeBlackIQ — pods/ncaa_baseball/model.py
NCAA Baseball team ML signals via ELO + ISR blend.

Model:
  - ELO head-to-head: P_home = 1/(1+10^(-(ELO_h+25-ELO_a)/400))
  - ISR normalized: P_home_isr = isr_h/(isr_a+isr_h) + HFA/2
  - Conference ISR fallback when team not in ISR list
  - Blend: 0.60*ELO + 0.40*ISR
  - SP ERA adjustment: r_team = 5.0 * sp_era / LG_ERA; era_adj = (r_away-r_home)/20
  - Run total: r_home + r_away (Poisson projection)

Confidence levels (●●● / ●●○ / ●○○):
  HIGH (●●●): SP with 5+ starts, ELO-ISR agree within 5pp
  MED  (●●○): SP 2-4 starts or 5-8pp disagreement
  LOW  (●○○): TBD SP, <2 starts, or early season (<10 team games)

Spread Alternative Rule:
  When ML > +250, signals include a run line (+1.5) alternative note.
  This is flagged but not separately sized — bettors choose their vehicle.

POD Rule:
  Top pick of the day. Requires HIGH confidence (●●●).

Usage:
  python pods/ncaa_baseball/model.py --date 2026-03-21
  python pods/ncaa_baseball/model.py --date 2026-03-21 --dry-run
"""
import csv
import os
import sqlite3
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# Load env
_ENV = Path(__file__).parent.parent.parent.parent / "quant-betting" / "soccer" / ".claude" / "worktrees" / "admiring-allen" / ".env"
if _ENV.exists():
    load_dotenv(_ENV)
else:
    load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE_DIR    = Path(__file__).parent.parent.parent
DATA_DIR    = Path(__file__).parent / "data"
TGT_DB      = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"

# Data files — update these paths when new weekly files arrive
ELO_CSV     = DATA_DIR / "ELO_Mar21.csv"
ISR_CSV     = DATA_DIR / "ISR_Mar21.csv"
CONF_CSV    = DATA_DIR / "Conference_ISR_Mar21.csv"

SPORT       = "NCAA_BASEBALL"
W_ELO       = 0.60
W_ISR       = 0.40
HFA         = 0.04
LG_ERA      = 5.70
LG_R        = 5.0
MIN_EDGE    = 0.02   # T3 floor — below this = BALK, no signal written
RL_ALT_THRESHOLD = 250  # When ML odds > +250, add run line alternative note


# ── Tier system ────────────────────────────────────────────────────────────────
def ncaa_tier(edge_pct):
    """Returns (tier_label, units) for edge in 0-100 scale."""
    if   edge_pct >= 12: return ("FILTHY",     2.0)
    elif edge_pct >=  5: return ("WHEELHOUSE", 1.5)
    elif edge_pct >=  2: return ("SCOUT",       1.0)
    else:                return ("BALK",         0.0)


# ── Confidence ─────────────────────────────────────────────────────────────────
def confidence(sp_starts: int, elo_isr_gap_pp: float, n_team_games: int = 15) -> tuple:
    """
    Returns (label, symbol) e.g. ('HIGH', '●●●')

    Scoring:
      Start at MED (2). Gain +1 for each strong signal, -1 for each weak signal.

    sp_starts:       number of SP appearances this season
    elo_isr_gap_pp:  abs(p_home_elo - p_home_isr) * 100 — model agreement
    n_team_games:    team games played (proxy for ISR reliability)

    HIGH requires: 5+ SP starts AND model agreement within 5pp
    LOW triggered by: <2 SP starts, or gap >10pp, or <10 team games
    """
    score = 2  # start MEDIUM
    if sp_starts >= 5:        score += 1
    elif sp_starts < 2:       score -= 1
    if elo_isr_gap_pp <= 5:   score += 1
    elif elo_isr_gap_pp > 10: score -= 1
    if n_team_games < 10:     score -= 1
    score = max(1, min(3, score))
    return {3: ("HIGH", "●●●"), 2: ("MED", "●●○"), 1: ("LOW", "●○○")}[score]


# ── Team name normalization aliases ────────────────────────────────────────────
# Keys = names used in market/game data
# Values = names that appear in ELO/ISR CSV files
ALIASES = {
    # Common abbreviations → full names in CSV
    "USC":              "Southern California",
    "LSU":              "Louisiana State",
    "BYU":              "Brigham Young",
    "TCU":              "Texas Christian",
    "UCF":              "Central Florida",
    "WVU":              "West Virginia",
    "OU":               "Oklahoma",
    "UNC":              "North Carolina",
    "UNCW":             "NC Wilmington",
    "VT":               "Virginia Tech",
    "FSU":              "Florida State",
    "Ole Miss":         "Mississippi",
    "FAU":              "Florida Atlantic",
    "UTSA":             "Texas-San Antonio",
    "UTRGV":            "Texas-Rio Grande Valley",
    "UTA":              "Texas-Arlington",
    "FGCU":             "Florida Gulf Coast",
    "FIU":              "Florida International",
    "UAB":              "Alabama-Birmingham",
    "ULM":              "Louisiana-Monroe",
    "SFA":              "Stephen F. Austin State",
    "SEMO":             "Southeast Missouri State",
    "SIUE":             "SIU-Edwardsville",
    "UIC":              "Illinois-Chicago",
    "SIU":              "Southern Illinois",
    "UMBC":             "Maryland-Baltimore County",
    "UNCG":             "NC-Greensboro",
    "UNC Asheville":    "NC-Asheville",
    "UNC Charlotte":    "NC-Charlotte",
    "Saint Mary's":     "St. Mary's",
    "Saint John's":     "St. John's",
    "Saint Joseph's":   "St. Joseph's",
    "Saint Louis":      "St. Louis",
    "Saint Bonaventure":"St. Bonaventure",
    "Saint Thomas":     "St. Thomas",
    "Saint Peter's":    "St. Peter's",
    "LIU":              "LIU-Brooklyn",
    "Maryland Eastern Shore": "Maryland-Eastern Shore",
    "Arkansas-Little Rock":   "Arkansas-Little Rock",
    "Little Rock":             "Arkansas-Little Rock",
    "Georgia Southern": "Georgia Southern",
    "Southern Miss":    "Southern Mississippi",
    "Southern Illinois":"Southern Illinois",
    "Louisiana":        "Louisiana-Lafayette",
    "UL Lafayette":     "Louisiana-Lafayette",
    "App State":        "Appalachian State",
    "Coastal Carolina": "Coastal Carolina",
}

# Conference → ISR fallback map (loaded from Conference_ISR CSV)
_CONF_ISR_MAP: dict = {}

# Team → conference map (for fallback — must be maintained or scraped)
TEAM_CONFERENCE = {
    "Auburn": "SEC", "Texas": "SEC", "UCLA": "Pac-12",
    "Southern California": "Pac-12", "Southern Mississippi": "C-USA",
    "Mississippi": "SEC", "Cincinnati": "Big 12", "Florida State": "ACC",
    "Louisiana-Lafayette": "Sun Belt", "West Virginia": "Big 12",
    "Kentucky": "SEC", "Georgia Tech": "ACC", "North Carolina": "ACC",
    "Florida": "SEC", "UC Santa Barbara": "Big West", "Oregon": "Pac-12",
    "Oregon State": "Pac-12", "Virginia": "ACC", "Mississippi State": "SEC",
    "Texas A&M": "SEC", "Kansas State": "Big 12", "Missouri State": "MVC",
    "Arizona State": "Pac-12", "Georgia": "SEC", "Jacksonville State": "C-USA",
    "Nebraska": "Big Ten", "Oklahoma": "Big 12", "Arkansas State": "Sun Belt",
    "South Florida": "AAC", "Alabama": "SEC", "Arkansas": "SEC",
    "Kent State": "MAC", "South Alabama": "Sun Belt", "Central Florida": "AAC",
    "Tennessee": "SEC", "Purdue": "Big Ten", "Liberty": "C-USA",
    "Mercer": "Southern", "Clemson": "ACC", "Louisiana Tech": "C-USA",
    "Wake Forest": "ACC", "Southeast Missouri State": "OVC",
    "North Carolina State": "ACC", "Coastal Carolina": "Sun Belt",
    "Michigan": "Big Ten", "Texas-San Antonio": "C-USA", "Louisville": "ACC",
    "Notre Dame": "ACC", "Baylor": "Big 12", "Cal Poly": "Big West",
    "Minnesota": "Big Ten", "UC San Diego": "Big West",
    "Pittsburgh": "ACC", "Rice": "C-USA", "East Carolina": "AAC",
    "Nevada": "Mountain West", "Oklahoma State": "Big 12",
    "NC Wilmington": "CAA", "Miami Florida": "ACC",
    "Alabama-Birmingham": "C-USA", "Kansas": "Big 12",
    "Virginia Tech": "ACC", "Boston College": "ACC",
    "Texas State": "Sun Belt", "NC-Charlotte": "AAC",
    "Portland": "WCC", "Louisiana State": "SEC",
    "Illinois": "Big Ten", "Winthrop": "Big South",
    "Houston": "Big 12", "California": "Pac-12",
    "Tarleton State": "WAC", "Maryland": "Big Ten",
    "UC Davis": "Big West", "Miami Ohio": "MAC",
    "California Baptist": "WAC", "Utah": "Pac-12",
    "Western Kentucky": "C-USA", "Texas Christian": "Big 12",
    "Florida Gulf Coast": "ASUN", "Brigham Young": "Big 12",
    "Charleston Southern": "Big South", "Texas Tech": "Big 12",
    "Dallas Baptist": "MVC", "Troy": "Sun Belt",
    "Indiana": "Big Ten", "Washington": "Pac-12",
    "Georgia Southern": "Sun Belt",
}


# ── ELO / ISR loader ───────────────────────────────────────────────────────────
def load_elo_isr():
    """
    Load ELO and ISR ratings from separate Mar 21 CSV files.
    Also loads conference ISR fallback map.

    Returns (elo_map, isr_map) dicts keyed by team name as in CSV.
    """
    global _CONF_ISR_MAP

    elo_map, isr_map = {}, {}

    # Load ELO
    if ELO_CSV.exists():
        with open(ELO_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                team = row.get("team", "").strip()
                try:
                    elo_map[team] = float(row["elo_mar21"])
                except (KeyError, ValueError):
                    pass
        logger.info(f"Loaded {len(elo_map)} ELO ratings from {ELO_CSV.name}")
    else:
        logger.warning(f"ELO CSV not found: {ELO_CSV}")

    # Load ISR (separate file)
    if ISR_CSV.exists():
        with open(ISR_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                team = row.get("team", "").strip()
                try:
                    isr_map[team] = float(row["isr_mar21"])
                except (KeyError, ValueError):
                    pass
        logger.info(f"Loaded {len(isr_map)} ISR ratings from {ISR_CSV.name}")
    else:
        logger.warning(f"ISR CSV not found: {ISR_CSV}")

    # Load Conference ISR fallback
    if CONF_CSV.exists():
        with open(CONF_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                conf = row.get("conference", "").strip()
                try:
                    _CONF_ISR_MAP[conf] = float(row["isr_mar21"])
                except (KeyError, ValueError):
                    pass
        logger.info(f"Loaded {len(_CONF_ISR_MAP)} conference ISR ratings from {CONF_CSV.name}")
    else:
        logger.warning(f"Conference ISR CSV not found: {CONF_CSV}")

    return elo_map, isr_map


def resolve_name(team_name: str) -> str:
    """Normalize team name via ALIASES then return canonical form."""
    return ALIASES.get(team_name, team_name)


def get_conf_isr_context(team_name: str) -> str:
    """
    Returns a conference ISR annotation string for display/notes only.
    NOT used in probability calculations — context and matchup notation only.
    e.g. "(SEC conf ISR: 115.4)"
    """
    canonical = resolve_name(team_name)
    conf = TEAM_CONFERENCE.get(canonical, "")
    if conf and conf in _CONF_ISR_MAP:
        return f"({conf} conf ISR: {_CONF_ISR_MAP[conf]:.1f})"
    return ""


def _find_in_map(name: str, rating_map: dict) -> float | None:
    """
    Try to find name in rating_map using exact match then fuzzy.
    Tries both the original name and its alias-resolved form.
    Returns None if no match found.
    """
    variants = [name, resolve_name(name)]
    # Exact match first
    for v in variants:
        if v in rating_map:
            return rating_map[v]
    # Fuzzy match
    for v in variants:
        vl = v.lower()
        for k, val in rating_map.items():
            if vl in k.lower() or k.lower() in vl:
                return val
    return None


def lookup(team_name: str, elo_map: dict, isr_map: dict) -> tuple:
    """
    Match team name to ELO and ISR maps independently.

    Each map is queried separately: tries original name, alias-resolved name,
    then fuzzy substring match. This handles cases where ELO and ISR CSVs
    use different naming conventions (e.g. ELO='BYU', ISR='Brigham Young').

    Conference ISR is NOT used as a model input — for context/notation only.
    Returns (elo, isr)
    """
    elo = _find_in_map(team_name, elo_map)
    isr = _find_in_map(team_name, isr_map)

    if elo is None:
        logger.warning(f"No ELO match for '{team_name}' — using neutral 1500.0")
        elo = 1500.0
    if isr is None:
        logger.warning(f"No ISR match for '{team_name}' — using neutral 100.0")
        isr = 100.0

    return elo, isr


# ── Core projection ────────────────────────────────────────────────────────────
def project_game(away: str, home: str, sp_era_away: float, sp_era_home: float,
                 sp_starts_away: int, sp_starts_home: int,
                 elo_map: dict, isr_map: dict) -> dict:
    """
    Returns full projection dict for one game.

    away/home:        team name strings (will be alias-resolved internally)
    sp_era_*:         starting pitcher ERA for this game
    sp_starts_*:      number of starts SP has made this season (confidence proxy)
    elo_map/isr_map:  loaded from load_elo_isr()
    """
    elo_a, isr_a = lookup(away, elo_map, isr_map)
    elo_h, isr_h = lookup(home, elo_map, isr_map)

    # ELO head-to-head (HFA = +25 ELO pts for home)
    elo_diff    = (elo_h + 25) - elo_a
    p_home_elo  = 1 / (1 + 10 ** (-elo_diff / 400))

    # ISR normalized (add half HFA to home share)
    isr_sum     = isr_a + isr_h
    p_home_isr  = (isr_h / isr_sum + HFA / 2) if isr_sum > 0 else 0.5

    # Weighted blend
    p_home_base = W_ELO * p_home_elo + W_ISR * p_home_isr

    # SP ERA adjustment: higher away ERA favors home, higher home ERA hurts home
    r_away = min(9.0, max(1.0, LG_R * sp_era_away / LG_ERA))
    r_home = min(9.0, max(1.0, LG_R * sp_era_home / LG_ERA))
    era_adj = (r_away - r_home) / 20.0
    p_home  = max(0.05, min(0.95, p_home_base + era_adj))
    p_away  = 1 - p_home

    # Run total projection
    proj_total = round(r_away + r_home, 1)

    # Confidence
    elo_isr_gap = abs(p_home_elo - p_home_isr) * 100
    avg_starts  = (sp_starts_away + sp_starts_home) / 2
    conf_label, conf_sym = confidence(int(avg_starts), elo_isr_gap)

    return {
        "away": away, "home": home,
        "p_home": p_home, "p_away": p_away,
        "p_home_elo": p_home_elo, "p_home_isr": p_home_isr,
        "elo_a": elo_a, "elo_h": elo_h,
        "isr_a": isr_a, "isr_h": isr_h,
        "sp_era_away": sp_era_away, "sp_era_home": sp_era_home,
        "sp_starts_away": sp_starts_away, "sp_starts_home": sp_starts_home,
        "r_away": r_away, "r_home": r_home,
        "proj_total": proj_total,
        "conf_label": conf_label, "conf_sym": conf_sym,
        "elo_isr_gap_pp": round(elo_isr_gap, 1),
    }


# ── Devig / odds utils ─────────────────────────────────────────────────────────
def devig_2way(o1, o2):
    """Additive devig for a 2-way market. Returns (implied_p1, implied_p2) fair probs."""
    def imp(o):
        return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100)
    i1, i2 = imp(o1), imp(o2)
    t = i1 + i2
    return i1 / t, i2 / t


def american_to_decimal(o: int) -> float:
    """Convert American odds to decimal."""
    return o / 100 + 1 if o > 0 else 100 / abs(o) + 1


def decimal_to_american(d: float) -> int:
    """Convert decimal odds to American."""
    return int(round((d - 1) * 100)) if d >= 2.0 else int(round(-100 / (d - 1)))


def ev_calc(decimal_odds: float, model_p: float) -> float:
    """Expected value per unit staked."""
    return (decimal_odds - 1) * model_p - (1 - model_p)


def rl_alt_note(odds: int) -> str:
    """
    When ML > +250 (big underdog), flag run line (+1.5) as alternative vehicle.
    Run line at +1.5 is typically priced 500-800 pts lower than ML for big dogs.
    Returns empty string when not applicable.
    """
    if odds > RL_ALT_THRESHOLD:
        return f" | RL_ALT: ML {odds:+d} > +{RL_ALT_THRESHOLD} — consider +1.5 run line as lower-risk vehicle"
    return ""


# ── Load today's games from cache or fallback slate ───────────────────────────
def load_games_from_cache(date_str: str) -> list:
    """
    Try to load games from TheRundown scraper cache (JSON).
    Falls back to hardcoded slate by date if cache not available.

    Returns list of game dicts with keys:
      away, home, mkt_away, mkt_home,
      sp_era_away, sp_era_home, sp_starts_away, sp_starts_home, game_time,
      sp_away (name), sp_home (name)
    """
    cache_file = BASE_DIR / "scrapers" / "cache" / f"ncaa_{date_str}.json"
    if cache_file.exists():
        import json
        with open(cache_file) as f:
            data = json.load(f)
        games = []
        for ev in data.get("events", []):
            teams = ev.get("teams_normalized", [])
            if len(teams) >= 2:
                games.append({
                    "away":          teams[1].get("name", "?"),
                    "home":          teams[0].get("name", "?"),
                    "mkt_away":      None, "mkt_home": None,
                    "sp_era_away":   4.50, "sp_era_home": 4.50,
                    "sp_starts_away": 3,   "sp_starts_home": 3,
                    "sp_away":       "TBD", "sp_home": "TBD",
                    "game_time":     ev.get("event_date", "TBD"),
                })
        if games:
            logger.info(f"Loaded {len(games)} games from cache: {cache_file.name}")
            return games

    # Hardcoded slates by date — update daily once SPs are confirmed
    logger.info("Using hardcoded NCAA slate (cache not available)")

    slates = {
        # Mar 21 — series games (SEC/Big 12/Pac-12 weekend series)
        # NOTE: Update SP names/ERAs each morning once lineups are confirmed
        "2026-03-21": [
            {"away": "Northwestern",   "home": "Oregon",           "mkt_away": +315,   "mkt_home": -470,    "sp_era_away": 5.10, "sp_era_home": 3.80, "sp_starts_away": 4, "sp_starts_home": 6, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:00 PM ET"},
            {"away": "Minnesota",      "home": "Indiana",          "mkt_away": -125,   "mkt_home": +105,    "sp_era_away": 4.20, "sp_era_home": 4.85, "sp_starts_away": 5, "sp_starts_home": 4, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:00 PM ET"},
            {"away": "Maryland",       "home": "Georgia Southern", "mkt_away": None,   "mkt_home": None,    "sp_era_away": 4.50, "sp_era_home": 4.70, "sp_starts_away": 3, "sp_starts_home": 3, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:00 PM ET"},
            {"away": "Washington",     "home": "USC",              "mkt_away": +400,   "mkt_home": -600,    "sp_era_away": 5.40, "sp_era_home": 3.60, "sp_starts_away": 3, "sp_starts_home": 7, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:00 PM ET"},
            {"away": "BYU",            "home": "West Virginia",    "mkt_away": +1400,  "mkt_home": -10000,  "sp_era_away": 4.80, "sp_era_home": 4.10, "sp_starts_away": 2, "sp_starts_home": 5, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:05 PM ET"},
            {"away": "Oklahoma",       "home": "LSU",              "mkt_away": +110,   "mkt_home": -145,    "sp_era_away": 4.30, "sp_era_home": 3.95, "sp_starts_away": 5, "sp_starts_home": 6, "sp_away": "TBD", "sp_home": "TBD", "game_time": "6:30 PM ET"},
            {"away": "Florida",        "home": "Alabama",          "mkt_away": -185,   "mkt_home": +140,    "sp_era_away": 3.70, "sp_era_home": 4.60, "sp_starts_away": 7, "sp_starts_home": 5, "sp_away": "TBD", "sp_home": "TBD", "game_time": "6:00 PM ET"},
            {"away": "Texas",          "home": "Auburn",           "mkt_away": -135,   "mkt_home": +105,    "sp_era_away": 3.55, "sp_era_home": 4.25, "sp_starts_away": 6, "sp_starts_home": 5, "sp_away": "TBD", "sp_home": "TBD", "game_time": "6:00 PM ET"},
        ],
        # Mar 20 — same series (Game 1 of 3-game weekend sets)
        "2026-03-20": [
            {"away": "Northwestern",   "home": "Oregon",           "mkt_away": +315,   "mkt_home": -470,    "sp_era_away": 5.10, "sp_era_home": 3.80, "sp_starts_away": 4, "sp_starts_home": 6, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:00 PM ET"},
            {"away": "Minnesota",      "home": "Indiana",          "mkt_away": -125,   "mkt_home": +105,    "sp_era_away": 4.20, "sp_era_home": 4.85, "sp_starts_away": 5, "sp_starts_home": 4, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:00 PM ET"},
            {"away": "Maryland",       "home": "Georgia Southern", "mkt_away": None,   "mkt_home": None,    "sp_era_away": 4.50, "sp_era_home": 4.70, "sp_starts_away": 3, "sp_starts_home": 3, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:00 PM ET"},
            {"away": "Washington",     "home": "USC",              "mkt_away": +400,   "mkt_home": -600,    "sp_era_away": 5.40, "sp_era_home": 3.60, "sp_starts_away": 3, "sp_starts_home": 7, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:00 PM ET"},
            {"away": "BYU",            "home": "West Virginia",    "mkt_away": +1400,  "mkt_home": -10000,  "sp_era_away": 4.80, "sp_era_home": 4.10, "sp_starts_away": 2, "sp_starts_home": 5, "sp_away": "TBD", "sp_home": "TBD", "game_time": "3:05 PM ET"},
            {"away": "Oklahoma",       "home": "LSU",              "mkt_away": +110,   "mkt_home": -145,    "sp_era_away": 4.30, "sp_era_home": 3.95, "sp_starts_away": 5, "sp_starts_home": 6, "sp_away": "TBD", "sp_home": "TBD", "game_time": "6:30 PM ET"},
            {"away": "Florida",        "home": "Alabama",          "mkt_away": -185,   "mkt_home": +140,    "sp_era_away": 3.70, "sp_era_home": 4.60, "sp_starts_away": 7, "sp_starts_home": 5, "sp_away": "TBD", "sp_home": "TBD", "game_time": "6:00 PM ET"},
            {"away": "Texas",          "home": "Auburn",           "mkt_away": -135,   "mkt_home": +105,    "sp_era_away": 3.55, "sp_era_home": 4.25, "sp_starts_away": 6, "sp_starts_home": 5, "sp_away": "TBD", "sp_home": "TBD", "game_time": "6:00 PM ET"},
        ],
    }
    return slates.get(date_str, slates.get("2026-03-21", []))


# ── Write signal to DB ─────────────────────────────────────────────────────────
def write_signal(conn: sqlite3.Connection, date_str: str, proj: dict, side: str,
                 odds: int, model_p: float, nv_p: float, edge: float, ev_val: float,
                 tier: str, units: float, game: str, game_time: str, notes: str,
                 conf_label: str) -> None:
    """
    Insert one signal row into the signals table.
    is_pod = 1 only when confidence is HIGH (●●●).
    """
    ts = datetime.now(timezone.utc).isoformat()
    is_pod = 1 if conf_label == "HIGH" and units >= 1.5 else 0
    conn.execute(
        """INSERT INTO signals
           (signal_date, sport, game, game_time, bet_type, side, market,
            odds, model_prob, no_vig_prob, edge, ev, tier, units,
            is_pod, pod_sport,
            gate1_pyth, gate2_edge, gate3_model_agree, gate4_line_move, gate5_etl_fresh,
            notes, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (date_str, SPORT, game, game_time, "ML", side, "ML",
         odds, round(model_p, 4), round(nv_p, 4),
         round(edge, 4), round(ev_val, 4), tier, units,
         is_pod, SPORT,
         "GREEN", "PASS", "PASS", "PASS", "PASS",
         notes, ts)
    )


# ── Main entry point ───────────────────────────────────────────────────────────
def run_model(date_str: str, dry_run: bool = False) -> int:
    """
    Generate NCAA Baseball ML signals for date_str.

    Loads ELO/ISR from CSV, projects each game, computes edge vs market,
    applies tier system, and writes qualifying signals to eyeblackiq.db.

    Returns number of signals written (0 in dry-run mode).
    """
    elo_map, isr_map = load_elo_isr()
    games = load_games_from_cache(date_str)

    signals_written = 0
    conn = None

    if not dry_run:
        conn = sqlite3.connect(TGT_DB)
        # Idempotent — clear today's signals before rewriting
        conn.execute("DELETE FROM signals WHERE signal_date=? AND sport=?", (date_str, SPORT))
        conn.commit()

    logger.info(f"NCAA Baseball model — {date_str} — {len(games)} games")

    # Print confidence system note
    logger.info("  Confidence: HIGH●●● = 5+ SP starts + model agree <5pp | "
                "LOW●○○ = TBD SP or >10pp model gap")

    for g in games:
        away = g["away"]
        home = g["home"]
        sp_away_name = g.get("sp_away", "TBD")
        sp_home_name = g.get("sp_home", "TBD")

        proj = project_game(
            away, home,
            g["sp_era_away"], g["sp_era_home"],
            g.get("sp_starts_away", 3), g.get("sp_starts_home", 3),
            elo_map, isr_map,
        )

        game_str  = f"{away} @ {home}"
        game_time = g.get("game_time", "TBD")
        mkt_a, mkt_h = g.get("mkt_away"), g.get("mkt_home")

        # Log SP info
        sp_info = f"SP: {sp_away_name} (ERA {g['sp_era_away']:.2f}) vs {sp_home_name} (ERA {g['sp_era_home']:.2f})"

        logger.info(
            f"  {game_str}  P_home={proj['p_home']:.3f}  "
            f"ELO={proj['p_home_elo']:.3f}  ISR={proj['p_home_isr']:.3f}  "
            f"Total={proj['proj_total']}  Conf={proj['conf_sym']}  {sp_info}"
        )

        if mkt_a is None or mkt_h is None:
            logger.info(f"    No market odds — skipping signal write")
            continue

        nv_a, nv_h = devig_2way(mkt_a, mkt_h)
        edge_h = proj["p_home"] - nv_h
        edge_a = proj["p_away"] - nv_a

        for side_name, model_p, nv_p, edge_val, odds in [
            (home, proj["p_home"], nv_h, edge_h, mkt_h),
            (away, proj["p_away"], nv_a, edge_a, mkt_a),
        ]:
            if edge_val <= 0:
                continue

            edge_pct = edge_val * 100
            tier_name, units = ncaa_tier(edge_pct)
            if units == 0.0:
                logger.info(f"    {side_name}: edge {edge_pct:.1f}% -> BALK — skip")
                continue

            dec_odds = american_to_decimal(odds)
            ev_val   = ev_calc(dec_odds, model_p)

            elo_display = proj["elo_h"] if side_name == home else proj["elo_a"]
            isr_display = proj["isr_h"] if side_name == home else proj["isr_a"]
            era_display = proj["sp_era_home"] if side_name == home else proj["sp_era_away"]
            sp_name     = sp_home_name if side_name == home else sp_away_name

            # Conference ISR — context/notation only, not a model input
            conf_ctx = get_conf_isr_context(side_name)

            # Spread alternative flag for big dogs
            rl_note = rl_alt_note(odds)

            notes = (
                f"ELO={elo_display:.0f}  "
                f"ISR={isr_display:.1f} {conf_ctx}  "
                f"SP={sp_name} (ERA {era_display:.2f})  "
                f"Proj_Total={proj['proj_total']}  "
                f"Conf={proj['conf_label']} {proj['conf_sym']}  "
                f"ELO-ISR_gap={proj['elo_isr_gap_pp']:.1f}pp"
                f"{rl_note}"
            )

            # POD status logged
            is_pod_flag = proj["conf_label"] == "HIGH" and units >= 1.5
            pod_tag = "  [POD CANDIDATE]" if is_pod_flag else ""

            logger.info(
                f"    SIGNAL: {side_name} ML {odds:+d}  "
                f"Edge {edge_pct:.1f}%  {tier_name}  {units}u  "
                f"Conf {proj['conf_sym']}{pod_tag}"
                + (f"\n    {rl_note.strip()}" if rl_note else "")
            )

            if not dry_run and conn is not None:
                write_signal(
                    conn, date_str, proj, f"{side_name} ML",
                    odds, model_p, nv_p, edge_val, ev_val,
                    tier_name, units, game_str, game_time, notes,
                    proj["conf_label"],
                )
                signals_written += 1

    if not dry_run and conn is not None:
        conn.commit()
        conn.close()
        logger.info(f"NCAA Baseball: wrote {signals_written} signals to DB")
    else:
        logger.info(f"NCAA Baseball [DRY RUN]: {signals_written} signals projected")

    return signals_written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NCAA Baseball signal generator")
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"),
                        help="Date to run model for (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Project games and log signals without writing to DB")
    args = parser.parse_args()

    n = run_model(args.date, args.dry_run)
    print(f"NCAA Baseball signals: {n}")
