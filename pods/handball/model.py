"""
EyeBlackIQ — pods/handball/model.py
=====================================
Handball Team ML + Totals via the Efficiency-Flow Model.

=== MODEL ARCHITECTURE — EFFICIENCY-FLOW ===

Handball is a "Possession-Flow" sport. We focus on POSSESSIONS, not raw
goals, because teams that play a slow pace look like good defenses but are
actually just low-possession teams. Total-goals SOS is misleading.

FORMULA HIERARCHY (in order):
  1. ELO Rating       : Head-to-head win probability from historical W/L + margin
  2. Team xG          : xG = possessions_per_game × shot_efficiency
                       where shot_efficiency = goals / total_shots
  3. Adjusted Eff     : adj_eff = raw_xG × (lg_avg_def_eff / opp_def_eff)
                       Note: adjusts for how "stingy" the opponent's defense is.
                       lg_avg_def_eff = league average goals allowed per possession.
                       If opp allows fewer → adj_eff goes DOWN (tougher defense).
  4. Poisson Win Prob : P(home>away) via Skellam distribution (diff of two Poissons)
  5. Signal Blend     : 0.55 × ELO_prob + 0.45 × Poisson_prob
                       (ELO heavier early-season before possession data is reliable)

PLAYER PROPS (when player-level data available):
  proj_goals = adj_usage × shot_efficiency × proj_team_possessions
  adj_usage  = base_usage + proportional share of any injured player's vacuum
  usage_share = (player_shots + player_turnovers) / team_possessions
  shot_efficiency = player_goals / player_shots

EDGE WINDOW:
  - ML / Totals  : 3% – 20%  (flagged_high above 20%)
  - PODs         : bypass cap — human-reviewed, always on slip

HFA: 0.06 (handball has strong home advantage; EHF CL ~58-60% home win rate)
HFA_ELO: 50 points added to home ELO for head-to-head calculation

POISSON: Standard Poisson for goals per team per half (NOT zero-inflated).
         EHF CL averages ~56-62 total goals/game (28-32 per team).

HANDBALL-SPECIFIC NOTES:
  - Draws are rare (<5%) — most leagues use OT/shootout → treat as 2-way ML
  - Late-season rotation risk significant (CL load management)
  - Recommend EHF CL as primary league (deepest data + sharpest odds market)

STATUS: DATA_PHASE until handball_matches table has >= 90 games
        (3 seasons × ~30 group-stage games = ~90 minimum)

GO-LIVE THRESHOLDS (per EyeBlackIQ spec):
  ROI > 3% · CLV >= 55% · Calibration ±2% · >= 300/500 sample bets
  30-day paper trade before real money

Usage:
  python pods/handball/model.py --date 2026-03-21
  python pods/handball/model.py --date 2026-03-21 --dry-run
  python pods/handball/model.py --date 2026-03-21 --verbose
"""

import os
import math
import sqlite3
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ── Environment ──────────────────────────────────────────────────────────────
_ENV = Path(__file__).resolve()
for _ in range(6):
    _ENV = _ENV.parent
    _cand = _ENV / ".env"
    if _cand.exists():
        load_dotenv(_cand)
        break
else:
    load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
TGT_DB   = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"

# ── Model constants ───────────────────────────────────────────────────────────
SPORT           = "HANDBALL"
MODEL_VERSION   = "1.0.0"
STATUS          = "DATA_PHASE"        # flips to LIVE when go-live thresholds cleared

# Data gate — require minimum matches before model is trusted
MIN_MATCHES_FOR_ELO  = 30            # per team minimum across league history
MIN_LEAGUE_MATCHES   = 90            # total in DB before model goes live

# Blending weights (ELO vs Poisson xG)
W_ELO            = 0.55             # ELO weight
W_POISSON        = 0.45             # Poisson xG weight

# Home-field advantage
HFA_PROB         = 0.06             # flat HFA added to home probability
HFA_ELO          = 50              # ELO points added to home rating

# ELO calibration
ELO_K_EARLY      = 32              # K-factor for new teams (< 20 games)
ELO_K_STANDARD   = 20             # K-factor standard
ELO_DEFAULT      = 1500            # new-team starting ELO

# League stats defaults (EHF Champions League calibration)
LG_AVG_GOALS_PER_GAME = 29.5       # per team per game (home + away ~ 59 total)
LG_AVG_POSSESSIONS    = 52         # possessions per team per game
LG_AVG_SHOT_EFF       = 0.568      # goals / shots (league average)
LG_AVG_DEF_EFF        = 0.568      # opp goals allowed per shot (same in balanced model)

# Edge window
MIN_EDGE          = 0.03           # below this = flagged_low
MAX_EDGE_TEAM     = 0.20           # ML / Totals cap
MAX_EDGE_PROP     = 0.25           # player goals prop cap

# Total goals line (used when no market line available)
DEFAULT_TOTAL_LINE = 59.5

# Minimum data for player prop signal
MIN_PLAYER_SHOTS_SAMPLE = 20


# ── Tier system ──────────────────────────────────────────────────────────────
def handball_tier(edge_pct: float) -> tuple:
    """Returns (tier_label, units) for edge in 0–100 scale."""
    if   edge_pct >= 12: return ("FAST BREAK", 2.0)   # T1 — high conviction
    elif edge_pct >=  7: return ("WHEELHOUSE",  1.5)   # T2 — solid edge
    elif edge_pct >=  4: return ("MONITOR",     1.0)   # T2 — worth tracking
    elif edge_pct >=  2: return ("SCOUT",       0.5)   # T3 — informational
    else:                return ("BALK",         0.0)  # below threshold


# ── Confidence scoring ────────────────────────────────────────────────────────
def handball_confidence(
    edge_pct: float,
    n_team_games: int,
    model_agree_pp: float,
    has_possession_data: bool,
) -> tuple:
    """
    Returns (label, symbol) e.g. ('HIGH', '●●●')

    Scoring (start at MED=2, adjust up/down):
      +1: edge >= 12% (model very decisive)
      +1: n_team_games >= 30 (ELO well-calibrated)
      +1: possession data available (xG reliable)
      +1: model agreement within 5pp
      -1: edge < 4%
      -1: n_team_games < 15 (early season, ELO thin)
      -1: model disagreement > 10pp
    HIGH requires: score >= 3
    """
    score = 2
    if edge_pct >= 12:         score += 1
    if edge_pct < 4:           score -= 1
    if n_team_games >= 30:     score += 1
    if n_team_games < 15:      score -= 1
    if has_possession_data:    score += 1
    if model_agree_pp <= 5:    score += 1
    elif model_agree_pp > 10:  score -= 1
    score = max(1, min(3, score))
    return {3: ("HIGH", "●●●"), 2: ("MED", "●●○"), 1: ("LOW", "●○○")}[score]


# ── Probability helpers ───────────────────────────────────────────────────────
def american_to_prob(odds: int) -> float:
    """Convert American odds to implied probability (no vig)."""
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def no_vig_prob(home_odds: int, away_odds: int) -> tuple:
    """Devig two-way market. Returns (p_home_nv, p_away_nv)."""
    p_h = american_to_prob(home_odds)
    p_a = american_to_prob(away_odds)
    total = p_h + p_a
    if total == 0:
        return 0.5, 0.5
    return p_h / total, p_a / total


def elo_win_prob(elo_home: float, elo_away: float, hfa_elo: float = HFA_ELO) -> float:
    """
    ELO win probability for home team.
    P_home = 1 / (1 + 10^(-(elo_home + hfa_elo - elo_away) / 400))
    """
    diff = (elo_home + hfa_elo) - elo_away
    return 1 / (1 + 10 ** (-diff / 400))


def poisson_pmf(k: int, lam: float) -> float:
    """P(X = k) for Poisson(lam). Uses log-space to avoid overflow."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    log_p = k * math.log(lam) - lam - sum(math.log(i) for i in range(1, k + 1))
    return math.exp(log_p)


def poisson_win_prob(lambda_home: float, lambda_away: float, max_goals: int = 60) -> tuple:
    """
    P(home wins), P(draw), P(away wins) from two independent Poisson distributions.
    Uses convolution (no scipy required — pure Python).

    For handball ML (2-way): p_home_ml = p_home + 0.5 * p_draw (OT/SO resolution)
    """
    p_win = 0.0
    p_draw = 0.0
    p_loss = 0.0

    # Precompute PMFs
    pmf_h = [poisson_pmf(k, lambda_home) for k in range(max_goals + 1)]
    pmf_a = [poisson_pmf(k, lambda_away) for k in range(max_goals + 1)]

    for h in range(max_goals + 1):
        for a in range(max_goals + 1):
            prob = pmf_h[h] * pmf_a[a]
            if h > a:
                p_win  += prob
            elif h == a:
                p_draw += prob
            else:
                p_loss += prob

    # OT/SO tiebreaker: split draws 50/50
    p_home_ml = p_win + 0.5 * p_draw
    p_away_ml = p_loss + 0.5 * p_draw
    return p_home_ml, p_away_ml


def edge_calc(model_prob: float, nv_prob: float) -> float:
    """Edge = model_prob - no_vig_prob."""
    return model_prob - nv_prob


def ev_calc(decimal_odds: float, model_p: float) -> float:
    """EV = (decimal_odds - 1) * model_p - (1 - model_p)."""
    return (decimal_odds - 1) * model_p - (1 - model_p)


def american_to_decimal(odds: int) -> float:
    """Convert American odds to decimal."""
    return odds / 100 + 1 if odds > 0 else 100 / abs(odds) + 1


# ── Adjusted xG (SOS baked) ───────────────────────────────────────────────────
def calc_adj_xg(
    team_possessions: float,
    team_shot_eff: float,
    opp_def_eff: float,
    lg_avg_def_eff: float = LG_AVG_DEF_EFF,
) -> float:
    """
    Adjusted xG incorporating opponent defensive quality.

    Formula (from EyeBlackIQ Handball Efficiency-Flow spec):
      raw_xG   = possessions × shot_efficiency
      adj_xG   = raw_xG × (lg_avg_def_eff / opp_def_eff)

    If opp_def_eff < lg_avg (stingy defense): adj_xG < raw_xG (harder to score)
    If opp_def_eff > lg_avg (leaky defense):  adj_xG > raw_xG (easier to score)

    opp_def_eff = goals_allowed_per_shot by opponent (lower = better defense)
    """
    raw_xg = team_possessions * team_shot_eff
    if opp_def_eff <= 0 or lg_avg_def_eff <= 0:
        return raw_xg
    sos_mult = lg_avg_def_eff / opp_def_eff
    # Cap SOS multiplier at ±40% to avoid extreme swings with small samples
    sos_mult = max(0.60, min(1.40, sos_mult))
    return raw_xg * sos_mult


# ── Player prop usage redistribution ─────────────────────────────────────────
def adjust_player_usage(players: list, inactive_id: str) -> list:
    """
    Redistribute usage from an inactive player to remaining active players.

    Implementation from EyeBlackIQ Handball spec (Usage Vacuum model):
      vacuum = inactive_player.base_usage
      Each active player gets: adj_usage = base_usage + (base_usage / total_active) × vacuum
      projected_goals = adj_usage × shot_efficiency × team_possessions_proj

    players: list of dicts with keys:
      id, name, base_usage, shot_efficiency, is_active
    inactive_id: str — player id of the inactive/injured player

    Returns list of active players with adj_usage and projected_goals added.
    """
    # Identify usage vacuum
    inactive = next((p for p in players if p["id"] == inactive_id), None)
    if not inactive:
        return [p for p in players if p.get("is_active")]

    vacuum = inactive.get("base_usage", 0)
    active = [p for p in players if p.get("is_active") and p["id"] != inactive_id]

    if not active:
        return active

    total_active_usage = sum(p.get("base_usage", 0) for p in active)

    for p in active:
        base = p.get("base_usage", 0)
        if total_active_usage > 0:
            p["adj_usage"] = base + (base / total_active_usage) * vacuum
        else:
            p["adj_usage"] = base
        p["projected_goals"] = p["adj_usage"] * p.get("shot_efficiency", 0)

    return active


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(TGT_DB)
    conn.row_factory = sqlite3.Row
    return conn


def get_team_stats(team_name: str, league_id: int = None) -> Optional[dict]:
    """
    Load team ELO + possession efficiency from handball_team_stats.
    Returns None if table doesn't exist or team not found.
    """
    try:
        with get_conn() as conn:
            q = """SELECT team_name, games_played, elo_rating,
                          goals_for, goals_against,
                          possessions_per_game, shots_per_game,
                          shot_efficiency, def_goals_allowed_per_shot,
                          wins, losses
                   FROM handball_team_stats
                   WHERE team_name = ?"""
            params = [team_name]
            if league_id:
                q += " AND league_id = ?"
                params.append(league_id)
            q += " ORDER BY season DESC LIMIT 1"
            row = conn.execute(q, params).fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError:
        return None  # table doesn't exist yet → DATA_PHASE


def get_league_stats(league_id: int = None) -> dict:
    """
    Compute league-average efficiency stats from handball_team_stats.
    Falls back to EHF CL calibrated defaults if table empty.
    """
    defaults = {
        "lg_avg_possessions": LG_AVG_POSSESSIONS,
        "lg_avg_shot_eff":    LG_AVG_SHOT_EFF,
        "lg_avg_def_eff":     LG_AVG_DEF_EFF,
        "lg_avg_goals":       LG_AVG_GOALS_PER_GAME,
        "total_matches":      0,
    }
    try:
        with get_conn() as conn:
            q = """SELECT AVG(possessions_per_game), AVG(shot_efficiency),
                          AVG(def_goals_allowed_per_shot), AVG(goals_for),
                          SUM(games_played) / 2
                   FROM handball_team_stats"""
            params = []
            if league_id:
                q += " WHERE league_id = ?"
                params.append(league_id)
            row = conn.execute(q, params).fetchone()
        if row and row[0] is not None:
            return {
                "lg_avg_possessions": row[0] or defaults["lg_avg_possessions"],
                "lg_avg_shot_eff":    row[1] or defaults["lg_avg_shot_eff"],
                "lg_avg_def_eff":     row[2] or defaults["lg_avg_def_eff"],
                "lg_avg_goals":       row[3] or defaults["lg_avg_goals"],
                "total_matches":      int(row[4] or 0),
            }
    except sqlite3.OperationalError:
        pass
    return defaults


def count_handball_matches() -> int:
    """Returns total matches in handball_matches table. 0 if table doesn't exist."""
    try:
        with get_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM handball_matches").fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def write_signal(signal: dict, dry_run: bool = False) -> Optional[int]:
    """Write a signal to the signals table. Returns inserted row ID."""
    if dry_run:
        logger.info(f"[DRY-RUN] Would write: {signal.get('side')} | edge={signal.get('edge'):.3f}")
        return None
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT OR REPLACE INTO signals
               (signal_date, sport, game, game_time, bet_type, side, market,
                odds, model_prob, no_vig_prob, edge, ev, tier, units,
                is_pod, pod_sport, notes,
                gate1_pyth, gate2_edge, gate3_model_agree,
                gate4_line_move, gate5_etl_fresh,
                pick_source, b2b_flag, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                signal["signal_date"], SPORT, signal["game"], signal.get("game_time"),
                signal["bet_type"], signal["side"], signal["market"],
                signal["odds"], signal["model_prob"], signal["no_vig_prob"],
                signal["edge"], signal.get("ev", 0), signal["tier"], signal["units"],
                int(signal.get("is_pod", 0)), SPORT if signal.get("is_pod") else None,
                signal.get("notes", ""),
                signal.get("gate1", "PASS"), signal.get("gate2", "PASS"),
                signal.get("gate3", "PASS"), signal.get("gate4", "PASS"),
                signal.get("gate5", "PASS"),
                signal.get("pick_source", "SPORTSBOOK"),
                signal.get("b2b_flag"),
                now,
            )
        )
        conn.commit()
    return cur.lastrowid


# ── 5-Gate filter ─────────────────────────────────────────────────────────────
def run_gates(
    p_home_model: float,
    p_home_nv: float,
    edge: float,
    n_matches: int,
    etl_hours_old: float = 0.0,
) -> dict:
    """
    EyeBlackIQ 5-gate sequential filter for handball.

    Gate 1 (Pythagorean / data quality): Model agrees with ELO direction
    Gate 2 (Edge): edge >= MIN_EDGE (3%)
    Gate 3 (Model agreement): ELO and Poisson within 15pp
    Gate 4 (Line movement): placeholder — requires live line feed
    Gate 5 (ETL freshness): data < 4 hours old

    Returns dict with gate statuses and overall pass/fail.
    """
    gates = {}

    # Gate 1: Data quality — need minimum sample
    if n_matches >= 30:
        gates["gate1"] = "GREEN"
    elif n_matches >= 10:
        gates["gate1"] = "YELLOW"
    else:
        gates["gate1"] = "RED"

    # Gate 2: Edge threshold
    gates["gate2"] = "PASS" if edge >= MIN_EDGE else "FAIL"

    # Gate 3: Placeholder (set by caller after ELO vs Poisson comparison)
    gates["gate3"] = "PASS"  # updated by get_signals()

    # Gate 4: Line movement (placeholder — live feed integration pending)
    gates["gate4"] = "PASS"

    # Gate 5: ETL freshness (< 4 hours)
    gates["gate5"] = "PASS" if etl_hours_old < 4 else "FAIL"

    # Overall: Gate 1 RED = blocked. All gates must pass.
    blocked_by = None
    if gates["gate1"] == "RED":
        blocked_by = "gate1_data"
    elif gates["gate2"] == "FAIL":
        blocked_by = "gate2_edge"
    elif gates["gate3"] == "FAIL":
        blocked_by = "gate3_model"
    elif gates["gate4"] == "FAIL":
        blocked_by = "gate4_line"
    elif gates["gate5"] == "FAIL":
        blocked_by = "gate5_etl"

    gates["passes"] = blocked_by is None
    gates["blocked_by"] = blocked_by
    return gates


# ── Main model function ───────────────────────────────────────────────────────
def get_signals(date_str: str, dry_run: bool = False, verbose: bool = False) -> list:
    """
    Generate handball ML + Totals signals for a given date.

    Reads fixtures from handball_matches (status=NS/scheduled) for date_str,
    looks up team stats from handball_team_stats, applies the Efficiency-Flow
    model, and writes signals passing all 5 gates.

    DATA_PHASE: returns empty list with log message if insufficient data.
    """
    n_matches = count_handball_matches()
    lg_stats  = get_league_stats()

    # ── DATA PHASE guard ─────────────────────────────────────────────────────
    if n_matches < MIN_LEAGUE_MATCHES:
        logger.info(
            f"[HANDBALL] DATA_PHASE — {n_matches}/{MIN_LEAGUE_MATCHES} matches in DB. "
            f"Run: python scrapers/fetch_historical_handball.py --seasons 3"
        )
        return []

    # ── Load today's scheduled handball fixtures ──────────────────────────────
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """SELECT game_id, date, league_id, league_name,
                          home_team, away_team, home_odds, away_odds,
                          total_line, over_odds, under_odds
                   FROM handball_matches
                   WHERE date = ? AND status IN ('NS','SCHEDULED','UPCOMING')""",
                (date_str,)
            ).fetchall()
            fixtures = [dict(r) for r in rows]
    except sqlite3.OperationalError:
        logger.warning("[HANDBALL] handball_matches table not found. Run db_init first.")
        return []

    if not fixtures:
        logger.info(f"[HANDBALL] No scheduled fixtures for {date_str}.")
        return []

    signals  = []
    pod_best = None  # Track highest-edge signal for POD consideration

    for fx in fixtures:
        home_team = fx["home_team"]
        away_team = fx["away_team"]
        game_str  = f"{away_team} @ {home_team}"
        lid       = fx.get("league_id")

        # ── Load team stats ───────────────────────────────────────────────────
        home_stats = get_team_stats(home_team, lid)
        away_stats = get_team_stats(away_team, lid)

        # Fall back to league defaults if team not found
        def stat_or_default(stats, key, default):
            return stats[key] if stats and stats.get(key) is not None else default

        home_elo  = stat_or_default(home_stats, "elo_rating",             ELO_DEFAULT)
        away_elo  = stat_or_default(away_stats, "elo_rating",             ELO_DEFAULT)
        home_poss = stat_or_default(home_stats, "possessions_per_game",   lg_stats["lg_avg_possessions"])
        away_poss = stat_or_default(away_stats, "possessions_per_game",   lg_stats["lg_avg_possessions"])
        home_seff = stat_or_default(home_stats, "shot_efficiency",        lg_stats["lg_avg_shot_eff"])
        away_seff = stat_or_default(away_stats, "shot_efficiency",        lg_stats["lg_avg_shot_eff"])
        home_def  = stat_or_default(home_stats, "def_goals_allowed_per_shot", lg_stats["lg_avg_def_eff"])
        away_def  = stat_or_default(away_stats, "def_goals_allowed_per_shot", lg_stats["lg_avg_def_eff"])
        home_gp   = stat_or_default(home_stats, "games_played",           0)
        away_gp   = stat_or_default(away_stats, "games_played",           0)
        min_gp    = min(home_gp, away_gp)

        has_possession_data = (
            home_stats is not None and home_stats.get("possessions_per_game") and
            away_stats is not None and away_stats.get("possessions_per_game")
        )

        # ── ELO probability ───────────────────────────────────────────────────
        p_home_elo  = elo_win_prob(home_elo, away_elo)
        p_away_elo  = 1 - p_home_elo

        # ── Poisson / xG probability ──────────────────────────────────────────
        # Home scores vs away defense; away scores vs home defense
        lambda_home = calc_adj_xg(home_poss, home_seff, away_def, lg_stats["lg_avg_def_eff"])
        lambda_away = calc_adj_xg(away_poss, away_seff, home_def, lg_stats["lg_avg_def_eff"])

        # Apply HFA to Poisson lambdas (home team gets a slight boost)
        lambda_home *= (1 + HFA_PROB / 2)
        lambda_away *= (1 - HFA_PROB / 4)

        p_home_poisson, p_away_poisson = poisson_win_prob(lambda_home, lambda_away, max_goals=50)

        # ── Blend ELO + Poisson ───────────────────────────────────────────────
        p_home = W_ELO * p_home_elo + W_POISSON * p_home_poisson
        p_away = W_ELO * p_away_elo + W_POISSON * p_away_poisson

        # Normalize to sum to 1.0 (rounding artifacts)
        total = p_home + p_away
        if total > 0:
            p_home /= total
            p_away /= total

        # Gate 3: model agreement check
        elo_poisson_gap_pp = abs(p_home_elo - p_home_poisson) * 100
        gate3_pass = elo_poisson_gap_pp <= 15.0

        # ── Market lines ──────────────────────────────────────────────────────
        home_odds_raw = fx.get("home_odds")
        away_odds_raw = fx.get("away_odds")

        if not home_odds_raw or not away_odds_raw:
            logger.debug(f"  {game_str}: no market odds available — skipping ML signal")
        else:
            # Devig
            p_home_nv, p_away_nv = no_vig_prob(int(home_odds_raw), int(away_odds_raw))

            # Which side has edge?
            for side_label, p_model, p_nv, odds_raw in [
                (f"{home_team} ML", p_home, p_home_nv, home_odds_raw),
                (f"{away_team} ML", p_away, p_away_nv, away_odds_raw),
            ]:
                edge = edge_calc(p_model, p_nv)
                if edge < MIN_EDGE:
                    continue

                # Edge window check
                if edge > MAX_EDGE_TEAM:
                    logger.debug(f"  {side_label}: edge {edge*100:.1f}% > cap {MAX_EDGE_TEAM*100:.0f}% — flagged_high")
                    continue

                # EV
                dec_odds = american_to_decimal(int(odds_raw))
                ev       = ev_calc(dec_odds, p_model)

                # Tier
                edge_pct = edge * 100
                tier_label, units = handball_tier(edge_pct)
                if units == 0:
                    continue

                # Gates
                gates = run_gates(p_home, p_home_nv, edge, n_matches)
                gates["gate3"] = "PASS" if gate3_pass else "FAIL"
                if not gates["passes"]:
                    logger.debug(f"  {side_label}: blocked by {gates['blocked_by']}")
                    continue

                # Confidence
                conf_label, conf_sym = handball_confidence(
                    edge_pct, min_gp, elo_poisson_gap_pp, has_possession_data
                )

                notes = (
                    f"ELO_home={home_elo:.0f}  ELO_away={away_elo:.0f}  "
                    f"λ_home={lambda_home:.1f}  λ_away={lambda_away:.1f}  "
                    f"ELO_p={p_home_elo:.3f}  Poisson_p={p_home_poisson:.3f}  "
                    f"Gap={elo_poisson_gap_pp:.1f}pp  "
                    f"Poss_data={'YES' if has_possession_data else 'NO'}  "
                    f"Conf={conf_label} {conf_sym}  "
                    f"League={fx.get('league_name','?')}"
                )

                signal = {
                    "signal_date": date_str,
                    "game":        game_str,
                    "game_time":   fx.get("game_time", "TBD"),
                    "bet_type":    "ML",
                    "side":        side_label,
                    "market":      "ML",
                    "odds":        int(odds_raw),
                    "model_prob":  round(p_model, 4),
                    "no_vig_prob": round(p_nv, 4),
                    "edge":        round(edge, 4),
                    "ev":          round(ev, 4),
                    "tier":        tier_label,
                    "units":       units,
                    "is_pod":      0,
                    "notes":       notes,
                    "gate1":       gates["gate1"],
                    "gate2":       gates["gate2"],
                    "gate3":       gates["gate3"],
                    "gate4":       gates["gate4"],
                    "gate5":       gates["gate5"],
                    "pick_source": "SPORTSBOOK",
                }
                signals.append(signal)

                # Track best for POD
                if pod_best is None or edge > pod_best["edge"]:
                    pod_best = signal

                if verbose:
                    logger.info(
                        f"  ✓ HANDBALL ML | {side_label} | "
                        f"Edge={edge_pct:.1f}% | EV={ev*100:.1f}% | "
                        f"Tier={tier_label} | {conf_label}"
                    )

        # ── Totals signal ─────────────────────────────────────────────────────
        total_line  = fx.get("total_line") or DEFAULT_TOTAL_LINE
        over_odds   = fx.get("over_odds")
        under_odds  = fx.get("under_odds")
        proj_total  = lambda_home + lambda_away

        if over_odds and under_odds:
            over_dec   = american_to_decimal(int(over_odds))
            under_dec  = american_to_decimal(int(under_odds))
            p_ov_nv    = american_to_prob(int(over_odds))
            p_un_nv    = american_to_prob(int(under_odds))
            total_vig  = p_ov_nv + p_un_nv
            p_ov_nv   /= total_vig
            p_un_nv   /= total_vig

            # P(total > line) via Poisson convolution
            p_over_model = 0.0
            for h in range(100):
                for a in range(100):
                    if h + a > total_line:
                        p_over_model += poisson_pmf(h, lambda_home) * poisson_pmf(a, lambda_away)
            p_under_model = 1 - p_over_model

            for side_label, p_model, p_nv, dec_odds_val, odds_raw_val in [
                (f"O{total_line} Total", p_over_model,  p_ov_nv,  over_dec,  over_odds),
                (f"U{total_line} Total", p_under_model, p_un_nv,  under_dec, under_odds),
            ]:
                edge     = edge_calc(p_model, p_nv)
                if edge < MIN_EDGE or edge > MAX_EDGE_TEAM:
                    continue
                ev       = ev_calc(dec_odds_val, p_model)
                edge_pct = edge * 100
                tier_label, units = handball_tier(edge_pct)
                if units == 0:
                    continue

                gates = run_gates(p_home, p_home_nv if home_odds_raw else 0.5, edge, n_matches)
                gates["gate3"] = "PASS" if gate3_pass else "FAIL"
                if not gates["passes"]:
                    continue

                conf_label, conf_sym = handball_confidence(
                    edge_pct, min_gp, elo_poisson_gap_pp, has_possession_data
                )

                signal = {
                    "signal_date": date_str,
                    "game":        game_str,
                    "game_time":   fx.get("game_time", "TBD"),
                    "bet_type":    "TOTAL",
                    "side":        side_label,
                    "market":      f"Total Goals {total_line}",
                    "odds":        int(odds_raw_val),
                    "model_prob":  round(p_model, 4),
                    "no_vig_prob": round(p_nv, 4),
                    "edge":        round(edge, 4),
                    "ev":          round(ev, 4),
                    "tier":        tier_label,
                    "units":       units,
                    "is_pod":      0,
                    "notes":       (
                        f"Proj_Total={proj_total:.1f}  Line={total_line}  "
                        f"λ_home={lambda_home:.1f}  λ_away={lambda_away:.1f}  "
                        f"Conf={conf_label} {conf_sym}  "
                        f"League={fx.get('league_name','?')}"
                    ),
                    "gate1":       gates["gate1"],
                    "gate2":       gates["gate2"],
                    "gate3":       gates["gate3"],
                    "gate4":       gates["gate4"],
                    "gate5":       gates["gate5"],
                    "pick_source": "SPORTSBOOK",
                }
                signals.append(signal)

    # ── POD selection: top signal by edge (requires HIGH conf) ───────────────
    if pod_best and pod_best["units"] >= 1.5:
        conf_label = pod_best["notes"].split("Conf=")[1].split(" ")[0] if "Conf=" in pod_best.get("notes","") else ""
        if conf_label == "HIGH":
            pod_best["is_pod"] = 1
            logger.info(f"[HANDBALL] POD → {pod_best['side']} | Edge={pod_best['edge']*100:.1f}%")

    # ── Write signals to DB ───────────────────────────────────────────────────
    written = 0
    for sig in signals:
        write_signal(sig, dry_run=dry_run)
        written += 1

    logger.info(
        f"[HANDBALL] {date_str} — {written} signals written "
        f"({'DRY-RUN' if dry_run else 'LIVE'}) | {n_matches} historical matches in DB"
    )
    return signals


# ── Backtest entry point ──────────────────────────────────────────────────────
def run_backtest(season: str = None) -> dict:
    """
    Walk-forward backtest over historical handball_matches.
    Returns summary dict with ROI, CLV%, calibration.
    Cannot be called until DATA_PHASE ends (>= MIN_LEAGUE_MATCHES).
    """
    n = count_handball_matches()
    if n < MIN_LEAGUE_MATCHES:
        return {
            "status": "DATA_PHASE",
            "message": f"Only {n}/{MIN_LEAGUE_MATCHES} matches. Run historical scraper first.",
            "roi": None, "clv": None, "n_bets": 0,
        }
    # Backtest logic: use backtest_harness.py pattern
    # Walk-forward: predict on week N using data from weeks 1..N-1 only
    # 10¢ slippage applied per entry
    # This is a placeholder — full backtest runs via core/backtest_harness.py
    return {"status": "NOT_IMPLEMENTED", "message": "Use core/backtest_harness.py --sport HANDBALL"}


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EyeBlackIQ Handball Efficiency-Flow Model")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"), help="Date YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Compute signals but do not write to DB")
    parser.add_argument("--verbose", action="store_true", help="Log each signal detail")
    parser.add_argument("--backtest", action="store_true", help="Run walk-forward backtest")
    parser.add_argument("--status", action="store_true", help="Show model status and data phase info")
    args = parser.parse_args()

    if args.status:
        n = count_handball_matches()
        lg = get_league_stats()
        print(f"\n{'='*60}")
        print(f"  HANDBALL MODEL STATUS — EyeBlackIQ v{MODEL_VERSION}")
        print(f"{'='*60}")
        print(f"  STATUS       : {STATUS}")
        print(f"  DB Matches   : {n}/{MIN_LEAGUE_MATCHES} required")
        print(f"  Data Phase   : {'YES — awaiting data' if n < MIN_LEAGUE_MATCHES else 'CLEARED'}")
        print(f"  Lg Avg Goals : {lg['lg_avg_goals']:.1f}/team/game")
        print(f"  Lg Avg Poss  : {lg['lg_avg_possessions']:.1f}/team/game")
        print(f"  Lg Shot Eff  : {lg['lg_avg_shot_eff']:.3f} (goals/shot)")
        print(f"  Edge Window  : {MIN_EDGE*100:.0f}%–{MAX_EDGE_TEAM*100:.0f}% (ML/Totals)")
        print(f"  HFA          : +{HFA_PROB*100:.0f}% prob / +{HFA_ELO} ELO pts")
        print(f"\n  To populate data:")
        print(f"    python scrapers/fetch_historical_handball.py --seasons 3")
        print(f"{'='*60}\n")
    elif args.backtest:
        result = run_backtest()
        print(result)
    else:
        sigs = get_signals(args.date, dry_run=args.dry_run, verbose=args.verbose)
        print(f"\n[HANDBALL] {len(sigs)} signals for {args.date}")
        for s in sigs:
            print(f"  {'📌 POD' if s.get('is_pod') else '   '} {s['side']:35s} "
                  f"Edge={s['edge']*100:.1f}%  EV={s['ev']*100:.1f}%  Tier={s['tier']}")
