"""
EyeBlackIQ — pods/cricket/model.py
=====================================
Cricket Team ML + Totals + Player Runs props via the Resource-Value Model.

=== MODEL ARCHITECTURE — RESOURCE-VALUE (RESOURCE-DEPLETION) ===

Cricket is a "Resource-Depletion" sport. A team's innings is constrained by
two depleting resources: BALLS and WICKETS. We model the probability of runs
occurring on a specific ball, conditioned on the remaining resources.

FORMULA HIERARCHY (in order):
  1. ELO Rating         : Head-to-head win probability from historical W/L
  2. Team Par Score      : Expected first-innings score
                          Par = Σ(ball_states) × E[runs_per_ball | b, w]
                          (Simplified: par = avg_sr × effective_overs × venue_z)
  3. Venue Z-Factor      : Standardizes team performance by venue
                          Z = (venue_avg - league_avg) / league_std
                          adj_par = par × venue_z_multiplier
  4. Toss Impact         : +3% win probability adjustment to toss winner
  5. Format Calibration  : T20 vs ODI vs TEST use different distributions
  6. Signal Blend        : 0.55 × ELO_prob + 0.45 × Par_Score_prob

PLAYER PROPS — Run Projections:
  proj_runs = expected_balls_faced × (strike_rate / 100) × survival_prob
  survival_prob = batter_dot_pct_advantage vs specific_bowler_type
  expected_balls_faced = position_usage × (1 - early_wicket_rate)
  T20 middle-order cap: if top3_efficiency > 140 SR, middle-order balls_faced drops

POISSON NOTES:
  - Standard Poisson: Team runs per innings (Normal approximation valid for T20+)
  - Zero-Inflated Poisson (ZIP): Wickets per bowler (0 wickets extremely common)
    ZIP: P(0) = π + (1-π)×e^(-λ)  |  P(k>0) = (1-π) × Poisson(k|λ)

EDGE WINDOW:
  - ML / Totals  : 3% – 20%  (flagged_high above 20%)
  - Player Props : 3% – 30%  (batting props; wider due to thin markets)
  - PODs         : bypass cap

FORMATS COVERED:
  T20 (primary): IPL, ICC T20 WC, Big Bash, The Hundred
  ODI: ICC ODI World Cup, bilateral series (secondary — thinner odds)
  TEST: not modeled (too long-form, different resource structure)

TOSS IMPACT: ~3% win probability shift to toss winner (more significant at neutral/spin venues)

STATUS: DATA_PHASE until cricket_matches table has >= 150 T20 matches

GO-LIVE THRESHOLDS (per EyeBlackIQ spec):
  ROI > 3% · CLV >= 55% · Calibration ±2% · >= 300/500 sample bets
  30-day paper trade before real money

Usage:
  python pods/cricket/model.py --date 2026-03-21
  python pods/cricket/model.py --date 2026-03-21 --format T20 --dry-run
  python pods/cricket/model.py --status
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
SPORT          = "CRICKET"
MODEL_VERSION  = "1.0.0"
STATUS         = "DATA_PHASE"

# Data gates
MIN_MATCHES_T20  = 150    # minimum T20 matches in DB before model is live
MIN_MATCHES_ODI  = 80     # minimum ODI matches

# Blend weights
W_ELO            = 0.55
W_PAR            = 0.45

# ELO
ELO_K_EARLY      = 32
ELO_K_STANDARD   = 20
ELO_DEFAULT      = 1500
HFA_ELO          = 35   # Cricket home-field is meaningful but less than handball

# Format-specific defaults (calibrated to T20 IPL / ICC T20 WC)
FORMAT_DEFAULTS = {
    "T20": {
        "avg_first_innings":   165.0,  # IPL avg first-innings score
        "std_first_innings":    22.0,  # standard deviation
        "avg_strike_rate":     130.0,  # league average T20 SR
        "dot_ball_rate":        0.37,  # fraction of balls that are dots
        "avg_wickets_bowling":   2.1,  # wickets per bowler per innings (ZIP)
        "p_zero_wickets":        0.35, # ZIP: P(bowler takes 0 wickets)
        "total_overs":          20.0,
        "toss_adj":             0.03,
    },
    "ODI": {
        "avg_first_innings":   270.0,
        "std_first_innings":    35.0,
        "avg_strike_rate":      85.0,
        "dot_ball_rate":        0.42,
        "avg_wickets_bowling":   1.8,
        "p_zero_wickets":        0.38,
        "total_overs":          50.0,
        "toss_adj":             0.025,
    },
    "TEST": {
        "avg_first_innings":   330.0,
        "std_first_innings":    65.0,
        "avg_strike_rate":      50.0,
        "dot_ball_rate":        0.55,
        "avg_wickets_bowling":   1.5,
        "p_zero_wickets":        0.40,
        "total_overs":          90.0,
        "toss_adj":             0.05,  # Toss more impactful in Tests
    },
}

# Edge window
MIN_EDGE         = 0.03
MAX_EDGE_TEAM    = 0.20
MAX_EDGE_PROP    = 0.30

# T20 middle-order cap: if top-3 SR > threshold, middle-order usage compressed
T20_TOP_ORDER_SR_THRESHOLD = 140.0
T20_MIDDLEORDER_BALLS_CAP  = 18   # max balls faced for positions 4-7 if openers dominant

# Batting position expected balls faced (T20 baseline, not accounting for top-order SR)
T20_POSITION_BALLS_BASELINE = {
    1: 30, 2: 28, 3: 22, 4: 18, 5: 14, 6: 10, 7: 7, 8: 4, 9: 3, 10: 2, 11: 1
}


# ── Tier system ──────────────────────────────────────────────────────────────
def cricket_tier(edge_pct: float) -> tuple:
    """Returns (tier_label, units) for edge in 0–100 scale."""
    if   edge_pct >= 12: return ("SCREAMER",  2.0)   # T1 — maximum conviction
    elif edge_pct >=  7: return ("WHEELHOUSE", 1.5)   # T2 — solid edge
    elif edge_pct >=  4: return ("MONITOR",    1.0)   # T2 — worth tracking
    elif edge_pct >=  2: return ("SCOUT",      0.5)   # T3 — informational
    else:                return ("NO BALL",    0.0)   # below threshold


# ── Confidence scoring ────────────────────────────────────────────────────────
def cricket_confidence(
    edge_pct: float,
    n_team_matches: int,
    elo_par_gap_pp: float,
    has_venue_data: bool,
    format_str: str = "T20",
) -> tuple:
    """
    Confidence for cricket signal.
      +1: edge >= 12%
      +1: n_team_matches >= 40 (ELO well-calibrated)
      +1: venue data available (Z-Factor reliable)
      +1: model agreement within 5pp
      -1: edge < 4%
      -1: n_team_matches < 20
      -1: model disagreement > 12pp
      -1: format = TEST (most variance, hardest to predict)
    """
    score = 2
    if edge_pct >= 12:         score += 1
    if edge_pct < 4:           score -= 1
    if n_team_matches >= 40:   score += 1
    if n_team_matches < 20:    score -= 1
    if has_venue_data:         score += 1
    if elo_par_gap_pp <= 5:    score += 1
    elif elo_par_gap_pp > 12:  score -= 1
    if format_str == "TEST":   score -= 1   # Tests have highest variance
    score = max(1, min(3, score))
    return {3: ("HIGH", "●●●"), 2: ("MED", "●●○"), 1: ("LOW", "●○○")}[score]


# ── Probability / math helpers ────────────────────────────────────────────────
def american_to_prob(odds: int) -> float:
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def no_vig_prob(home_odds: int, away_odds: int) -> tuple:
    p_h = american_to_prob(home_odds)
    p_a = american_to_prob(away_odds)
    t   = p_h + p_a
    if t == 0:
        return 0.5, 0.5
    return p_h / t, p_a / t


def elo_win_prob(elo_home: float, elo_away: float, hfa_elo: float = HFA_ELO) -> float:
    """ELO win probability for home team."""
    diff = (elo_home + hfa_elo) - elo_away
    return 1 / (1 + 10 ** (-diff / 400))


def american_to_decimal(odds: int) -> float:
    return odds / 100 + 1 if odds > 0 else 100 / abs(odds) + 1


def ev_calc(decimal_odds: float, model_p: float) -> float:
    return (decimal_odds - 1) * model_p - (1 - model_p)


def normal_cdf(x: float) -> float:
    """Standard normal CDF approximation (Abramowitz & Stegun)."""
    if x < -6:
        return 0.0
    if x > 6:
        return 1.0
    b1, b2, b3, b4, b5 = 0.319381530, -0.356563782, 1.781477937, -1.821255978, 1.330274429
    p_coeff = 0.2316419
    t = 1 / (1 + p_coeff * abs(x))
    poly = t * (b1 + t * (b2 + t * (b3 + t * (b4 + t * b5))))
    phi = math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)
    cdf = 1 - phi * poly
    return cdf if x >= 0 else 1 - cdf


# ── Venue Z-Factor ─────────────────────────────────────────────────────────────
def venue_z_factor(
    venue_avg: float,
    league_avg: float,
    league_std: float,
    format_str: str = "T20",
) -> float:
    """
    The "Z-Factor" — venue-adjusted scoring multiplier.

    Formula:
      Z = (venue_avg_score - league_avg_score) / league_std_score
      multiplier = 1.0 + (Z × calibration_coefficient)

    Calibration coefficient = 0.08 (8% per sigma):
      +1σ venue → team scores ~8% more than their average
      -1σ venue → team scores ~8% less than their average

    Caps at ±2σ to prevent extreme adjustments with small venue samples.
    """
    if league_std <= 0:
        return 1.0
    z = (venue_avg - league_avg) / league_std
    z = max(-2.0, min(2.0, z))          # cap at ±2 sigma
    coeff = 0.08                         # 8% per sigma adjustment
    return 1.0 + (z * coeff)


# ── Team Par Score (First Innings) ─────────────────────────────────────────────
def calc_par_score(
    team_avg_score: float,
    venue_avg: float,
    league_avg: float,
    league_std: float,
    opp_bowling_avg: float,
    league_bowling_avg: float,
    format_str: str = "T20",
) -> float:
    """
    Expected first-innings score for a team at a venue vs a specific bowling attack.

    Formula chain:
      1. Base par = team's historical avg score (batting first, this format)
      2. Apply Venue Z-Factor: adj_par = base_par × venue_z_mult
      3. Apply bowling SOS: adj_par × (league_bowling_avg / opp_bowling_avg)
         (if opponent concedes fewer runs → adj_par goes DOWN)

    opp_bowling_avg = runs conceded per over by opponent bowling attack
    league_bowling_avg = league average runs conceded per over
    """
    # Step 1: team base
    base_par = team_avg_score

    # Step 2: venue adjustment
    z_mult = venue_z_factor(venue_avg, league_avg, league_std, format_str)
    venue_adj_par = base_par * z_mult

    # Step 3: opponent bowling quality (SOS)
    if opp_bowling_avg > 0 and league_bowling_avg > 0:
        bowling_sos = league_bowling_avg / opp_bowling_avg
        bowling_sos = max(0.65, min(1.35, bowling_sos))  # cap at ±35%
    else:
        bowling_sos = 1.0

    final_par = venue_adj_par * bowling_sos
    return max(50.0, final_par)    # floor at 50 (degenerate edge case)


# ── Par-score win probability ──────────────────────────────────────────────────
def par_score_win_prob(
    par_home: float,
    par_away: float,
    std_home: float,
    std_away: float,
    toss_adj: float = 0.0,
    home_bats_first: bool = True,
) -> tuple:
    """
    Win probability derived from par scores.

    Model: scores approximately Normal (central limit theorem — ~120 balls in T20).
    P(home > away) = P(X_home - X_away > 0)
      where diff ~ Normal(par_home - par_away, sqrt(std_home² + std_away²))

    toss_adj: probability boost to toss winner (default 0.03 = 3%)
    home_bats_first: if True, home team's par is their 1st innings score.

    Returns (p_home_wins, p_away_wins) as a 2-way market.
    """
    diff_mean = par_home - par_away
    diff_std  = math.sqrt(std_home ** 2 + std_away ** 2)

    if diff_std <= 0:
        p_home_raw = 0.5
    else:
        z = diff_mean / diff_std
        p_home_raw = normal_cdf(z)

    # Apply toss adjustment
    p_home = min(0.99, max(0.01, p_home_raw + toss_adj))
    p_away = 1 - p_home
    return p_home, p_away


# ── Zero-Inflated Poisson (for wickets) ───────────────────────────────────────
def zip_pmf(k: int, lam: float, pi: float) -> float:
    """
    ZIP PMF: P(K = k | λ, π)
      P(0)   = π + (1-π) × e^(-λ)
      P(k>0) = (1-π) × Poisson(k|λ)

    π = probability of structural zero (bowler simply doesn't take wickets this match)
    λ = expected wickets given that bowler IS in a wicket-taking state
    """
    if k == 0:
        return pi + (1 - pi) * math.exp(-lam)
    # Poisson PMF for k > 0
    log_p = k * math.log(lam) - lam - sum(math.log(i) for i in range(1, k + 1))
    return (1 - pi) * math.exp(log_p)


def zip_cdf(k_max: int, lam: float, pi: float) -> float:
    """P(K <= k_max) for ZIP distribution."""
    return sum(zip_pmf(k, lam, pi) for k in range(k_max + 1))


# ── Player prop: survival probability ─────────────────────────────────────────
def batter_survival_prob(
    batter_avg_wicket_rate: float,
    bowler_wicket_rate: float,
    league_avg_wicket_rate: float = 0.045,
) -> float:
    """
    Survival probability per ball: probability batter is NOT dismissed.

    Formula: survival = 1 - (batter_vulnerability × bowler_threat / lg_avg_rate)
      batter_vulnerability = batter's historical wicket loss rate per ball
      bowler_threat        = bowler's historical wicket rate per ball
      league_avg           = normalization factor

    Combines batter efficiency (how rarely they get out) vs bowler SOS.
    """
    if league_avg_wicket_rate <= 0:
        return 1 - batter_avg_wicket_rate
    combined_rate = (batter_avg_wicket_rate * bowler_wicket_rate) / league_avg_wicket_rate
    return max(0.85, min(0.99, 1 - combined_rate))


def calc_player_run_projection(
    batting_position: int,
    team_avg_batting_sr: float,
    top3_strike_rate: float,
    survival_prob_per_ball: float,
    format_str: str = "T20",
) -> float:
    """
    Player prop run projection.

    Formula:
      expected_balls_faced = position_baseline × (1 - early_wicket_compression)
      T20 middle-order cap: if top3 SR > 140, middle-order balls_faced compressed
      proj_runs = expected_balls_faced × (team_avg_batting_sr / 100) × survival_prob

    Critical T20 note (from spec): if openers are highly efficient (SR > 140),
    the middle-order usage drops significantly — they simply face fewer balls.
    """
    if format_str != "T20":
        # ODI / TEST: simpler approximation
        baseline_balls = T20_POSITION_BALLS_BASELINE.get(batting_position, 10) * 2.5
    else:
        baseline_balls = T20_POSITION_BALLS_BASELINE.get(batting_position, 10)

    # T20 middle-order compression
    if format_str == "T20" and batting_position >= 4:
        if top3_strike_rate > T20_TOP_ORDER_SR_THRESHOLD:
            # Top order dominates → fewer balls left for middle order
            compression = (top3_strike_rate - T20_TOP_ORDER_SR_THRESHOLD) / 40.0
            compression = min(0.45, compression)  # max 45% compression
            baseline_balls = baseline_balls * (1 - compression)
            baseline_balls = max(baseline_balls, T20_MIDDLEORDER_BALLS_CAP * 0.5)

    # Expected runs = balls × SR × survival
    proj_runs = baseline_balls * (team_avg_batting_sr / 100) * survival_prob_per_ball
    return max(0.0, proj_runs)


def adjust_batter_usage(batters: list, inactive_id: str) -> list:
    """
    Redistribute batting usage (expected balls faced) from an inactive batter.

    Usage Vacuum model (from EyeBlackIQ Cricket spec):
      When a key batter is out, higher-position batters absorb more of the load.
      Redistribution is proportional to existing position-based usage.
    """
    inactive = next((b for b in batters if b["id"] == inactive_id), None)
    if not inactive:
        return [b for b in batters if b.get("is_active")]

    vacuum = inactive.get("base_balls_faced", 0)
    active = [b for b in batters if b.get("is_active") and b["id"] != inactive_id]

    if not active:
        return active

    total_active_usage = sum(b.get("base_balls_faced", 0) for b in active)

    for b in active:
        base = b.get("base_balls_faced", 0)
        if total_active_usage > 0:
            b["adj_balls_faced"]  = base + (base / total_active_usage) * vacuum
        else:
            b["adj_balls_faced"]  = base
        sr   = b.get("strike_rate", 100.0)
        surv = b.get("survival_prob", 0.96)
        b["projected_runs"] = b["adj_balls_faced"] * (sr / 100) * surv

    return active


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(TGT_DB)
    conn.row_factory = sqlite3.Row
    return conn


def count_cricket_matches(format_str: str = "T20") -> int:
    """Returns total T20 matches in DB. 0 if table doesn't exist."""
    try:
        with get_conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM cricket_matches WHERE format = ?", (format_str,)
            ).fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def get_team_stats(team: str, fmt: str = "T20", league_id: int = None) -> Optional[dict]:
    """Load team ELO + batting/bowling stats from cricket_team_stats."""
    try:
        with get_conn() as conn:
            q = """SELECT team_name, games_played, elo_rating,
                          avg_score_batting_first, avg_score_batting_second,
                          avg_runs_conceded, win_pct_batting_first,
                          win_pct_batting_second
                   FROM cricket_team_stats
                   WHERE team_name = ? AND format = ?"""
            params = [team, fmt]
            if league_id:
                q += " AND league_id = ?"
                params.append(league_id)
            q += " ORDER BY season DESC LIMIT 1"
            row = conn.execute(q, params).fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError:
        return None


def get_venue_stats(venue: str, fmt: str = "T20") -> Optional[dict]:
    """Load venue scoring stats from cricket_venue_stats."""
    try:
        with get_conn() as conn:
            row = conn.execute(
                """SELECT venue, avg_first_innings_score, std_first_innings_score,
                          avg_total_runs, matches_played
                   FROM cricket_venue_stats WHERE venue = ?""",
                (venue,)
            ).fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError:
        return None


def get_league_venue_stats(fmt: str = "T20") -> dict:
    """League-average venue stats for Z-Factor normalization."""
    fmtd = FORMAT_DEFAULTS.get(fmt, FORMAT_DEFAULTS["T20"])
    defaults = {
        "league_avg":      fmtd["avg_first_innings"],
        "league_std":      fmtd["std_first_innings"],
        "league_bowl_avg": fmtd["avg_first_innings"] / fmtd["total_overs"],
    }
    try:
        with get_conn() as conn:
            row = conn.execute(
                """SELECT AVG(avg_first_innings_score), AVG(std_first_innings_score)
                   FROM cricket_venue_stats"""
            ).fetchone()
        if row and row[0]:
            defaults["league_avg"] = row[0] or defaults["league_avg"]
            defaults["league_std"] = row[1] or defaults["league_std"]
    except sqlite3.OperationalError:
        pass
    return defaults


def write_signal(signal: dict, dry_run: bool = False) -> Optional[int]:
    """Write cricket signal to signals table."""
    if dry_run:
        logger.info(f"[DRY-RUN] {signal.get('side')} | edge={signal.get('edge'):.3f}")
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
def run_gates(edge: float, n_matches: int, elo_par_gap_pp: float, etl_hours_old: float = 0.0) -> dict:
    """
    5-gate filter for cricket signals.

    Gate 1 (Data quality): n_matches >= minimum threshold
    Gate 2 (Edge):         edge >= 3%
    Gate 3 (Model agree):  ELO and Par-score probabilities within 15pp
    Gate 4 (Line move):    placeholder (live feed pending)
    Gate 5 (ETL freshness): data < 4 hours old
    """
    gates = {}
    if n_matches >= 100:
        gates["gate1"] = "GREEN"
    elif n_matches >= 40:
        gates["gate1"] = "YELLOW"
    else:
        gates["gate1"] = "RED"

    gates["gate2"] = "PASS" if edge >= MIN_EDGE else "FAIL"
    gates["gate3"] = "PASS" if elo_par_gap_pp <= 15.0 else "FAIL"
    gates["gate4"] = "PASS"
    gates["gate5"] = "PASS" if etl_hours_old < 4 else "FAIL"

    blocked_by = None
    if gates["gate1"] == "RED":     blocked_by = "gate1_data"
    elif gates["gate2"] == "FAIL":  blocked_by = "gate2_edge"
    elif gates["gate3"] == "FAIL":  blocked_by = "gate3_model"
    elif gates["gate4"] == "FAIL":  blocked_by = "gate4_line"
    elif gates["gate5"] == "FAIL":  blocked_by = "gate5_etl"

    gates["passes"]     = blocked_by is None
    gates["blocked_by"] = blocked_by
    return gates


# ── Main model function ───────────────────────────────────────────────────────
def get_signals(
    date_str: str,
    format_str: str = "T20",
    dry_run: bool = False,
    verbose: bool = False,
) -> list:
    """
    Generate cricket ML + Totals signals for a given date and format.

    Flow:
      1. Check DATA_PHASE gate
      2. Load today's fixtures from cricket_matches
      3. For each fixture:
         a. Load team ELO from cricket_team_stats
         b. Load venue stats from cricket_venue_stats
         c. Compute Par Score for each team (venue-adjusted, SOS-adjusted)
         d. Compute win probability (ELO + Par blend)
         e. Apply toss adjustment if toss known
         f. Run 5-gate filter
         g. Write signals that pass
    """
    n_matches = count_cricket_matches(format_str)
    min_req   = MIN_MATCHES_T20 if format_str == "T20" else MIN_MATCHES_ODI
    fmtd      = FORMAT_DEFAULTS.get(format_str, FORMAT_DEFAULTS["T20"])
    lg_vs     = get_league_venue_stats(format_str)

    # ── DATA_PHASE guard ──────────────────────────────────────────────────────
    if n_matches < min_req:
        logger.info(
            f"[CRICKET/{format_str}] DATA_PHASE — {n_matches}/{min_req} matches. "
            f"Run: python scrapers/fetch_historical_cricket.py --format {format_str}"
        )
        return []

    # ── Load today's fixtures ─────────────────────────────────────────────────
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """SELECT fixture_id, date, league_id, league_name, format,
                          home_team, away_team, venue,
                          toss_winner, toss_decision,
                          home_odds, away_odds,
                          total_line, over_odds, under_odds
                   FROM cricket_matches
                   WHERE date = ? AND format = ? AND status IN ('NS','SCHEDULED','UPCOMING')""",
                (date_str, format_str)
            ).fetchall()
            fixtures = [dict(r) for r in rows]
    except sqlite3.OperationalError:
        logger.warning("[CRICKET] cricket_matches table not found. Run db_init first.")
        return []

    if not fixtures:
        logger.info(f"[CRICKET/{format_str}] No scheduled fixtures for {date_str}.")
        return []

    signals  = []
    pod_best = None

    for fx in fixtures:
        home_team = fx["home_team"]
        away_team = fx["away_team"]
        venue     = fx.get("venue", "")
        game_str  = f"{away_team} @ {home_team}"
        lid       = fx.get("league_id")

        # ── Team stats ────────────────────────────────────────────────────────
        home_stats  = get_team_stats(home_team, format_str, lid)
        away_stats  = get_team_stats(away_team, format_str, lid)
        venue_stats = get_venue_stats(venue, format_str)

        def s(stats, key, default):
            return stats[key] if stats and stats.get(key) is not None else default

        home_elo = s(home_stats, "elo_rating",                ELO_DEFAULT)
        away_elo = s(away_stats, "elo_rating",                ELO_DEFAULT)
        home_gp  = s(home_stats, "games_played",              0)
        away_gp  = s(away_stats, "games_played",              0)
        min_gp   = min(home_gp, away_gp)

        # Home par score components
        home_avg_bat1  = s(home_stats, "avg_score_batting_first",  fmtd["avg_first_innings"])
        away_avg_bat1  = s(away_stats, "avg_score_batting_first",  fmtd["avg_first_innings"])
        home_bowl_avg  = s(home_stats, "avg_runs_conceded",         fmtd["avg_first_innings"])
        away_bowl_avg  = s(away_stats, "avg_runs_conceded",         fmtd["avg_first_innings"])

        # Venue stats
        v_avg  = s(venue_stats, "avg_first_innings_score", lg_vs["league_avg"])
        v_std  = s(venue_stats, "std_first_innings_score", lg_vs["league_std"])
        has_venue_data = venue_stats is not None and venue_stats.get("matches_played", 0) >= 10

        # League bowling average (runs per over)
        lg_bowl_avg = lg_vs["league_bowl_avg"]

        # ── Par Score (Venue + SOS adjusted) ─────────────────────────────────
        par_home = calc_par_score(
            home_avg_bat1, v_avg, lg_vs["league_avg"], lg_vs["league_std"],
            away_bowl_avg, lg_bowl_avg, format_str
        )
        par_away = calc_par_score(
            away_avg_bat1, v_avg, lg_vs["league_avg"], lg_vs["league_std"],
            home_bowl_avg, lg_bowl_avg, format_str
        )

        std_home = fmtd["std_first_innings"] * 0.9   # teams have slightly less variance than league avg
        std_away = fmtd["std_first_innings"] * 0.9

        # ── Toss adjustment ───────────────────────────────────────────────────
        toss_winner = fx.get("toss_winner", "")
        toss_adj_home = 0.0
        if toss_winner:
            if toss_winner == home_team:
                toss_adj_home = fmtd["toss_adj"]
            elif toss_winner == away_team:
                toss_adj_home = -fmtd["toss_adj"]

        # ── ELO probability ───────────────────────────────────────────────────
        p_home_elo  = elo_win_prob(home_elo, away_elo)
        p_away_elo  = 1 - p_home_elo

        # ── Par Score probability ─────────────────────────────────────────────
        p_home_par, p_away_par = par_score_win_prob(
            par_home, par_away, std_home, std_away, toss_adj_home
        )

        # ── Blend ─────────────────────────────────────────────────────────────
        p_home = W_ELO * p_home_elo + W_PAR * p_home_par
        p_away = W_ELO * p_away_elo + W_PAR * p_away_par
        total  = p_home + p_away
        if total > 0:
            p_home /= total
            p_away /= total

        # Gate 3: ELO vs Par agreement
        elo_par_gap_pp = abs(p_home_elo - p_home_par) * 100

        # ── Market lines — ML ─────────────────────────────────────────────────
        home_odds_raw = fx.get("home_odds")
        away_odds_raw = fx.get("away_odds")

        if home_odds_raw and away_odds_raw:
            p_home_nv, p_away_nv = no_vig_prob(int(home_odds_raw), int(away_odds_raw))

            for side_label, p_model, p_nv, odds_raw in [
                (f"{home_team} ML", p_home, p_home_nv, home_odds_raw),
                (f"{away_team} ML", p_away, p_away_nv, away_odds_raw),
            ]:
                edge = p_model - p_nv
                if edge < MIN_EDGE or edge > MAX_EDGE_TEAM:
                    continue

                dec_odds    = american_to_decimal(int(odds_raw))
                ev          = ev_calc(dec_odds, p_model)
                edge_pct    = edge * 100
                tier_label, units = cricket_tier(edge_pct)
                if units == 0:
                    continue

                gates = run_gates(edge, n_matches, elo_par_gap_pp)
                if not gates["passes"]:
                    logger.debug(f"  {side_label}: blocked by {gates['blocked_by']}")
                    continue

                conf_label, conf_sym = cricket_confidence(
                    edge_pct, min_gp, elo_par_gap_pp, has_venue_data, format_str
                )

                notes = (
                    f"ELO_home={home_elo:.0f}  ELO_away={away_elo:.0f}  "
                    f"Par_home={par_home:.1f}  Par_away={par_away:.1f}  "
                    f"VenueZ={'YES' if has_venue_data else 'DEFAULT'}  "
                    f"Toss_adj={toss_adj_home:+.2f}  "
                    f"ELO_p={p_home_elo:.3f}  Par_p={p_home_par:.3f}  "
                    f"Gap={elo_par_gap_pp:.1f}pp  "
                    f"Format={format_str}  Conf={conf_label} {conf_sym}  "
                    f"Venue={venue or '?'}"
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
                if pod_best is None or edge > pod_best["edge"]:
                    pod_best = signal

                if verbose:
                    logger.info(
                        f"  ✓ CRICKET {format_str} ML | {side_label} | "
                        f"Edge={edge_pct:.1f}% | Par={par_home:.0f}v{par_away:.0f} | "
                        f"Tier={tier_label}"
                    )

        # ── Totals signal ─────────────────────────────────────────────────────
        total_line = fx.get("total_line")
        over_odds  = fx.get("over_odds")
        under_odds = fx.get("under_odds")
        proj_total = par_home + par_away  # total runs projected (both innings batting first)

        if total_line and over_odds and under_odds:
            p_ov_nv = american_to_prob(int(over_odds))
            p_un_nv = american_to_prob(int(under_odds))
            tv      = p_ov_nv + p_un_nv
            p_ov_nv /= tv
            p_un_nv /= tv

            # P(total > line): combined both innings via Normal approximation
            combined_mean = proj_total
            combined_std  = math.sqrt(std_home**2 + std_away**2) * 1.2  # cross-innings correlation
            z_over = (total_line - combined_mean) / (combined_std if combined_std > 0 else 1)
            p_over_model  = 1 - normal_cdf(z_over)
            p_under_model = 1 - p_over_model

            for side_label, p_model, p_nv, odds_raw in [
                (f"O{total_line} Runs", p_over_model,  p_ov_nv, over_odds),
                (f"U{total_line} Runs", p_under_model, p_un_nv, under_odds),
            ]:
                edge = p_model - p_nv
                if edge < MIN_EDGE or edge > MAX_EDGE_TEAM:
                    continue
                dec_odds    = american_to_decimal(int(odds_raw))
                ev          = ev_calc(dec_odds, p_model)
                edge_pct    = edge * 100
                tier_label, units = cricket_tier(edge_pct)
                if units == 0:
                    continue

                gates = run_gates(edge, n_matches, elo_par_gap_pp)
                if not gates["passes"]:
                    continue

                conf_label, conf_sym = cricket_confidence(
                    edge_pct, min_gp, elo_par_gap_pp, has_venue_data, format_str
                )
                signal = {
                    "signal_date": date_str,
                    "game":        game_str,
                    "game_time":   fx.get("game_time", "TBD"),
                    "bet_type":    "TOTAL",
                    "side":        side_label,
                    "market":      f"Total Runs {total_line}",
                    "odds":        int(odds_raw),
                    "model_prob":  round(p_model, 4),
                    "no_vig_prob": round(p_nv, 4),
                    "edge":        round(edge, 4),
                    "ev":          round(ev, 4),
                    "tier":        tier_label,
                    "units":       units,
                    "is_pod":      0,
                    "notes":       (
                        f"Proj_Total={proj_total:.0f}  Line={total_line}  "
                        f"Par_home={par_home:.0f}  Par_away={par_away:.0f}  "
                        f"Format={format_str}  Conf={conf_label} {conf_sym}"
                    ),
                    "gate1":       gates["gate1"],
                    "gate2":       gates["gate2"],
                    "gate3":       gates["gate3"],
                    "gate4":       gates["gate4"],
                    "gate5":       gates["gate5"],
                    "pick_source": "SPORTSBOOK",
                }
                signals.append(signal)

    # ── POD selection ─────────────────────────────────────────────────────────
    if pod_best and pod_best["units"] >= 1.5:
        conf_lbl = pod_best["notes"].split("Conf=")[1].split(" ")[0] if "Conf=" in pod_best.get("notes","") else ""
        if conf_lbl == "HIGH":
            pod_best["is_pod"] = 1
            logger.info(f"[CRICKET] POD → {pod_best['side']} | Edge={pod_best['edge']*100:.1f}%")

    # ── Write signals ─────────────────────────────────────────────────────────
    written = 0
    for sig in signals:
        write_signal(sig, dry_run=dry_run)
        written += 1

    logger.info(
        f"[CRICKET/{format_str}] {date_str} — {written} signals written "
        f"({'DRY-RUN' if dry_run else 'LIVE'}) | {n_matches} historical matches in DB"
    )
    return signals


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EyeBlackIQ Cricket Resource-Value Model")
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"), help="Date YYYY-MM-DD")
    parser.add_argument("--format",  default="T20", choices=["T20","ODI","TEST"], help="Match format")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--status",  action="store_true", help="Show model status")
    args = parser.parse_args()

    if args.status:
        for fmt in ["T20", "ODI"]:
            n = count_cricket_matches(fmt)
            mn = MIN_MATCHES_T20 if fmt == "T20" else MIN_MATCHES_ODI
            fmtd = FORMAT_DEFAULTS[fmt]
            print(f"\n{'='*60}")
            print(f"  CRICKET [{fmt}] — EyeBlackIQ v{MODEL_VERSION}")
            print(f"{'='*60}")
            print(f"  DB Matches   : {n}/{mn} required")
            print(f"  Data Phase   : {'YES — awaiting data' if n < mn else 'CLEARED'}")
            print(f"  Avg 1st Inn  : {fmtd['avg_first_innings']:.0f} runs")
            print(f"  Toss Adj     : +{fmtd['toss_adj']*100:.1f}% to winner")
            print(f"  ZIP π (wkts) : {fmtd['p_zero_wickets']:.2f}")
            print(f"  Edge Window  : {MIN_EDGE*100:.0f}%–{MAX_EDGE_TEAM*100:.0f}% (ML)")
            print(f"\n  To populate data (T20 via Cricsheet):")
            print(f"    python scrapers/fetch_historical_cricket.py --format T20")
    else:
        sigs = get_signals(args.date, format_str=args.format, dry_run=args.dry_run, verbose=args.verbose)
        print(f"\n[CRICKET/{args.format}] {len(sigs)} signals for {args.date}")
        for s in sigs:
            print(f"  {'📌 POD' if s.get('is_pod') else '   '} {s['side']:35s} "
                  f"Edge={s['edge']*100:.1f}%  EV={s['ev']*100:.1f}%  Tier={s['tier']}")
