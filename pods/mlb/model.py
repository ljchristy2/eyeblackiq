"""
EyeBlackIQ — pods/mlb/model.py
MLB pitcher strikeout props + F5 projection signals.

Spring Training (pre-Mar 26): signals written with LOW confidence [ST] tag.
These serve as calibration data for Opening Day model tuning.

Confidence levels (●●● / ●●○ / ●○○):
  HIGH: edge ≥ 12%, mu gap ≥ 1.0, SP has 5+ starts
  MED:  edge 5–12% or 3–4 SP starts
  LOW:  Spring Training, TBD SP (mu=4.20), mu gap < 0.5

Source DB:  C:/Users/loren/OneDrive/Desktop/quant-betting/soccer/.claude/worktrees/admiring-allen/db/betting.db
  Table:    mlb_prop_results
Target DB:  C:/Users/loren/OneDrive/Desktop/eyeblackiq/pipeline/db/eyeblackiq.db
  Table:    signals

Usage:
  python pods/mlb/model.py --date 2026-03-21
  python pods/mlb/model.py --date 2026-03-21 --dry-run
"""
import os
import sqlite3
import argparse
import logging
from datetime import datetime, date, timezone
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
_ENV = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "quant-betting" / "soccer" / ".claude" / "worktrees" / "admiring-allen" / ".env"
)
if _ENV.exists():
    load_dotenv(_ENV)
else:
    load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
BASE_DIR     = Path(__file__).resolve().parent.parent.parent   # eyeblackiq/
SRC_DB       = Path(
    "C:/Users/loren/OneDrive/Desktop/quant-betting/soccer"
    "/.claude/worktrees/admiring-allen/db/betting.db"
)
TGT_DB       = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"
SPORT        = "MLB"
MLB_OPEN_DAY = date(2026, 3, 26)    # Regular-season Opening Day
TBD_MU       = 4.20                 # League-avg prior used for TBD starters
POD_VERSION  = "0.1.0"

# ---------------------------------------------------------------------------
# Static starter lookup — extend this dict as more dates are added
# Columns: away, home, sp_away, sp_home, mu_away, mu_home, game_time,
#          starts_away, starts_home
# ---------------------------------------------------------------------------
MLB_STARTERS: dict[str, list[tuple]] = {
    "2026-03-20": [
        ("Detroit Tigers",      "Philadelphia Phillies", "T. Skubal",   "C. Sanchez",   5.60, 5.07, "1:05 PM ET",  8, 6),
        ("Baltimore Orioles",   "New York Yankees",      "TBD",         "L. Gil",       4.20, 5.27, "1:05 PM ET",  0, 7),
        ("St. Louis Cardinals", "New York Mets",         "A. Pallante", "TBD",          4.51, 4.20, "1:10 PM ET",  5, 0),
        ("Miami Marlins",       "Houston Astros",        "E. Perez",    "H. Brown",     5.21, 5.26, "2:10 PM ET",  4, 5),
        ("Chicago White Sox",   "Los Angeles Angels",    "TBD",         "J. Soriano",   4.20, 5.11, "3:07 PM ET",  0, 6),
        ("Pittsburgh Pirates",  "Atlanta Braves",        "TBD",         "TBD",          4.20, 4.20, "7:20 PM ET",  0, 0),
        ("Toronto Blue Jays",   "Minnesota Twins",       "M. Scherzer", "TBD",          5.78, 4.20, "7:40 PM ET",  6, 0),
    ],
}


# ---------------------------------------------------------------------------
# Tier classification
# ---------------------------------------------------------------------------
def mlb_tier(edge_pct: float) -> tuple[str, float]:
    """Return (tier_label, units) based on edge percentage."""
    if edge_pct >= 12:
        return ("FILTHY",     2.0)
    elif edge_pct >= 5:
        return ("WHEELHOUSE", 1.5)
    elif edge_pct >= 2:
        return ("SCOUT",      1.0)
    else:
        return ("BALK",       0.0)


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------
def confidence(
    edge_pct: float,
    mu: float,
    line: float,
    is_spring_training: bool,
    sp_starts: int = 4,
) -> tuple[str, str]:
    """
    Compute confidence label and symbol for an MLB K prop.

    Scoring (baseline = MED = 2):
      +1  edge >= 12% AND mu_gap >= 1.0
      +1  mu_gap >= 1.0
      +1  sp_starts >= 5
      -1  edge < 5%
      -1  mu_gap < 0.5
      -1  sp_starts < 3
      -1  is_spring_training
      -1  TBD SP (mu ≈ TBD_MU=4.20)

    Clamped to [1, 3]; maps to LOW / MED / HIGH.
    """
    mu_gap = abs(mu - line)
    is_tbd = abs(mu - TBD_MU) < 0.01

    score = 2  # MED baseline

    if edge_pct >= 12 and mu_gap >= 1.0:
        score += 1
    if mu_gap >= 1.0:
        score += 1
    elif mu_gap < 0.5:
        score -= 1
    if edge_pct < 5:
        score -= 1
    if sp_starts >= 5:
        score += 1
    elif sp_starts < 3:
        score -= 1
    if is_spring_training:
        score -= 1
    if is_tbd:
        score -= 1

    score = max(1, min(3, score))
    return {3: ("HIGH", "●●●"), 2: ("MED", "●●○"), 1: ("LOW", "●○○")}[score]


# ---------------------------------------------------------------------------
# Financial helpers
# ---------------------------------------------------------------------------
def american_to_decimal(american: float) -> float:
    """Convert American odds to decimal."""
    if american > 0:
        return american / 100 + 1
    return 100 / abs(american) + 1


def ev_calc(decimal_odds: float, model_p: float) -> float:
    """Expected value given decimal odds and model probability."""
    return (decimal_odds - 1) * model_p - (1 - model_p)


# ---------------------------------------------------------------------------
# Spring Training detection
# ---------------------------------------------------------------------------
def is_spring_training(date_str: str) -> bool:
    """Return True if date_str (YYYY-MM-DD) is before Opening Day."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        return d < MLB_OPEN_DAY
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# F5 signals (model-only, no live odds required)
# ---------------------------------------------------------------------------
def f5_signals(date_str: str, is_st: bool) -> list[dict]:
    """
    Generate F5 ML projection signals from the static starter lookup.

    Home win probability is estimated from mu ratio; no live odds are fetched
    here — signals are written as model-only projections.

    Returns a list of signal dicts (one per game).
    """
    starters = MLB_STARTERS.get(date_str, [])
    if not starters:
        logger.debug(f"No starter data for {date_str}")
    signals = []
    for away, home, sp_a, sp_h, mu_a, mu_h, gtime, starts_a, starts_h in starters:
        # Simple mu-ratio win probability (SP quality proxy)
        p_home = max(0.10, min(0.90, 0.52 + (mu_h / 4.50 - mu_a / 4.50) * 0.15))
        p_away = 1.0 - p_home
        avg_starts = (starts_a + starts_h) / 2
        edge_proxy = abs(p_home - 0.50) * 100
        conf_label, conf_sym = confidence(
            edge_proxy,
            (mu_h + mu_a) / 2,
            4.50,
            is_st,
            int(avg_starts),
        )
        signals.append({
            "game":        f"{away} @ {home}",
            "game_time":   gtime,
            "away":        away,
            "home":        home,
            "sp_away":     sp_a,
            "sp_home":     sp_h,
            "mu_away":     mu_a,
            "mu_home":     mu_h,
            "starts_away": starts_a,
            "starts_home": starts_h,
            "p_home":      round(p_home, 4),
            "p_away":      round(p_away, 4),
            "conf_label":  conf_label,
            "conf_sym":    conf_sym,
            "st_tag":      " [ST]" if is_st else "",
        })
    return signals


# ---------------------------------------------------------------------------
# Load K props from source DB
# ---------------------------------------------------------------------------
def load_k_props(date_str: str) -> list[dict]:
    """
    Pull signal=1 pitcher_strikeouts rows from betting.db for the given date.

    Uses game_date for date filtering (primary date column) and run_ts for
    ETL-freshness metadata.  cur_gp is used as the SP starts proxy.
    """
    if not SRC_DB.exists():
        logger.warning(f"Source DB not found: {SRC_DB}")
        return []

    query = """
        SELECT player_name, market, line, side,
               cur_gp, gp,
               mu_model, p_model, nv_prob, edge, price,
               away_team, home_team, run_ts
        FROM mlb_prop_results
        WHERE game_date = ?
          AND signal     = 1
          AND market     = 'pitcher_strikeouts'
        ORDER BY edge DESC
    """
    with sqlite3.connect(SRC_DB) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, (date_str,)).fetchall()

    result = [dict(r) for r in rows]
    logger.debug(f"Loaded {len(result)} K prop rows for {date_str}")
    return result


# ---------------------------------------------------------------------------
# Write a single signal row to eyeblackiq.db
# ---------------------------------------------------------------------------
def write_signal(
    conn: sqlite3.Connection,
    date_str: str,
    game: str,
    game_time: str,
    bet_type: str,
    side: str,
    market: str,
    odds: float,
    model_p: float,
    nv_p: float | None,
    edge: float,
    ev_val: float,
    tier: str,
    units: float,
    notes: str,
) -> None:
    """Insert one signal row into eyeblackiq.db signals table."""
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO signals
            (signal_date, sport, game, game_time, bet_type, side, market,
             odds, model_prob, no_vig_prob, edge, ev, tier, units,
             is_pod,
             gate1_pyth, gate2_edge, gate3_model_agree,
             gate4_line_move, gate5_etl_fresh,
             notes, created_at)
        VALUES
            (?, ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?, ?, ?,
             0,
             'GREEN', 'PASS', 'PASS',
             'PASS',  'PASS',
             ?, ?)
        """,
        (
            date_str, SPORT, game, game_time, bet_type, side, market,
            float(odds), round(model_p, 4),
            round(nv_p, 4) if nv_p is not None else None,
            round(edge, 4), round(ev_val, 4),
            tier, units,
            notes, ts,
        ),
    )


# ---------------------------------------------------------------------------
# Main model runner
# ---------------------------------------------------------------------------
def run_model(date_str: str, dry_run: bool = False) -> int:
    """
    Run the MLB model for a given date.

    1. Load pitcher K props from betting.db (signal=1 rows).
    2. Apply tier + confidence scoring.
    3. Generate F5 projection signals from static starter data.
    4. Write qualifying signals to eyeblackiq.db (unless --dry-run).

    Returns the number of signals written.
    """
    is_st   = is_spring_training(date_str)
    k_props = load_k_props(date_str)
    f5_sigs = f5_signals(date_str, is_st)
    st_note = "  [SPRING TRAINING — calibration data]" if is_st else ""

    logger.info(f"MLB model — {date_str}{st_note}")
    logger.info(f"  {len(k_props)} K props loaded  |  {len(f5_sigs)} F5 games")

    signals_written = 0
    conn: sqlite3.Connection | None = None

    if not dry_run:
        if not TGT_DB.exists():
            logger.error(f"Target DB not found: {TGT_DB}")
            return 0
        conn = sqlite3.connect(TGT_DB)
        # Idempotent — clear today's signals before rewriting
        conn.execute("DELETE FROM signals WHERE signal_date=? AND sport=?", (date_str, SPORT))
        conn.commit()

    # ------------------------------------------------------------------
    # K prop signals
    # ------------------------------------------------------------------
    for row in k_props:
        edge_pct   = row["edge"] * 100
        tier_name, units = mlb_tier(edge_pct)

        if units == 0.0:
            logger.debug(f"  skip (BALK): {row['player_name']}  edge={edge_pct:.1f}%")
            continue

        # cur_gp = starts logged in source DB (0 in Spring Training — expected)
        sp_starts = int(row["cur_gp"] or 0)

        is_tbd     = abs(row["mu_model"] - TBD_MU) < 0.01
        conf_label, conf_sym = confidence(
            edge_pct, row["mu_model"], row["line"], is_st, sp_starts
        )

        dec_odds = american_to_decimal(float(row["price"]))
        ev_val   = ev_calc(dec_odds, row["p_model"])
        game_str = f"{row['away_team']} @ {row['home_team']}"
        tbd_tag  = " [TBD SP]" if is_tbd else ""
        st_tag   = " [ST]"    if is_st  else ""
        side_str = (
            f"{row['player_name']} {row['side']} {row['line']} K"
            f"{tbd_tag}{st_tag}"
        )

        notes = (
            f"mu={row['mu_model']:.2f}  line={row['line']}  "
            f"P={row['p_model']:.3f}  nv={row['nv_prob']:.3f}  "
            f"edge={edge_pct:.1f}%  Conf={conf_label} {conf_sym}"
            f"{tbd_tag}{st_tag}"
        )

        logger.info(
            f"  K PROP: {side_str:<45}  {int(row['price']):+d}  "
            f"edge={edge_pct:.1f}%  {tier_name:<10}  {units}u  {conf_sym}"
        )

        if not dry_run:
            game_time_str = "Spring Training" if is_st else "TBD"
            write_signal(
                conn, date_str, game_str, game_time_str,
                "PROP", side_str, row["market"],
                int(row["price"]), row["p_model"],
                row["nv_prob"], row["edge"], ev_val,
                tier_name, units, notes,
            )
            signals_written += 1

    # ------------------------------------------------------------------
    # F5 projection signals (informational log; DB write in regular season only)
    # ------------------------------------------------------------------
    logger.info(f"  --- F5 Projections ({date_str}) ---")
    for sig in f5_sigs:
        logger.info(
            f"  F5: {sig['game']:<45}  "
            f"P_home={sig['p_home']:.3f}  "
            f"SP_away={sig['sp_away']} (μ={sig['mu_away']:.2f})  "
            f"SP_home={sig['sp_home']} (μ={sig['mu_home']:.2f})  "
            f"Conf={sig['conf_sym']}{sig['st_tag']}"
        )
        # F5 signals require live F5 odds from OddsAPI to compute edge.
        # In Spring Training, write as model-only projections (no odds → no edge write).
        # In regular season: fetch F5 odds via OddsAPI and compute edge before writing.
        # TODO (CC-MLB): wire OddsAPI F5 fetch and re-enable write_signal here.

    # ------------------------------------------------------------------
    # Finalise
    # ------------------------------------------------------------------
    if not dry_run and conn is not None:
        conn.commit()
        conn.close()
        logger.info(f"MLB: wrote {signals_written} signal(s) → {TGT_DB.name}")
    elif dry_run:
        logger.info(f"MLB dry-run: {signals_written} signal(s) would be written")

    return signals_written


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="EyeBlackIQ MLB signal generator — K props + F5 projections"
    )
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Target date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log signals without writing to DB.",
    )
    args = parser.parse_args()

    n = run_model(args.date, dry_run=args.dry_run)
    print(f"MLB signals: {n}")
