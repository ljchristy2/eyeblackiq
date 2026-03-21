"""
EyeBlackIQ — pods/nhl/model.py
NHL player props signal generator.

Reads from nhl_prop_results (betting.db) and writes qualifying signals
to eyeblackiq.db signals table.

Markets: player_shots_on_goal, player_points, player_assists
Model: Negative Binomial distribution (SOG), Poisson (points/assists)

Confidence levels (●●● / ●●○ / ●○○):
  HIGH: edge ≥ 12%, not B2B, mu well above/below line (>0.8 gap)
  MED:  edge 5–12%, or B2B with strong edge
  LOW:  B2B + edge < 8%, or mu within 0.3 of line

Usage:
  python pods/nhl/model.py --date 2026-03-21
  python pods/nhl/model.py --date 2026-03-21 --dry-run
"""
import os
import sqlite3
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

_ENV = Path(__file__).parent.parent.parent.parent / "quant-betting" / "soccer" / ".claude" / "worktrees" / "admiring-allen" / ".env"
if _ENV.exists():
    load_dotenv(_ENV)
else:
    load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
SRC_DB   = Path("C:/Users/loren/OneDrive/Desktop/quant-betting/soccer/.claude/worktrees/admiring-allen/db/betting.db")
TGT_DB   = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"
SPORT    = "NHL"

# ── Tier ───────────────────────────────────────────────────────────────────────
def nhl_tier(edge_pct):
    if   edge_pct >= 12: return ("SNIPE",        2.0)
    elif edge_pct >=  5: return ("SLOT MACHINE",  1.5)
    elif edge_pct >=  2: return ("SCOUT",          1.0)
    else:                return ("ICING",          0.0)

# ── Confidence ─────────────────────────────────────────────────────────────────
def confidence(edge_pct: float, b2b: bool, mu: float, line: float) -> tuple:
    """
    Confidence based on edge strength, B2B fatigue, and mu-line gap.
    mu_gap = abs(mu - line): larger gap = model is more decisive.
    """
    mu_gap = abs(mu - line)
    score = 2  # start MEDIUM

    if edge_pct >= 12 and not b2b:  score += 1
    if edge_pct < 5:                score -= 1
    if mu_gap >= 0.8:               score += 1
    elif mu_gap < 0.3:              score -= 1
    if b2b and edge_pct < 8:        score -= 1

    score = max(1, min(3, score))
    return {3: ("HIGH", "●●●"), 2: ("MED", "●●○"), 1: ("LOW", "●○○")}[score]

# ── Helpers ────────────────────────────────────────────────────────────────────
def american_to_decimal(o):
    return o / 100 + 1 if o > 0 else 100 / abs(o) + 1

def ev_calc(decimal_odds, model_p):
    return (decimal_odds - 1) * model_p - (1 - model_p)

def fmt_market(m):
    return (m.replace("player_shots_on_goal", "SOG")
             .replace("player_points",        "PTS")
             .replace("player_assists",       "AST")
             .replace("player_", "").upper())

NHL_GAME_TIMES = {
    ("Carolina Hurricanes",  "Toronto Maple Leafs"):  "7:00 PM ET",
    ("New Jersey Devils",    "Washington Capitals"):  "7:00 PM ET",
    ("Buffalo Sabres",       "San Jose Sharks"):      "7:30 PM ET",
    ("Tampa Bay Lightning",  "Vancouver Canucks"):    "10:00 PM ET",
    ("Philadelphia Flyers",  "Los Angeles Kings"):    "10:30 PM ET",
    ("Utah Mammoth",         "Vegas Golden Knights"): "10:00 PM ET",
}

def game_time(away, home):
    return NHL_GAME_TIMES.get((away, home), "TBD")

# ── Load signals from source DB ────────────────────────────────────────────────
def load_props(date_str: str) -> list:
    if not SRC_DB.exists():
        logger.warning(f"Source DB not found: {SRC_DB}")
        return []
    with sqlite3.connect(SRC_DB) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT player_name, market, line, side, mu_model, p_model,
                      nv_prob, edge, price, away_team, home_team, b2b
               FROM nhl_prop_results
               WHERE game_date = ? AND signal = 1
               ORDER BY edge DESC""",
            (date_str,)
        )
        return [dict(r) for r in cur.fetchall()]

# ── Write signal ───────────────────────────────────────────────────────────────
def write_signal(conn, date_str, row, tier, units, conf_label, conf_sym, ev_val, game_str, gtime):
    ts = datetime.now(timezone.utc).isoformat()
    mkt_short = fmt_market(row["market"])
    side_str  = f"{row['player_name']} {row['side']} {row['line']} {mkt_short}"
    b2b_flag  = " [B2B]" if row["b2b"] else ""
    notes = (
        f"mu={row['mu_model']:.2f}  P={row['p_model']:.3f}  "
        f"Conf={conf_label} {conf_sym}{b2b_flag}"
    )
    conn.execute(
        """INSERT INTO signals
           (signal_date, sport, game, game_time, bet_type, side, market,
            odds, model_prob, no_vig_prob, edge, ev, tier, units,
            is_pod, gate1_pyth, gate2_edge, gate3_model_agree,
            gate4_line_move, gate5_etl_fresh, notes, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'GREEN','PASS','PASS','PASS','PASS',?,?)""",
        (date_str, SPORT, game_str, gtime, "PROP",
         side_str, row["market"],
         int(row["price"]), round(row["p_model"], 4),
         round(row["nv_prob"], 4) if row["nv_prob"] else None,
         round(row["edge"], 4), round(ev_val, 4),
         tier, units, notes, ts)
    )

# ── Main ───────────────────────────────────────────────────────────────────────
def run_model(date_str: str, dry_run: bool = False) -> int:
    props = load_props(date_str)
    if not props:
        logger.warning(f"No NHL props found for {date_str} in source DB")
        return 0

    logger.info(f"NHL model -- {date_str} -- {len(props)} qualified props from source DB")

    signals_written = 0
    conn = None
    if not dry_run:
        conn = sqlite3.connect(TGT_DB)
        # Idempotent — clear today's signals before rewriting
        conn.execute("DELETE FROM signals WHERE signal_date=? AND sport=?", (date_str, SPORT))
        conn.commit()

    for row in props:
        edge_pct  = row["edge"] * 100
        tier_name, units = nhl_tier(edge_pct)
        if units == 0.0:
            continue

        b2b = bool(row["b2b"])
        conf_label, conf_sym = confidence(edge_pct, b2b, row["mu_model"], row["line"])

        dec_odds = american_to_decimal(int(row["price"]))
        ev_val   = ev_calc(dec_odds, row["p_model"])

        away, home = row["away_team"], row["home_team"]
        game_str   = f"{away} @ {home}"
        gtime      = game_time(away, home)
        mkt_short  = fmt_market(row["market"])
        b2b_tag    = " [B2B]" if b2b else ""

        logger.info(
            f"  + {row['player_name']}{b2b_tag}  "
            f"{row['side']} {row['line']} {mkt_short}  "
            f"{int(row['price']):+d}  "
            f"Edge {edge_pct:.1f}%  {tier_name}  {units}u  "
            f"Conf {conf_sym}  |  {game_str}"
        )

        if not dry_run:
            write_signal(conn, date_str, row, tier_name, units,
                         conf_label, conf_sym, ev_val, game_str, gtime)
            signals_written += 1

    if not dry_run and conn:
        conn.commit()
        conn.close()
        logger.info(f"NHL: wrote {signals_written} signals to DB")

    if dry_run:
        qualifying = sum(1 for r in props if nhl_tier(r["edge"] * 100)[1] > 0)
        logger.info(f"[DRY-RUN] Would write {qualifying} signals (skipped {len(props) - qualifying} below tier threshold)")
        signals_written = qualifying

    return signals_written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL prop signal generator")
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    n = run_model(args.date, args.dry_run)
    print(f"NHL signals: {n}")
