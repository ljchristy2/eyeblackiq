"""
EyeBlackIQ — pods/soccer/model.py
Soccer player props + team ML signal generator.

Markets: player_shots_on_target, player_goals, player_assists
Model: Poisson (goals/assists), custom SOT distribution

Confidence levels (●●● / ●●○ / ●○○):
  HIGH: edge ≥ 12%, top league, mu gap ≥ 0.3
  MED:  edge 5–12% or mid-tier league
  LOW:  edge < 5%, MLS/lower league, mu gap < 0.15

Usage:
  python pods/soccer/model.py --date 2026-03-21
  python pods/soccer/model.py --date 2026-03-21 --dry-run
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
SPORT    = "SOCCER"

# Top leagues (higher confidence)
TOP_LEAGUES = {"prizepicks", "epl", "laliga", "bundesliga", "seriea", "ligue1", "ucl"}

# ── Tier ───────────────────────────────────────────────────────────────────────
def soccer_tier(edge_pct):
    if   edge_pct >= 12: return ("🥅 UPPER 90",    2.0)
    elif edge_pct >=  5: return ("😏 CHEEKY",       1.5)
    elif edge_pct >=  2: return ("🟡 SCOUT",         1.0)
    else:                return ("⬛ PARK THE BUS", 0.0)

# ── Confidence ─────────────────────────────────────────────────────────────────
def confidence(edge_pct: float, mu: float, line: float, sport_key: str) -> tuple:
    """
    Confidence for soccer props.
    top_league bonus: EPL/La Liga/Bundesliga data is more reliable.
    mu_gap: model decisiveness.
    """
    mu_gap = abs(mu - line)
    key_lower = (sport_key or "").lower()
    is_top = any(lg in key_lower for lg in TOP_LEAGUES)

    score = 2

    if edge_pct >= 12:   score += 1
    if edge_pct < 5:     score -= 1
    if mu_gap >= 0.3:    score += 1
    elif mu_gap < 0.15:  score -= 1
    if is_top:           score += 1
    else:                score -= 1   # lower leagues: less data confidence

    score = max(1, min(3, score))
    return {3: ("HIGH", "●●●"), 2: ("MED", "●●○"), 1: ("LOW", "●○○")}[score]

# ── Helpers ────────────────────────────────────────────────────────────────────
def american_to_decimal(o):
    """Convert American odds to decimal."""
    return o / 100 + 1 if o > 0 else 100 / abs(o) + 1

def ev_calc(decimal_odds, model_p):
    """Expected value given decimal odds and model probability."""
    return (decimal_odds - 1) * model_p - (1 - model_p)

def fmt_market(m):
    """Shorten market string for display."""
    return (m.replace("player_shots_on_target", "SOT")
             .replace("player_goals",           "Goals")
             .replace("player_assists",         "Assists")
             .replace("player_shots",           "Shots")
             .replace("player_", "").upper())

# ── Team ML (hardcoded forward signal) ─────────────────────────────────────────
TEAM_ML_SIGNALS = [
    {
        "home": "Real Madrid", "away": "Atletico Madrid",
        "league": "La Liga", "date": "2026-03-22", "time": "3:00 PM ET",
        "model_prob": 0.559, "mkt_odds": -193, "nv_prob": 0.522,
        "edge": 0.037, "fair_ml": -125,
    }
]

# ── Load props from source DB ──────────────────────────────────────────────────
def load_props(date_str: str) -> list:
    """
    Pull qualified prop signals for a given date from betting.db.
    Returns rows where signal = 1 (model gate passed), ordered by edge desc.
    """
    if not SRC_DB.exists():
        logger.warning(f"Source DB not found: {SRC_DB}")
        return []
    with sqlite3.connect(SRC_DB) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT player_name, market, line, side, mu_model, p_model,
                      nv_prob, edge, price, home_team, away_team, sport_key
               FROM soccer_prop_results
               WHERE DATE(created_at) = ? AND signal = 1
               ORDER BY edge DESC""",
            (date_str,)
        )
        return [dict(r) for r in cur.fetchall()]

# ── Write signal ───────────────────────────────────────────────────────────────
def write_signal(conn, date_str, game, game_time, bet_type, side, market,
                 odds, model_p, nv_p, edge, ev_val, tier, units, notes):
    """Insert a single signal row into eyeblackiq.db signals table."""
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO signals
           (signal_date, sport, game, game_time, bet_type, side, market,
            odds, model_prob, no_vig_prob, edge, ev, tier, units,
            is_pod, gate1_pyth, gate2_edge, gate3_model_agree,
            gate4_line_move, gate5_etl_fresh, notes, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'GREEN','PASS','PASS','PASS','PASS',?,?)""",
        (date_str, SPORT, game, game_time, bet_type, side, market,
         odds, round(model_p, 4), round(nv_p, 4) if nv_p else None,
         round(edge, 4), round(ev_val, 4), tier, units, notes, ts)
    )

# ── Main ───────────────────────────────────────────────────────────────────────
def run_model(date_str: str, dry_run: bool = False) -> int:
    """
    Run soccer signal generator for a given date.

    Args:
        date_str: ISO date string (YYYY-MM-DD)
        dry_run:  If True, log signals but do not write to DB

    Returns:
        Number of signals written (0 if dry_run)
    """
    props = load_props(date_str)
    logger.info(f"Soccer model — {date_str} — {len(props)} qualified props")

    signals_written = 0
    conn = None
    if not dry_run:
        conn = sqlite3.connect(TGT_DB)
        # Idempotent — clear today's signals before rewriting
        conn.execute("DELETE FROM signals WHERE signal_date=? AND sport=?", (date_str, SPORT))
        conn.commit()

    # ── Player props ──
    for row in props:
        edge_pct  = row["edge"] * 100
        tier_name, units = soccer_tier(edge_pct)
        if units == 0.0:
            continue

        conf_label, conf_sym = confidence(
            edge_pct, row["mu_model"], row["line"], row.get("sport_key", "")
        )

        dec_odds  = american_to_decimal(int(row["price"]))
        ev_val    = ev_calc(dec_odds, row["p_model"])
        home, away = row["home_team"], row["away_team"]
        game_str  = f"{away} @ {home}" if away else home
        mkt_short = fmt_market(row["market"])
        src_tag   = f" [{row['sport_key'].upper()}]" if row.get("sport_key") else ""
        side_str  = f"{row['player_name']}{src_tag} {row['side']} {row['line']} {mkt_short}"

        notes = (
            f"mu={row['mu_model']:.3f}  P={row['p_model']:.3f}  "
            f"Conf={conf_label} {conf_sym}  "
            f"League={row.get('sport_key', '?')}"
        )

        logger.info(
            f"  + {side_str}  {int(row['price']):+d}  "
            f"Edge {edge_pct:.1f}%  {tier_name}  {units}u  Conf {conf_sym}"
        )

        if not dry_run:
            write_signal(
                conn, date_str, game_str, "TBD",
                "PROP", side_str, row["market"],
                int(row["price"]), row["p_model"],
                row.get("nv_prob"), row["edge"], ev_val,
                tier_name, units, notes
            )
            signals_written += 1

    # ── Team ML forward signals ──
    for tm in TEAM_ML_SIGNALS:
        edge_pct_ml = tm["edge"] * 100
        tier_name, units = soccer_tier(edge_pct_ml)
        if units == 0.0:
            logger.info(
                f"  Team ML: {tm['home']} — edge {edge_pct_ml:.1f}% -> PARK THE BUS"
            )
            continue

        conf_label, conf_sym = confidence(
            edge_pct_ml, tm["model_prob"], tm["nv_prob"], "laliga"
        )
        dec_odds = american_to_decimal(tm["mkt_odds"])
        ev_val   = ev_calc(dec_odds, tm["model_prob"])
        game_str = f"{tm['home']} vs {tm['away']}"
        notes = (
            f"Fair ML: {tm['fair_ml']:+d}  "
            f"Conf={conf_label} {conf_sym}  League={tm['league']}"
        )

        logger.info(
            f"  + TEAM ML: {tm['home']} {tm['mkt_odds']:+d}  "
            f"Edge {edge_pct_ml:.1f}%  {tier_name}  {units}u  Conf {conf_sym}  "
            f"({tm['date']} {tm['time']})"
        )

        if not dry_run:
            write_signal(
                conn, date_str, game_str, f"{tm['date']} {tm['time']}",
                "ML", f"{tm['home']} ML", "ML",
                tm["mkt_odds"], tm["model_prob"], tm["nv_prob"],
                tm["edge"], ev_val, tier_name, units, notes
            )
            signals_written += 1

    if not dry_run and conn is not None:
        conn.commit()
        conn.close()
        logger.info(f"Soccer: wrote {signals_written} signals to DB")

    return signals_written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Soccer signal generator")
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    n = run_model(args.date, args.dry_run)
    print(f"Soccer signals: {n}")
