"""
EyeBlackIQ — export.py
Reads today's signals from eyeblackiq.db and writes JSON to docs/data/.

Writes:
  - docs/data/today_slip.json     ← Today's picks (recommended + flagged)
  - docs/data/record.json         ← Season P&L summary
  - docs/data/results.json        ← Last 10 graded results

Run after model signals are generated:
  python pipeline/export.py [--date YYYY-MM-DD]
"""
import json
import sqlite3
import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

BASE_DIR  = Path(__file__).parent.parent
DB_PATH   = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"
DOCS_DATA = BASE_DIR / "docs" / "data"

MIN_EDGE = 0.03
MAX_EDGE = 0.15


def edge_window(edge):
    """Returns 'recommended', 'flagged_high', or 'flagged_low'."""
    if edge < MIN_EDGE:  return "flagged_low"
    if edge > MAX_EDGE:  return "flagged_high"
    return "recommended"


def get_conn():
    return sqlite3.connect(DB_PATH)


def export_today_slip(date_str: str) -> dict:
    """
    Reads signals for date_str from DB, returns slip dict.
    Splits into recommended and flagged sections.
    """
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT id, sport, game, game_time, bet_type, side, market,
                      odds, model_prob, no_vig_prob, edge, ev, tier, units,
                      is_pod, pod_sport, notes,
                      gate1_pyth, gate2_edge, gate3_model_agree,
                      gate4_line_move, gate5_etl_fresh
               FROM signals
               WHERE signal_date = ?
               ORDER BY sport, is_pod DESC, edge DESC""",
            (date_str,)
        )
        rows = [dict(r) for r in cur.fetchall()]

    # Also get POD records for today
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT * FROM pod_records WHERE date = ?", (date_str,)
        )
        pods = [dict(r) for r in cur.fetchall()]

    recommended = []
    flagged     = []

    for row in rows:
        edge_val = row.get("edge") or 0
        status = edge_window(edge_val)
        row["edge_status"] = status
        row["edge_pct"]    = round(edge_val * 100, 2)
        if status == "recommended":
            recommended.append(row)
        else:
            flag_reason = "Edge >15% — model artifact risk" if status == "flagged_high" else "Edge <3% — below threshold"
            row["flag_reason"] = flag_reason
            flagged.append(row)

    # Build POD summary
    pod_summary = []
    for p in pods:
        pod_summary.append({
            "sport":      p["sport"],
            "pick":       p["pick"],
            "odds":       p["odds"],
            "tier":       p["tier"],
            "units":      p["units"],
            "game":       p["game"],
            "game_time":  p["game_time"],
            "model_prob": p["model_prob"],
            "edge":       round((p["edge"] or 0) * 100, 2),
            "ev":         p["ev"],
            "result":     p["result"],
        })

    slip = {
        "date":        date_str,
        "generated":   datetime.now(timezone.utc).isoformat(),
        "recommended": recommended,
        "flagged":     flagged,
        "pod":         pod_summary,
        "counts": {
            "recommended": len(recommended),
            "flagged":     len(flagged),
            "pods":        len(pod_summary),
        }
    }
    return slip


def export_record() -> dict:
    """Reads results table for season summary."""
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT
                   COUNT(*) as n,
                   SUM(CASE WHEN result='WIN'  THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                   SUM(CASE WHEN result='PUSH' THEN 1 ELSE 0 END) as pushes,
                   SUM(units_net) as net_units,
                   SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) as clv_pos,
                   COUNT(clv) as clv_n
               FROM results
               WHERE result IN ('WIN','LOSS','PUSH')"""
        )
        row = cur.fetchone()

    if not row or row[0] == 0:
        n = wins = losses = pushes = 0
        net_units = roi = clv_pct = 0.0
    else:
        n, wins, losses, pushes, net_units, clv_pos, clv_n = row
        net_units = round(net_units or 0, 2)
        roi       = round((net_units / n * 100) if n > 0 else 0, 2)
        clv_pct   = round((clv_pos / clv_n * 100) if clv_n > 0 else 0, 1)

    # POD record
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT
                   SUM(CASE WHEN result='WIN'  THEN 1 ELSE 0 END),
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END)
               FROM pod_records
               WHERE result IN ('WIN','LOSS')"""
        )
        pr = cur.fetchone()
    pod_wins   = pr[0] or 0
    pod_losses = pr[1] or 0

    # Current streak
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT result FROM results
               WHERE result IN ('WIN','LOSS')
               ORDER BY signal_date DESC, id DESC
               LIMIT 20"""
        )
        streak_rows = [r[0] for r in cur.fetchall()]

    streak_n = 0
    streak_type = None
    for r in streak_rows:
        if streak_type is None:
            streak_type = r
        if r == streak_type:
            streak_n += 1
        else:
            break

    if streak_n == 0:
        streak_str = "—"
    else:
        streak_str = f"{'W' if streak_type == 'WIN' else 'L'}{streak_n}"

    return {
        "wins":        wins or 0,
        "losses":      losses or 0,
        "pushes":      pushes or 0,
        "net_units":   net_units,
        "roi":         roi,
        "clv_pct":     clv_pct,
        "pod_wins":    pod_wins,
        "pod_losses":  pod_losses,
        "streak":      streak_str,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


def export_results(limit: int = 10) -> list:
    """Returns last N graded results."""
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT r.signal_date, r.sport, r.game, r.side, r.market,
                      r.odds, r.units, r.result, r.units_net,
                      r.actual_val, r.clv, r.graded_at
               FROM results r
               WHERE r.result IN ('WIN','LOSS','PUSH','VOID')
               ORDER BY r.signal_date DESC, r.id DESC
               LIMIT ?""",
            (limit,)
        )
        return [dict(r) for r in cur.fetchall()]


def write_json(path: Path, data):
    """Write JSON with pretty print."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Wrote {path.name}  ({path.stat().st_size:,} bytes)")


def run_export(date_str: str):
    """Main export — writes all three JSON files."""
    DOCS_DATA.mkdir(parents=True, exist_ok=True)

    slip    = export_today_slip(date_str)
    record  = export_record()
    results = export_results(10)

    write_json(DOCS_DATA / "today_slip.json", slip)
    write_json(DOCS_DATA / "record.json",     record)
    write_json(DOCS_DATA / "results.json",    results)

    logger.info(
        f"Export complete — {slip['counts']['recommended']} recommended, "
        f"{slip['counts']['flagged']} flagged, {slip['counts']['pods']} PODs"
    )
    return slip, record, results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export eyeblackiq signals to JSON")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="Date YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    run_export(args.date)
