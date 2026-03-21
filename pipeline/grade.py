"""
EyeBlackIQ — grade.py
Grade pending signals after games complete.

Usage:
  python pipeline/grade.py --date YYYY-MM-DD          # Grade all pending for date
  python pipeline/grade.py --signal-id 42 --result WIN  # Grade single signal
  python pipeline/grade.py --interactive               # Interactive mode

After grading, re-runs export.py to update website JSON.
"""
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

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "pipeline" / "db" / "eyeblackiq.db"


def get_conn():
    return sqlite3.connect(DB_PATH)


def get_pending(date_str: str = None) -> list:
    """Return all PENDING signals (not yet graded as results)."""
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        if date_str:
            cur = conn.execute(
                """SELECT s.id, s.signal_date, s.sport, s.game, s.side,
                          s.market, s.odds, s.units, s.bet_type
                   FROM signals s
                   LEFT JOIN results r ON r.signal_id = s.id
                   WHERE r.id IS NULL AND s.signal_date = ?
                   ORDER BY s.sport, s.id""",
                (date_str,)
            )
        else:
            cur = conn.execute(
                """SELECT s.id, s.signal_date, s.sport, s.game, s.side,
                          s.market, s.odds, s.units, s.bet_type
                   FROM signals s
                   LEFT JOIN results r ON r.signal_id = s.id
                   WHERE r.id IS NULL
                   ORDER BY s.signal_date DESC, s.sport, s.id"""
            )
        return [dict(r) for r in cur.fetchall()]


def grade_signal(signal_id: int, result: str, actual_val: str = None,
                 closing_line: int = None, clv: float = None, notes: str = None):
    """
    Grade a single signal.

    Args:
        signal_id:    signals.id
        result:       WIN | LOSS | PUSH | VOID
        actual_val:   e.g. "4 Ks" or "1-0 USC"
        closing_line: Pinnacle no-vig closing ML
        clv:          Closing line value (positive = beat close)
        notes:        Optional note
    """
    result = result.upper()
    assert result in ("WIN", "LOSS", "PUSH", "VOID"), f"Invalid result: {result}"

    ts = datetime.now(timezone.utc).isoformat()

    # Fetch signal
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT signal_date, sport, game, side, market, odds, units FROM signals WHERE id=?",
            (signal_id,)
        )
        row = cur.fetchone()
        if not row:
            logger.error(f"Signal {signal_id} not found")
            return False
        signal_date, sport, game, side, market, odds, units = row

        # Auto-compute units_net
        units_net = None
        if result == "WIN" and odds:
            if odds > 0:
                units_net = units * (odds / 100.0)
            else:
                units_net = units * (100.0 / abs(odds))
        elif result == "LOSS":
            units_net = -units
        elif result in ("PUSH", "VOID"):
            units_net = 0.0

        conn.execute(
            """INSERT INTO results
               (signal_id, signal_date, sport, game, side, market, odds, units,
                result, units_net, actual_val, closing_line, clv, notes, graded_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (signal_id, signal_date, sport, game, side, market, odds, units,
             result, units_net, actual_val, closing_line, clv, notes, ts)
        )
        conn.commit()

    logger.info(f"Graded signal {signal_id}: {result} ({units_net:+.2f}u) — {side}")

    # Also update pod_records if this was a POD
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id FROM signals WHERE id=? AND is_pod=1", (signal_id,)
        )
        if cur.fetchone():
            conn.execute(
                """UPDATE pod_records SET result=?, units_net=?, actual_val=?
                   WHERE date=? AND sport=?""",
                (result, units_net, actual_val, signal_date, sport)
            )
            conn.commit()
            logger.info(f"Updated pod_records for {sport} {signal_date}: {result}")

    return True


def grade_batch(date_str: str, grades: dict):
    """
    Grade multiple signals at once.

    grades: {signal_id: {"result": "WIN", "actual_val": "4 Ks"}}
    """
    success = 0
    for sid, data in grades.items():
        ok = grade_signal(
            int(sid),
            data["result"],
            data.get("actual_val"),
            data.get("closing_line"),
            data.get("clv"),
            data.get("notes"),
        )
        if ok:
            success += 1

    logger.info(f"Graded {success}/{len(grades)} signals for {date_str}")

    # Re-export JSON after grading
    try:
        from pipeline.export import run_export
        run_export(date_str)
        logger.info("JSON export updated after grading")
    except Exception as e:
        logger.warning(f"Export update failed: {e}")

    return success


def interactive_grade(date_str: str = None):
    """Interactive terminal grader."""
    pending = get_pending(date_str)
    if not pending:
        print(f"No pending signals for {date_str or 'all dates'}")
        return

    print(f"\n{'='*60}")
    print(f"  EyeBlackIQ — Grade Results")
    print(f"  {len(pending)} pending signals{f' for {date_str}' if date_str else ''}")
    print(f"{'='*60}\n")

    for sig in pending:
        print(f"[{sig['id']}]  {sig['sport']}  |  {sig['game']}")
        print(f"      {sig['side']}  {sig['market'] or ''}  {sig['odds']:+d}  {sig['units']}u")
        print(f"      Date: {sig['signal_date']}")

        result = input("  Result (W/L/P/V/skip): ").strip().upper()
        if result in ("", "SKIP", "S"):
            print("  Skipped\n")
            continue

        result_map = {"W": "WIN", "L": "LOSS", "P": "PUSH", "V": "VOID"}
        result = result_map.get(result, result)
        if result not in ("WIN", "LOSS", "PUSH", "VOID"):
            print(f"  Invalid: {result} — skipping\n")
            continue

        actual_val    = input("  Actual value (e.g. '4 Ks', press Enter to skip): ").strip() or None
        closing_input = input("  Closing line ML (press Enter to skip): ").strip()
        closing_line  = int(closing_input) if closing_input else None

        ok = grade_signal(sig["id"], result, actual_val, closing_line)
        print(f"  {'OK' if ok else 'FAILED'}\n")

    # Re-export
    from pipeline.export import run_export
    run_export(date_str or datetime.now().strftime("%Y-%m-%d"))
    print("JSON export updated.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Grade EyeBlackIQ signals")
    parser.add_argument("--date",      default=None, help="Date YYYY-MM-DD")
    parser.add_argument("--signal-id", type=int,     default=None)
    parser.add_argument("--result",    default=None, help="WIN|LOSS|PUSH|VOID")
    parser.add_argument("--actual",    default=None, help="Actual value string")
    parser.add_argument("--closing",   type=int,     default=None, help="Pinnacle closing ML")
    parser.add_argument("--clv",       type=float,   default=None)
    parser.add_argument("--interactive", action="store_true")
    args = parser.parse_args()

    if args.interactive:
        interactive_grade(args.date)
    elif args.signal_id and args.result:
        grade_signal(args.signal_id, args.result, args.actual, args.closing, args.clv)
    else:
        pending = get_pending(args.date)
        print(f"Pending signals{f' for {args.date}' if args.date else ''}: {len(pending)}")
        for p in pending:
            print(f"  [{p['id']}]  {p['sport']}  {p['side']}  {p['odds']:+d}  — {p['game']}")
