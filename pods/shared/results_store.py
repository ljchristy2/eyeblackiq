"""
EyeBlackIQ — results_store.py
Stores signals, grades results, tracks CLV.

Key functions:
  - store_signal(): Save a model signal before game
  - grade_result(): Fill in WIN/LOSS/PUSH after game
  - get_summary(): Season P&L summary by sport
"""
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)


class ResultsStore:
    """
    Interface to signals + results tables in eyeblackiq.db.

    Usage:
        store = ResultsStore("pipeline/db/eyeblackiq.db")
        sig_id = store.store_signal(...)
        store.grade_result(sig_id, "WIN", units_net=1.5, actual_val="5 Ks")
    """

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def store_signal(
        self,
        signal_date: str,
        sport: str,
        game: str,
        game_time: Optional[str],
        bet_type: str,
        side: str,
        market: Optional[str],
        odds: Optional[int],
        model_prob: float,
        no_vig_prob: Optional[float],
        edge: float,
        ev: Optional[float],
        tier: Optional[str],
        units: float,
        is_pod: bool = False,
        pod_sport: Optional[str] = None,
        notes: Optional[str] = None,
        gate1: str = "PASS", gate2: str = "PASS",
        gate3: str = "PASS", gate4: str = "PASS", gate5: str = "PASS",
    ) -> int:
        """
        Store a model signal. Returns signal ID.
        """
        ts = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO signals
                   (signal_date, sport, game, game_time, bet_type, side, market,
                    odds, model_prob, no_vig_prob, edge, ev, tier, units,
                    is_pod, pod_sport, notes,
                    gate1_pyth, gate2_edge, gate3_model_agree, gate4_line_move, gate5_etl_fresh,
                    created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (signal_date, sport, game, game_time, bet_type, side, market,
                 odds, model_prob, no_vig_prob, edge, ev, tier, units,
                 int(is_pod), pod_sport, notes,
                 gate1, gate2, gate3, gate4, gate5,
                 ts)
            )
            conn.commit()
            return cur.lastrowid

    def grade_result(
        self,
        signal_id: int,
        result: str,
        units_net: Optional[float] = None,
        actual_val: Optional[str] = None,
        closing_line: Optional[int] = None,
        clv: Optional[float] = None,
        notes: Optional[str] = None,
    ) -> None:
        """
        Grade a signal result. Result: WIN | LOSS | PUSH | VOID.
        """
        ts = datetime.now(timezone.utc).isoformat()
        # Fetch signal data
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT signal_date, sport, game, side, market, odds, units FROM signals WHERE id=?",
                (signal_id,)
            )
            row = cur.fetchone()
            if not row:
                logger.error(f"Signal {signal_id} not found")
                return
            signal_date, sport, game, side, market, odds, units = row

            # Compute units_net if not provided
            if units_net is None and result in ("WIN", "LOSS", "PUSH"):
                if result == "WIN" and odds is not None:
                    if odds > 0:
                        units_net = units * (odds / 100.0)
                    else:
                        units_net = units * (100.0 / abs(odds))
                elif result == "LOSS":
                    units_net = -units
                else:
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
        logger.info(f"Graded signal {signal_id}: {result} ({units_net:+.2f}u)")

    def get_summary(self, sport: Optional[str] = None, days: int = 30) -> Dict:
        """
        Return P&L summary. Optionally filter by sport and lookback days.
        """
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

        where = "WHERE r.signal_date >= ?"
        params = [cutoff]
        if sport:
            where += " AND r.sport = ?"
            params.append(sport)

        with self._conn() as conn:
            cur = conn.execute(
                f"""SELECT
                       COUNT(*) as n_bets,
                       SUM(CASE WHEN r.result='WIN'  THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN r.result='LOSS' THEN 1 ELSE 0 END) as losses,
                       SUM(r.units_net) as units_net,
                       AVG(r.units_net) as avg_net,
                       SUM(CASE WHEN r.clv > 0 THEN 1 ELSE 0 END) as clv_pos,
                       COUNT(r.clv) as clv_total
                   FROM results r
                   {where}
                   AND r.result IN ('WIN','LOSS')""",
                params
            )
            row = cur.fetchone()

        if not row or row[0] == 0:
            return {"n_bets": 0, "message": "No graded results"}

        n, wins, losses, units_net, avg_net, clv_pos, clv_total = row
        clv_pct = (clv_pos / clv_total * 100) if clv_total > 0 else None

        return {
            "n_bets": n,
            "record": f"{wins}W-{losses}L",
            "units_net": round(units_net or 0, 2),
            "avg_net_per_bet": round(avg_net or 0, 4),
            "roi_pct": round((units_net or 0) / n * 100, 2) if n > 0 else 0,
            "clv_pct": round(clv_pct, 1) if clv_pct is not None else None,
            "days": days,
            "sport": sport or "ALL",
        }
