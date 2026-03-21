"""
EyeBlackIQ — drawdown.py
Kill switch monitoring per spec v4.1.

Kill switches:
  - 3-month rolling CLV < 0 -> full suspension
  - 25 consecutive losses -> 72-hour review
  - 7 consecutive losing days -> stakes to 25%, 5-day review
"""
import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class KillSwitchError(Exception):
    """Raised when a kill switch is triggered."""
    pass


class DrawdownMonitor:
    """
    Monitors betting results and enforces kill switches.

    Usage:
        monitor = DrawdownMonitor(db_path="pipeline/db/eyeblackiq.db")
        status = monitor.check()
        if status["blocked"]:
            raise KillSwitchError(status["reason"])
    """

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def consecutive_losses(self) -> int:
        """Count current consecutive loss streak from most recent graded results."""
        with self._get_conn() as conn:
            cur = conn.execute(
                """SELECT result FROM results
                   WHERE result IN ('WIN','LOSS')
                   ORDER BY signal_date DESC, id DESC
                   LIMIT 50"""
            )
            rows = [r[0] for r in cur.fetchall()]

        streak = 0
        for r in rows:
            if r == "LOSS":
                streak += 1
            else:
                break
        return streak

    def consecutive_losing_days(self) -> int:
        """Count current streak of losing calendar days."""
        with self._get_conn() as conn:
            cur = conn.execute(
                """SELECT signal_date, SUM(units_net) as day_net
                   FROM results
                   WHERE result IN ('WIN','LOSS')
                   GROUP BY signal_date
                   ORDER BY signal_date DESC
                   LIMIT 30"""
            )
            days = cur.fetchall()

        streak = 0
        for date, net in days:
            if net is not None and net < 0:
                streak += 1
            else:
                break
        return streak

    def rolling_3m_clv(self) -> Optional[float]:
        """Calculate 3-month rolling CLV % (fraction of bets that beat closing line)."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            cur = conn.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) as positive_clv
                   FROM results
                   WHERE signal_date >= ? AND clv IS NOT NULL
                   AND result IN ('WIN','LOSS')""",
                (cutoff,)
            )
            row = cur.fetchone()

        if not row or row[0] < 20:  # Need at least 20 graded bets
            return None

        total, pos = row
        return (pos / total) if total > 0 else None

    def check(self) -> dict:
        """
        Run all kill switch checks. Returns status dict.

        Returns:
            {
                "blocked": bool,
                "stake_multiplier": float,  # 1.0 normal, 0.25 reduced
                "reason": str or None,
                "consec_losses": int,
                "consec_losing_days": int,
                "clv_3m": float or None,
            }
        """
        try:
            consec_losses = self.consecutive_losses()
            consec_days = self.consecutive_losing_days()
            clv_3m = self.rolling_3m_clv()
        except Exception as e:
            logger.warning(f"DrawdownMonitor check failed: {e}")
            return {
                "blocked": False, "stake_multiplier": 1.0,
                "reason": f"Monitor error: {e}",
                "consec_losses": 0, "consec_losing_days": 0, "clv_3m": None
            }

        # Kill switch 1: 3-month rolling CLV < 0 -> full suspension
        if clv_3m is not None and clv_3m < 0.50:  # < 50% CLV positive = below breakeven
            reason = f"FULL SUSPENSION: 3-month CLV {clv_3m*100:.1f}% < 50% — HUMAN REVIEW REQUIRED"
            logger.critical(reason)
            return {
                "blocked": True, "stake_multiplier": 0.0,
                "reason": reason,
                "consec_losses": consec_losses,
                "consec_losing_days": consec_days,
                "clv_3m": clv_3m,
            }

        # Kill switch 2: 25 consecutive losses -> 72-hour review
        if consec_losses >= 25:
            reason = f"72HR REVIEW: {consec_losses} consecutive losses — pause and review"
            logger.critical(reason)
            return {
                "blocked": True, "stake_multiplier": 0.0,
                "reason": reason,
                "consec_losses": consec_losses,
                "consec_losing_days": consec_days,
                "clv_3m": clv_3m,
            }

        # Kill switch 3: 7 consecutive losing days -> 25% stakes, 5-day review
        if consec_days >= 7:
            reason = f"REDUCED STAKES (25%): {consec_days} consecutive losing days — 5-day review"
            logger.warning(reason)
            return {
                "blocked": False, "stake_multiplier": 0.25,
                "reason": reason,
                "consec_losses": consec_losses,
                "consec_losing_days": consec_days,
                "clv_3m": clv_3m,
            }

        return {
            "blocked": False, "stake_multiplier": 1.0,
            "reason": None,
            "consec_losses": consec_losses,
            "consec_losing_days": consec_days,
            "clv_3m": clv_3m,
        }
