"""
EyeBlackIQ — month_context.py
Monthly performance context and rolling stats for dashboard display.
Provides context strings like "March: 12W-8L +4.2u (+21.3% ROI)"
"""
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


class MonthContext:
    """
    Generates monthly and rolling performance context strings.

    Usage:
        ctx = MonthContext("pipeline/db/eyeblackiq.db")
        print(ctx.month_summary("NCAA_BASEBALL"))
        print(ctx.season_record())
    """

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def month_summary(self, sport: Optional[str] = None) -> str:
        """
        Returns month-to-date summary string.
        e.g. "March: 12W-8L  +4.2u  ROI +21.3%"
        """
        now = datetime.now(timezone.utc)
        month_start = now.strftime("%Y-%m-01")
        month_name = now.strftime("%B")

        where = "WHERE r.signal_date >= ? AND r.result IN ('WIN','LOSS')"
        params = [month_start]
        if sport:
            where += " AND r.sport = ?"
            params.append(sport)

        with self._conn() as conn:
            cur = conn.execute(
                f"""SELECT
                       COUNT(*) as n,
                       SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
                       SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l,
                       SUM(units_net) as net
                   FROM results r {where}""",
                params
            )
            row = cur.fetchone()

        if not row or row[0] == 0:
            sport_str = f" ({sport})" if sport else ""
            return f"{month_name}{sport_str}: No results yet"

        n, w, l, net = row
        net = net or 0
        roi = (net / n * 100) if n > 0 else 0
        sport_str = f" {sport}" if sport else ""
        sign = "+" if net >= 0 else ""
        return f"{month_name}{sport_str}: {w}W-{l}L  {sign}{net:.1f}u  ROI {sign}{roi:.1f}%"

    def season_record(self, sport: Optional[str] = None) -> str:
        """
        Returns season-to-date record string.
        """
        now = datetime.now(timezone.utc)
        year_start = now.strftime("%Y-01-01")

        where = "WHERE r.signal_date >= ? AND r.result IN ('WIN','LOSS')"
        params = [year_start]
        if sport:
            where += " AND r.sport = ?"
            params.append(sport)

        with self._conn() as conn:
            cur = conn.execute(
                f"""SELECT COUNT(*),
                       SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
                       SUM(units_net)
                   FROM results r {where}""",
                params
            )
            row = cur.fetchone()

        if not row or row[0] == 0:
            return "0W-0L  +0.0u"

        n, w, l, net = row
        net = net or 0
        sign = "+" if net >= 0 else ""
        return f"{w}W-{l}L  {sign}{net:.1f}u"

    def pod_record(self, sport: Optional[str] = None) -> str:
        """
        Returns POD-specific record string from pod_records table.
        """
        where = "WHERE result IN ('WIN','LOSS')"
        params = []
        if sport:
            where += " AND sport = ?"
            params.append(sport)

        with self._conn() as conn:
            cur = conn.execute(
                f"""SELECT COUNT(*),
                       SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
                       SUM(units_net)
                   FROM pod_records {where}""",
                params
            )
            row = cur.fetchone()

        if not row or row[0] == 0:
            return "0W-0L  +0.0u"

        n, w, l, net = row
        net = net or 0
        sign = "+" if net >= 0 else ""
        return f"{w}W-{l}L  {sign}{net:.1f}u"

    def all_sports_dashboard(self) -> List[str]:
        """Returns list of context strings for all sports."""
        sports = ["NCAA_BASEBALL", "NHL", "MLB", "SOCCER", "HANDBALL"]
        lines = ["=== EyeBlackIQ Season Dashboard ==="]
        for sport in sports:
            lines.append(self.season_record(sport) + f"  [{sport}]")
        lines.append(f"ALL SPORTS: {self.season_record()}")
        return lines
