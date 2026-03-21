"""
EyeBlackIQ — cap.py
Quarter-Kelly sizing with bankroll hard cap.

Rules (spec v4.1):
  - Quarter Kelly fraction: 0.25
  - Hard cap: 3% of bankroll per bet
  - Daily max: 15 bets
  - Tier system overrides minimum:
    T1 >= 12% edge = 2.0u cap
    T2 >= 5%  edge = 1.5u
    T3 >= 2%  edge = 1.0u
    T4 < 2%   edge = 0.0u (no bet)
"""
import os
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

KELLY_FRACTION   = float(os.getenv("KELLY_FRACTION",   "0.25"))
BANKROLL_MAX_PCT = float(os.getenv("BANKROLL_MAX_PCT", "0.03"))
MIN_EDGE_PCT     = float(os.getenv("MIN_EDGE_PCT",     "0.03"))
DAILY_MAX_BETS   = 15
MAX_UNITS        = 2.0


def kelly_fraction(prob: float, decimal_odds: float) -> float:
    """
    Full Kelly fraction of bankroll.

    f* = (b*p - q) / b
    where b = decimal_odds - 1, p = win prob, q = 1 - p
    """
    b = decimal_odds - 1.0
    if b <= 0:
        return 0.0
    q = 1.0 - prob
    f = (b * prob - q) / b
    return max(0.0, f)


def quarter_kelly(prob: float, decimal_odds: float) -> float:
    """Quarter-Kelly fraction of bankroll."""
    return KELLY_FRACTION * kelly_fraction(prob, decimal_odds)


def american_to_decimal(american: int) -> float:
    """Convert American odds to decimal."""
    if american > 0:
        return 1.0 + american / 100.0
    else:
        return 1.0 + 100.0 / abs(american)


def get_tier(edge_pct: float, sport: str = "generic") -> Tuple[str, float]:
    """
    Return (tier_label, units) based on edge and sport.

    edge_pct: 0-100 scale (e.g. 12.0 for 12%)
    """
    e = edge_pct * 100 if edge_pct < 1.0 else edge_pct  # normalize

    sport_lower = sport.lower()

    if "nhl" in sport_lower or "hockey" in sport_lower:
        if   e >= 12: return ("🎯 SNIPE",         2.0)
        elif e >=  5: return ("🎰 SLOT MACHINE",   1.5)
        elif e >=  2: return ("🟡 SCOUT",           1.0)
        else:         return ("⬛ ICING",           0.0)
    elif "soccer" in sport_lower or "football" in sport_lower:
        if   e >= 12: return ("🥅 UPPER 90",       2.0)
        elif e >=  5: return ("😏 CHEEKY",         1.5)
        elif e >=  2: return ("🟡 SCOUT",           1.0)
        else:         return ("⬛ PARK THE BUS",   0.0)
    elif "handball" in sport_lower:
        if   e >= 12: return ("💥 SCREAMER",       2.0)
        elif e >=  5: return ("⚡ FAST BREAK",      1.5)
        elif e >=  2: return ("🟡 SCOUT",           1.0)
        else:         return ("⬛ RED CARD",        0.0)
    else:  # MLB, NCAA, default
        if   e >= 12: return ("🔴 FILTHY",         2.0)
        elif e >=  5: return ("🔵 WHEELHOUSE",      1.5)
        elif e >=  2: return ("🟡 SCOUT",           1.0)
        else:         return ("⬛ BALK",            0.0)


def size_bet(
    prob: float,
    decimal_odds: float,
    edge_pct: float,
    bankroll: float,
    bets_today: int = 0,
    sport: str = "generic",
) -> dict:
    """
    Compute bet size in units and bankroll %.

    Returns dict with: units, bankroll_pct, tier, kelly_f, blocked_reason
    """
    if bets_today >= DAILY_MAX_BETS:
        return {
            "units": 0.0, "bankroll_pct": 0.0,
            "tier": "BLOCKED", "kelly_f": 0.0,
            "blocked_reason": f"Daily max {DAILY_MAX_BETS} bets reached"
        }

    tier_label, tier_units = get_tier(edge_pct, sport)

    if tier_units == 0.0:
        return {
            "units": 0.0, "bankroll_pct": 0.0,
            "tier": tier_label, "kelly_f": 0.0,
            "blocked_reason": "Edge below minimum tier threshold"
        }

    # Quarter-Kelly as % of bankroll
    qk = quarter_kelly(prob, decimal_odds)

    # Hard cap: 3% of bankroll
    capped_pct = min(qk, BANKROLL_MAX_PCT)

    # Convert to units (1 unit = 1% of bankroll by convention)
    units_kelly = capped_pct * 100

    # Use tier as minimum, capped at 2.0u max
    units = min(MAX_UNITS, max(tier_units, units_kelly))

    bankroll_pct = units / 100.0

    return {
        "units": round(units, 2),
        "bankroll_pct": round(bankroll_pct, 4),
        "tier": tier_label,
        "kelly_f": round(qk, 4),
        "blocked_reason": None,
    }
