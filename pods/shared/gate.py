"""
EyeBlackIQ — gate.py
5-gate sequential signal filter. ALL 5 must pass or signal is blocked.

Gates (in order — first failure blocks all subsequent):
  1. Pythagorean gate: GREEN or YELLOW-cleared. RED = blocked.
  2. Edge >= 3.0% above no-vig fair market probability.
  3. Decomposition model and direct model agree within 15%.
  4. Line has not moved > 0.5 units sharp-direction against model.
  5. ETL data timestamp < 4 hours old.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

MIN_EDGE_PCT = 0.03       # Gate 2: minimum edge
MODEL_AGREE_THRESH = 0.15  # Gate 3: max model disagreement
LINE_MOVE_THRESH = 0.5     # Gate 4: max adverse line move (units)
ETL_MAX_HOURS = 4          # Gate 5: max data age


@dataclass
class GateResult:
    """Result from running all 5 gates on a signal."""
    passed: bool
    gate1_pyth: str    # GREEN | YELLOW | RED
    gate2_edge: str    # PASS | FAIL (edge pct)
    gate3_agree: str   # PASS | FAIL (model agreement)
    gate4_line: str    # PASS | FAIL (line movement)
    gate5_etl: str     # PASS | FAIL (data freshness)
    blocking_gate: Optional[int]  # First gate that failed, or None
    edge: float
    notes: str

    def to_dict(self):
        return {
            "passed": self.passed,
            "gate1_pyth": self.gate1_pyth,
            "gate2_edge": self.gate2_edge,
            "gate3_agree": self.gate3_agree,
            "gate4_line": self.gate4_line,
            "gate5_etl": self.gate5_etl,
            "blocking_gate": self.blocking_gate,
            "edge": round(self.edge, 4),
            "notes": self.notes,
        }


def run_gates(
    pyth_signal: str,
    model_prob: float,
    no_vig_prob: float,
    decomp_prob: Optional[float],
    opening_line: Optional[float],
    current_line: Optional[float],
    etl_timestamp: Optional[datetime],
) -> GateResult:
    """
    Run all 5 gates sequentially. Returns GateResult.

    Args:
        pyth_signal:    'GREEN' | 'YELLOW' | 'RED'
        model_prob:     Model win probability (0-1)
        no_vig_prob:    No-vig market probability (0-1)
        decomp_prob:    Decomposition model probability (0-1), or None to skip gate 3
        opening_line:   Opening line (decimal odds), or None to skip gate 4
        current_line:   Current line (decimal odds), or None to skip gate 4
        etl_timestamp:  UTC datetime of last ETL run, or None to skip gate 5

    Returns:
        GateResult with pass/fail per gate and overall pass flag
    """
    edge = model_prob - no_vig_prob
    g1 = g2 = g3 = g4 = g5 = "PASS"
    blocking = None
    notes_parts = []

    # Gate 1 — Pythagorean signal
    if pyth_signal == "RED":
        g1 = "RED"
        blocking = 1
        notes_parts.append("G1: Pythagorean RED — model divergence too large")
        return GateResult(False, g1, "SKIP", "SKIP", "SKIP", "SKIP", blocking, edge, "; ".join(notes_parts))
    elif pyth_signal == "YELLOW":
        g1 = "YELLOW"
        notes_parts.append("G1: YELLOW-cleared")
    else:
        g1 = "GREEN"

    # Gate 2 — Edge threshold
    if edge < MIN_EDGE_PCT:
        g2 = f"FAIL (edge={edge*100:.1f}% < {MIN_EDGE_PCT*100:.1f}%)"
        blocking = 2
        notes_parts.append(f"G2: Edge {edge*100:.1f}% below {MIN_EDGE_PCT*100:.0f}% min")
        return GateResult(False, g1, g2, "SKIP", "SKIP", "SKIP", blocking, edge, "; ".join(notes_parts))
    g2 = f"PASS (edge={edge*100:.1f}%)"

    # Gate 3 — Model agreement
    if decomp_prob is not None:
        disagree = abs(model_prob - decomp_prob)
        if disagree > MODEL_AGREE_THRESH:
            g3 = f"FAIL (disagree={disagree*100:.1f}% > {MODEL_AGREE_THRESH*100:.0f}%)"
            blocking = 3
            notes_parts.append(f"G3: Models disagree {disagree*100:.1f}%")
            return GateResult(False, g1, g2, g3, "SKIP", "SKIP", blocking, edge, "; ".join(notes_parts))
        g3 = f"PASS (disagree={disagree*100:.1f}%)"
    else:
        g3 = "SKIP (decomp_prob not provided)"

    # Gate 4 — Line movement
    if opening_line is not None and current_line is not None:
        move = current_line - opening_line
        # Sharp-direction = line moving against model (model likes away, line moves to home)
        if abs(move) > LINE_MOVE_THRESH:
            g4 = f"FAIL (move={move:+.2f} > {LINE_MOVE_THRESH} units)"
            blocking = 4
            notes_parts.append(f"G4: Adverse line move {move:+.2f}")
            return GateResult(False, g1, g2, g3, g4, "SKIP", blocking, edge, "; ".join(notes_parts))
        g4 = f"PASS (move={move:+.2f})"
    else:
        g4 = "SKIP (line data not provided)"

    # Gate 5 — ETL freshness
    if etl_timestamp is not None:
        now = datetime.now(timezone.utc)
        if etl_timestamp.tzinfo is None:
            etl_timestamp = etl_timestamp.replace(tzinfo=timezone.utc)
        age_hours = (now - etl_timestamp).total_seconds() / 3600
        if age_hours > ETL_MAX_HOURS:
            g5 = f"FAIL (age={age_hours:.1f}h > {ETL_MAX_HOURS}h)"
            blocking = 5
            notes_parts.append(f"G5: ETL data {age_hours:.1f}h old")
            return GateResult(False, g1, g2, g3, g4, g5, blocking, edge, "; ".join(notes_parts))
        g5 = f"PASS (age={age_hours:.1f}h)"
    else:
        g5 = "SKIP (etl_timestamp not provided)"

    notes_parts.append("All gates PASS")
    return GateResult(True, g1, g2, g3, g4, g5, None, edge, "; ".join(notes_parts))
