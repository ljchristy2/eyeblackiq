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

# Edge window — active (v2: wider caps per performance data)
# MIN_EDGE       = 0.03   (< 3%  → models set units=0, caught below as flagged_low)
# MAX_EDGE_TEAM  = 0.20   (> 20% on ML/total → likely stale line or model artifact)
# MAX_EDGE_PROP  = 0.30   (> 30% on props → props market is thin, higher real edges exist)
# PODs bypass the cap — human-reviewed, highest conviction, always on the slip.
MIN_EDGE      = 0.03
MAX_EDGE_TEAM = 0.20
MAX_EDGE_PROP = 0.30


def edge_window(edge, units=None, bet_type=None, is_pod=False):
    """
    Returns 'recommended', 'flagged_low', or 'flagged_high'.

    PODs always return 'recommended' — human-approved, cap does not apply.
    Props cap: 20%. Team ML/total cap: 15%. Below 3% (units=0): flagged_low.
    """
    if is_pod:
        return "recommended"
    if units is not None and units == 0:
        return "flagged_low"
    if edge == 0 and not is_pod:
        return "flagged_low"
    cap = MAX_EDGE_PROP if '_PROP' in (bet_type or '').upper() or (bet_type or '').upper() in ("PROP", "PROPS") else MAX_EDGE_TEAM
    if edge > cap:
        return "flagged_high"
    return "recommended"


def get_conn():
    return sqlite3.connect(DB_PATH)


def parse_conf_from_notes(notes: str) -> tuple:
    """
    Extract confidence level and symbol from the notes string.
    Returns (label, symbol) e.g. ('HIGH', '●●●')
    """
    if not notes:
        return ("MED", "●●○")
    if "HIGH" in notes:
        return ("HIGH", "●●●")
    elif "LOW" in notes:
        return ("LOW", "●○○")
    return ("MED", "●●○")


def parse_rl_alt(notes: str) -> bool:
    """Returns True if signal has a run-line alternative flag in notes."""
    return "RL_ALT" in (notes or "")


def export_today_slip(date_str: str) -> dict:
    """
    Reads signals for date_str from DB, returns slip dict.
    Splits into recommended and flagged.
    PODs sourced from signals.is_pod=1 (HIGH confidence + WHEELHOUSE+ tier).
    Sorted: PODs first, then by units DESC, edge DESC.
    """
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        # Include today's signals + upcoming signals (future game dates, created within 7 days)
        # This supports forward-looking picks (handball/cricket future games, soccer next-day games)
        cur = conn.execute(
            """SELECT id, sport, game, game_time, bet_type, side, market,
                      odds, model_prob, no_vig_prob, edge, ev, tier, units,
                      is_pod, pod_sport, notes,
                      gate1_pyth, gate2_edge, gate3_model_agree,
                      gate4_line_move, gate5_etl_fresh,
                      COALESCE(pick_source, 'SPORTSBOOK') as pick_source,
                      b2b_flag, signal_date
               FROM signals
               WHERE (
                   signal_date = ?
                   OR (signal_date > ? AND created_at >= datetime(?, '-7 days')
                       AND id NOT IN (SELECT signal_id FROM results WHERE result NOT IN ('PENDING','VOID')))
               )
               ORDER BY signal_date ASC, is_pod DESC, game_time ASC NULLS LAST, units DESC, edge DESC""",
            (date_str, date_str, date_str)
        )
        rows = [dict(r) for r in cur.fetchall()]

    # Strip silently-tracked signals (NHL PTS/AST) — stored in DB for calibration
    # but should never appear on the public bet slip
    rows = [r for r in rows if "[SILENT]" not in (r.get("notes") or "")]

    recommended = []
    flagged     = []
    pod_summary = []

    for row in rows:
        edge_val  = row.get("edge") or 0
        units_val = row.get("units") or 0
        bet_type  = row.get("bet_type") or ""
        is_pod    = bool(row.get("is_pod"))
        status    = edge_window(edge_val, units=units_val, bet_type=bet_type, is_pod=is_pod)
        row["edge_status"] = status
        row["edge_pct"]    = round(edge_val * 100, 2)

        # Parse confidence and RL alt from notes
        conf_label, conf_sym = parse_conf_from_notes(row.get("notes", ""))
        row["conf_label"] = conf_label
        row["conf_sym"]   = conf_sym
        row["rl_alt"]     = parse_rl_alt(row.get("notes", ""))

        if status == "recommended":
            recommended.append(row)
            # Build POD summary from is_pod=1 signals
            if is_pod:
                pod_summary.append({
                    "sport":       row["sport"],
                    "pick":        row["side"],
                    "odds":        row["odds"],
                    "tier":        row["tier"],
                    "units":       row["units"],
                    "game":        row["game"],
                    "game_time":   row["game_time"],
                    "model_prob":  row["model_prob"],
                    "edge":        row["edge_pct"],
                    "ev":          row["ev"],
                    "conf_label":  conf_label,
                    "conf_sym":    conf_sym,
                    "result":      "PENDING",
                    "bet_type":    row.get("bet_type") or "",
                    "pick_source": row.get("pick_source") or "SPORTSBOOK",
                })
        else:
            if status == "flagged_high":
                cap_pct = MAX_EDGE_PROP * 100 if '_PROP' in bet_type.upper() or bet_type.upper() in ("PROP", "PROPS") else MAX_EDGE_TEAM * 100
                row["flag_reason"] = f"Edge {row['edge_pct']:.1f}% exceeds {cap_pct:.0f}% cap — verify line before betting"
            else:
                row["flag_reason"] = "Below minimum tier threshold (0 units)"
            flagged.append(row)

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

    # POD record — read from results+signals join (pod_records table is for pod_approval flow)
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT
                   SUM(CASE WHEN r.result='WIN'  THEN 1 ELSE 0 END),
                   SUM(CASE WHEN r.result='LOSS' THEN 1 ELSE 0 END)
               FROM results r
               JOIN signals s ON s.id = r.signal_id
               WHERE s.is_pod = 1 AND r.result IN ('WIN','LOSS')"""
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

    def _build_streak(rows):
        """Build current streak string from a list of 'WIN'/'LOSS' strings (newest first)."""
        n, t = 0, None
        for r in rows:
            if t is None: t = r
            if r == t: n += 1
            else: break
        if n == 0: return "—"
        return f"{'W' if t == 'WIN' else 'L'}{n}"

    # POD streak
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT r.result FROM results r
               JOIN signals s ON s.id = r.signal_id
               WHERE s.is_pod = 1 AND r.result IN ('WIN','LOSS')
               ORDER BY r.signal_date DESC, r.id DESC LIMIT 20"""
        )
        pod_streak_rows = [r[0] for r in cur.fetchall()]
    pod_streak = _build_streak(pod_streak_rows)

    # Winning/losing day streak
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT signal_date,
                      SUM(CASE WHEN result='WIN'  THEN 1 ELSE 0 END) as w,
                      SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l
               FROM results WHERE result IN ('WIN','LOSS')
               GROUP BY signal_date ORDER BY signal_date DESC LIMIT 14"""
        )
        day_rows_raw = cur.fetchall()

    day_streak = "—"
    if day_rows_raw:
        ds_type = None
        ds_n = 0
        for _, dw, dl in day_rows_raw:
            dtype = "WIN" if (dw or 0) > 0 and (dl or 0) == 0 else "LOSS" if (dl or 0) > 0 and (dw or 0) == 0 else "MIX"
            if dtype == "MIX":
                break
            if ds_type is None: ds_type = dtype
            if dtype == ds_type: ds_n += 1
            else: break
        if ds_n > 0:
            day_streak = f"{'W' if ds_type == 'WIN' else 'L'}{ds_n}"

    # Per-sport streaks
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT sport, result FROM results
               WHERE result IN ('WIN','LOSS')
               ORDER BY sport, signal_date DESC, id DESC"""
        )
        sport_results_raw = {}
        for sp, res in cur.fetchall():
            sport_results_raw.setdefault(sp, []).append(res)
    sport_streaks = {sp: _build_streak(rows) for sp, rows in sport_results_raw.items()}

    # Per-bet-type streaks
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT s.bet_type, r.result FROM results r
               JOIN signals s ON s.id = r.signal_id
               WHERE r.result IN ('WIN','LOSS') AND s.bet_type IS NOT NULL
               ORDER BY s.bet_type, r.signal_date DESC, r.id DESC"""
        )
        type_results_raw = {}
        for bt, res in cur.fetchall():
            type_results_raw.setdefault(bt, []).append(res)
    type_streaks = {bt: _build_streak(rows) for bt, rows in type_results_raw.items()}

    return {
        "wins":          wins or 0,
        "losses":        losses or 0,
        "pushes":        pushes or 0,
        "net_units":     net_units,
        "roi":           roi,
        "clv_pct":       clv_pct,
        "pod_wins":      pod_wins,
        "pod_losses":    pod_losses,
        "streak":        streak_str,
        "pod_streak":    pod_streak,
        "day_streak":    day_streak,
        "sport_streaks": sport_streaks,
        "type_streaks":  type_streaks,
        "last_updated":  datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


TIER_COLORS = {
    "FILTHY":       "#DC143C",
    "SNIPE":        "#DC143C",
    "UPPER 90":     "#DC143C",
    "SCREAMER":     "#DC143C",
    "WHEELHOUSE":   "#4A90D9",
    "SLOT MACHINE": "#4A90D9",
    "SLOT_MACHINE": "#4A90D9",
    "CHEEKY":       "#4A90D9",
    "FAST BREAK":   "#4A90D9",
    "SCOUT":        "#B8960C",
    "MONITOR":      "#B8960C",
    "CONVICTION":   "#B8960C",
    "ON_RADAR":     "#555555",
    "ON RADAR":     "#555555",
}


def tier_color(tier: str) -> str:
    """Returns hex color for a tier name."""
    if not tier:
        return "#555555"
    t = tier.upper().replace("🟡 ", "").replace("🔴 ", "").strip()
    for k, v in TIER_COLORS.items():
        if k in t:
            return v
    return "#555555"


def clean_tier(tier: str) -> str:
    """Remove emoji prefixes from tier names."""
    if not tier:
        return tier
    return tier.replace("🟡 ", "").replace("🔴 ", "").replace("🟢 ", "").strip()


def export_results(limit: int = 200) -> list:
    """Returns graded results (all recommended picks, units > 0), including loss analysis data."""
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT r.signal_date, r.sport, r.game, r.side, r.market,
                      r.odds, r.units, r.result, r.units_net,
                      r.actual_val, r.closing_line, r.clv, r.graded_at,
                      s.notes, s.tier, s.ev, s.is_pod, s.pod_sport, s.edge
               FROM results r
               LEFT JOIN signals s ON s.id = r.signal_id
               WHERE r.result IN ('WIN','LOSS','PUSH','VOID')
                 AND (s.units IS NULL OR s.units > 0)
               ORDER BY r.signal_date DESC, r.id DESC
               LIMIT ?""",
            (limit,)
        )
        rows = [dict(r) for r in cur.fetchall()]

    for r in rows:
        r["tier_color"] = tier_color(r.get("tier") or "")
        r["tier"] = clean_tier(r.get("tier") or "")
        if r.get("result") in ("LOSS", "L"):
            parts = []
            actual = r.get("actual_val")
            line   = r.get("closing_line")
            if actual is not None and line is not None:
                try:
                    miss = float(actual) - float(line)
                    direction = f"over by {abs(miss):.1f}" if miss > 0 else f"short by {abs(miss):.1f}"
                    parts.append(f"Miss margin: {direction} (needed {line}, got {actual})")
                except (TypeError, ValueError):
                    pass
            if r.get("clv") is not None:
                clv_pct = r["clv"] * 100
                parts.append(f"CLV: {'+' if clv_pct >= 0 else ''}{clv_pct:.1f}%")
            if r.get("notes"):
                note = (r["notes"] or "").split("  Conf=")[0]
                if note:
                    parts.append(note)
            r["loss_analysis"] = "  ·  ".join(parts) if parts else None

        # Variance verdict for all results (WIN gets None, LOSS gets assessment)
        if r.get("result") in ("LOSS", "L"):
            edge_val = r.get("edge") or 0
            clv = r.get("clv")
            if clv is not None:
                if clv > 0:
                    r["variance_verdict"] = "VARIANCE"
                    r["variance_note"]    = f"Beat closing line (CLV +{clv*100:.1f}%) — good process, variance loss"
                else:
                    r["variance_verdict"] = "REVIEW"
                    r["variance_note"]    = f"Line moved against model (CLV {clv*100:.1f}%) — review process"
            elif edge_val >= 0.10:
                r["variance_verdict"] = "VARIANCE"
                r["variance_note"]    = f"High-edge signal ({edge_val*100:.1f}%) — statistically expected to lose sometimes"
            elif edge_val >= 0.05:
                r["variance_verdict"] = "UNCLEAR"
                r["variance_note"]    = "Moderate edge, no CLV data — cannot fully assess"
            else:
                r["variance_verdict"] = "REVIEW"
                r["variance_note"]    = "Low edge, no CLV data — worth reviewing"
        else:
            r["variance_verdict"] = None
            r["variance_note"]    = None

    return rows


def export_daily_summaries() -> list:
    """Builds per-date running record from results table."""
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT signal_date,
                      SUM(CASE WHEN result='WIN'  THEN 1 ELSE 0 END) as day_w,
                      SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as day_l,
                      SUM(CASE WHEN result='PUSH' THEN 1 ELSE 0 END) as day_p,
                      ROUND(SUM(units_net), 3) as day_net,
                      SUM(units) as day_units_risked
               FROM results
               WHERE result IN ('WIN','LOSS','PUSH')
               GROUP BY signal_date
               ORDER BY signal_date ASC"""
        )
        day_rows = cur.fetchall()

    summaries = []
    season_w = season_l = season_p = 0
    season_net = 0.0
    season_units = 0.0
    streak_type = None
    streak_n = 0

    for row in day_rows:
        date, dw, dl, dp, dn, du = row
        season_w += dw or 0
        season_l += dl or 0
        season_p += dp or 0
        season_net += dn or 0
        season_units += du or 0
        roi = round((season_net / season_units * 100) if season_units > 0 else 0, 1)

        # Streak logic
        if dw > 0 and dl == 0:
            day_result = "W"
        elif dl > 0 and dw == 0:
            day_result = "L"
        else:
            day_result = "M"  # mixed day

        if streak_type is None or day_result == streak_type:
            streak_type = day_result
            streak_n += 1
        else:
            streak_type = day_result
            streak_n = 1

        streak_str = f"{streak_type}{streak_n}" if streak_type in ("W", "L") else f"M{streak_n}"

        summaries.append({
            "date":       date,
            "day_w":      dw or 0,
            "day_l":      dl or 0,
            "day_p":      dp or 0,
            "day_net":    round(dn or 0, 3),
            "season_w":   season_w,
            "season_l":   season_l,
            "season_p":   season_p,
            "season_net": round(season_net, 3),
            "season_roi": roi,
            "streak":     streak_str,
        })
    return summaries


def export_full_market_view(limit: int = 500) -> list:
    """
    Returns ALL graded picks (recommended + flagged) for the Full Market View tab.
    Includes units=0 (flagged) picks that are excluded from main results display.
    """
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT r.signal_date as date, r.sport, r.side as pick, r.market,
                      r.odds as line, r.units, r.result as status, r.units_net,
                      r.actual_val, s.tier, s.ev, s.edge, s.notes, s.is_pod,
                      COALESCE(s.pick_source, 'SPORTSBOOK') as pick_source,
                      s.b2b_flag
               FROM results r
               LEFT JOIN signals s ON s.id = r.signal_id
               WHERE r.result IN ('WIN','LOSS','PUSH','VOID')
               ORDER BY r.signal_date DESC, r.id DESC
               LIMIT ?""",
            (limit,)
        )
        rows = [dict(r) for r in cur.fetchall()]

    for r in rows:
        edge_val      = r.get("edge") or 0
        units_val     = r.get("units") or 0
        bet_type      = r.get("market") or ""
        is_pod        = bool(r.get("is_pod"))
        fmv_status    = edge_window(edge_val, units=units_val, bet_type=bet_type, is_pod=is_pod)
        r["is_flagged"]  = fmv_status != "recommended"
        r["flag_status"] = fmv_status   # 'recommended' | 'flagged_low' | 'flagged_high'
        r["tier_color"]  = tier_color(r.get("tier") or "")
        r["tier"]        = clean_tier(r.get("tier") or "")
        r["edge_pct"]    = round(edge_val * 100, 2)
        ev_val           = r.get("ev") or 0
        r["ev_pct"]      = round(ev_val * 100, 2)
        r.pop("edge", None)
        r.pop("ev", None)
    return rows


def export_pod_picks(limit: int = 100) -> list:
    """Returns all graded POD picks (WIN/LOSS/PUSH) plus any still PENDING."""
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT r.signal_date as date, r.sport, r.side as pick,
                      r.odds as line, r.units, r.result, r.units_net as net_units,
                      r.actual_val, s.tier, s.ev, s.edge, s.game, s.game_time
               FROM results r
               JOIN signals s ON s.id = r.signal_id
               WHERE s.is_pod = 1
               ORDER BY r.signal_date DESC
               LIMIT ?""",
            (limit,)
        )
        rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r["tier"]     = clean_tier(r.get("tier") or "")
        r["edge_pct"] = round((r.pop("edge") or 0) * 100, 2)
        ev = r.pop("ev") or 0
        r["ev_pct"]   = round(ev * 100, 2)
    return rows


def write_json(path: Path, data):
    """Write JSON with pretty print."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Wrote {path.name}  ({path.stat().st_size:,} bytes)")


def run_export(date_str: str):
    """Main export — writes all three JSON files."""
    DOCS_DATA.mkdir(parents=True, exist_ok=True)

    slip       = export_today_slip(date_str)
    record     = export_record()
    picks      = export_results(200)
    summaries  = export_daily_summaries()
    pod_picks  = export_pod_picks(100)
    fmv        = export_full_market_view(500)

    write_json(DOCS_DATA / "today_slip.json", slip)
    write_json(DOCS_DATA / "record.json",     record)
    write_json(DOCS_DATA / "results.json",    {
        "picks":             picks,
        "daily_summaries":   summaries,
        "pod_picks":         pod_picks,
        "full_market_view":  fmv,
    })

    logger.info(
        f"Export complete — {slip['counts']['recommended']} recommended, "
        f"{slip['counts']['flagged']} flagged, {slip['counts']['pods']} PODs | "
        f"{len(picks)} total graded picks, {len(summaries)} day summaries"
    )
    return slip, record, picks


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export eyeblackiq signals to JSON")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="Date YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    run_export(args.date)
