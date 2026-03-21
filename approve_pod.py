"""
EyeBlackIQ — approve_pod.py
Approve a POD pick: flips PENDING_APPROVAL -> APPROVED, rebuilds site JSON,
and optionally pushes to GitHub Pages.

POD picks are ALWAYS included on the bet slip once the model generates them
(is_pod=1 in signals). Approval here is the human sign-off that also triggers
the site rebuild so the public page reflects the approved POD.

Usage (streamlined):
  python approve_pod.py --list             # Show pending PODs for today
  python approve_pod.py --sport NCAA       # Approve NCAA POD + auto-rebuild
  python approve_pod.py --sport MLB --push # Approve + rebuild + git push
  python approve_pod.py --all --push       # Approve all pending PODs + push
  python approve_pod.py --no-rebuild       # Approve only, skip rebuild (rare)
"""
import json
import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)-7s %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR      = Path(__file__).parent
SLIP_PATH     = BASE_DIR / "docs" / "data" / "today_slip.json"
APPROVALS_LOG = BASE_DIR / "results" / "pod_approvals.json"


def _load_json(path: Path) -> dict:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def list_pending_pods(slip: dict) -> list:
    """Return POD picks with status=PENDING_APPROVAL."""
    pods = slip.get("pod", [])
    return [p for p in pods if p.get("approval_status") in ("PENDING_APPROVAL", None, "PENDING")]


def approve_pod(date_str: str, sport: str) -> bool:
    """
    Flip approval_status from PENDING_APPROVAL to APPROVED for the given sport's POD.
    Logs to pod_approvals.json.
    Returns True if found and flipped.
    """
    slip = _load_json(SLIP_PATH)
    if not slip:
        logger.error(f"Slip not found at {SLIP_PATH}")
        return False

    found = False
    for pod in slip.get("pod", []):
        pod_sport = (pod.get("sport") or "").upper()
        if sport.upper() in pod_sport or pod_sport in sport.upper():
            old_status = pod.get("approval_status", "PENDING_APPROVAL")
            pod["approval_status"] = "APPROVED"
            pod["approved_at"]     = datetime.now(timezone.utc).isoformat()
            pod["approved_by"]     = "human"
            logger.info(f"Approved POD: {sport} — {pod.get('pick','?')} ({old_status} -> APPROVED)")
            found = True

    if not found:
        logger.warning(f"No POD found for sport={sport} in slip for {date_str}")
        return False

    _write_json(SLIP_PATH, slip)

    # Log to approvals
    log = _load_json(APPROVALS_LOG)
    if not isinstance(log, list):
        log = []
    log.append({
        "date":        date_str,
        "sport":       sport.upper(),
        "approved_at": datetime.now(timezone.utc).isoformat(),
        "slip_date":   slip.get("date", ""),
    })
    _write_json(APPROVALS_LOG, log)
    logger.info(f"Approval logged to {APPROVALS_LOG}")
    return True


def rebuild_site(date_str: str) -> bool:
    """Re-run export.py to refresh all JSON data files."""
    export_script = BASE_DIR / "pipeline" / "export.py"
    if not export_script.exists():
        logger.error(f"export.py not found at {export_script}")
        return False
    result = subprocess.run(
        [sys.executable, str(export_script), "--date", date_str],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"export.py failed:\n{result.stderr}")
        return False
    logger.info("Site data rebuilt successfully")
    return True


def git_push(commit_msg: str) -> bool:
    """Stage docs/data changes and push to GitHub Pages."""
    try:
        subprocess.run(["git", "-C", str(BASE_DIR), "add", "docs/data/"],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(BASE_DIR), "commit", "-m", commit_msg],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(BASE_DIR), "push"],
                       check=True, capture_output=True)
        logger.info("Pushed to GitHub Pages")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"git push failed: {e.stderr.decode() if e.stderr else e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Approve EyeBlackIQ POD pick")
    parser.add_argument("--date",       default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--sport",      default=None, help="NCAA | MLB | NHL | SOCCER | etc.")
    parser.add_argument("--all",        action="store_true", help="Approve ALL pending PODs for --date")
    parser.add_argument("--list",       action="store_true", help="List pending PODs")
    parser.add_argument("--push",       action="store_true", help="Rebuild + git push after approval")
    parser.add_argument("--no-rebuild", action="store_true", help="Skip rebuild (approval only, rare)")
    args = parser.parse_args()

    slip = _load_json(SLIP_PATH)

    if args.list or (not args.sport and not args.all):
        pending = list_pending_pods(slip)
        if not pending:
            print("No pending PODs found.")
        else:
            print(f"\nPending PODs ({len(pending)}):")
            for p in pending:
                print(f"  [{p.get('sport','?')}]  {p.get('pick','?')}  "
                      f"{p.get('odds','?')}  {p.get('units','?')}u  Edge={p.get('edge','?')}%")
        sys.exit(0)

    sports_to_approve = []
    if args.all:
        sports_to_approve = [p.get("sport") for p in list_pending_pods(slip)]
        if not sports_to_approve:
            print("No pending PODs to approve.")
            sys.exit(0)
    elif args.sport:
        sports_to_approve = [args.sport]

    approved_any = False
    for sport in sports_to_approve:
        ok = approve_pod(args.date, sport)
        if ok:
            print(f"APPROVED: {sport.upper()} POD")
            approved_any = True
        else:
            print(f"FAILED: {sport.upper()} — check logs")

    # Auto-rebuild after approval (skip only if --no-rebuild passed)
    if approved_any and not args.no_rebuild:
        rebuilt = rebuild_site(args.date)
        print("Site rebuilt." if rebuilt else "Rebuild FAILED — check logs")
        if args.push and rebuilt:
            pushed = git_push(f"approve: {'+'.join(sports_to_approve)} POD {args.date}")
            print("Pushed to GitHub Pages." if pushed else "Push FAILED — push manually")
