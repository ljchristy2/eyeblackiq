"""
EyeBlackIQ — push.py
One command runs the full daily pipeline:
  1. fetch_lines.py     — TheRundown API game lines
  2. fetch_odds.py      — OddsAPI prop odds
  3. Pod models         — generate signals per sport
  4. export.py          — write signals to docs/data/ JSON
  5. Git commit + push  — update eyeblackiq.github.io live

Usage:
  python pipeline/push.py                     # Run today
  python pipeline/push.py --date 2026-03-26   # Run specific date
  python pipeline/push.py --dry-run           # Print steps without executing
  python pipeline/push.py --skip-fetch        # Skip API calls (use cached data)
  python pipeline/push.py --skip-push         # Run everything except git push
"""
import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# Load .env from eyeblackiq root OR from the main quant-betting worktree
_ROOT = Path(__file__).parent.parent
_ENV_PATHS = [
    _ROOT / ".env",
    _ROOT.parent.parent / "quant-betting" / "soccer" / ".claude" / "worktrees" / "admiring-allen" / ".env",
]
for _ep in _ENV_PATHS:
    if _ep.exists():
        load_dotenv(_ep)
        break

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DIR  = _ROOT
DOCS_DATA = BASE_DIR / "docs" / "data"
GH_TOKEN  = os.getenv("GH_TOKEN", "")
REPO_URL  = "https://github.com/eyeblackiq/eyeblackiq.github.io.git"


# ── Step runner ───────────────────────────────────────────────────────────────

def run_step(label: str, cmd: list, dry_run: bool = False, cwd: Path = None) -> bool:
    """Run a pipeline step. Returns True on success."""
    cwd = cwd or BASE_DIR
    if dry_run:
        logger.info(f"[DRY RUN]  {label}")
        logger.info(f"           {' '.join(cmd)}")
        return True

    logger.info(f"▶  {label}")
    result = subprocess.run(cmd, cwd=str(cwd), capture_output=False, text=True)
    if result.returncode != 0:
        logger.error(f"✗  {label} failed (exit {result.returncode})")
        return False
    logger.info(f"✓  {label}")
    return True


def run_python(label: str, script: str, args: list = None, dry_run: bool = False) -> bool:
    """Run a Python script within the repo."""
    cmd = [sys.executable, script] + (args or [])
    return run_step(label, cmd, dry_run=dry_run)


# ── Git push ──────────────────────────────────────────────────────────────────

def git_push_docs(date_str: str, dry_run: bool = False) -> bool:
    """Stage docs/data/, commit, and push to origin/main."""
    if dry_run:
        logger.info("[DRY RUN]  git add + commit + push docs/data/")
        return True

    # Stage only docs/data/ — never stage .env or *.db
    cmds = [
        (["git", "add", "docs/data/"],                     "git add docs/data/"),
        (["git", "commit", "-m", f"data: daily slip {date_str}"], "git commit"),
        (["git", "push",
          f"https://eyeblackiq:{GH_TOKEN}@github.com/eyeblackiq/eyeblackiq.github.io.git",
          "main"],                                          "git push"),
    ]
    for cmd, label in cmds:
        result = subprocess.run(cmd, cwd=str(BASE_DIR), capture_output=True, text=True)
        if result.returncode != 0:
            # "nothing to commit" is OK
            if "nothing to commit" in result.stdout + result.stderr:
                logger.info(f"  {label}: nothing to commit — skipped")
                continue
            logger.warning(f"  {label} warning: {result.stderr.strip()[:200]}")
        else:
            logger.info(f"✓  {label}")
    return True


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(date_str: str, dry_run: bool = False,
                 skip_fetch: bool = False, skip_push: bool = False):
    """Execute the full daily pipeline."""
    start = datetime.now(timezone.utc)
    logger.info(f"{'='*55}")
    logger.info(f"  EyeBlackIQ Pipeline  —  {date_str}")
    logger.info(f"  {'DRY RUN  ' if dry_run else ''}Started: {start.strftime('%H:%M:%S UTC')}")
    logger.info(f"{'='*55}")

    steps_ok = []

    # ── Step 1: Fetch game lines ──────────────────────────────
    if not skip_fetch:
        ok = run_python(
            "Fetch game lines (TheRundown)",
            "scrapers/fetch_lines.py",
            ["--date", date_str, "--sport", "all"],
            dry_run=dry_run,
        )
        steps_ok.append(("fetch_lines", ok))
    else:
        logger.info("  [SKIP] fetch_lines (--skip-fetch)")

    # ── Step 2: Fetch prop odds ───────────────────────────────
    if not skip_fetch:
        ok = run_python(
            "Fetch prop odds (OddsAPI)",
            "scrapers/fetch_odds.py",
            ["--date", date_str, "--sport", "all"],
            dry_run=dry_run,
        )
        steps_ok.append(("fetch_odds", ok))
    else:
        logger.info("  [SKIP] fetch_odds (--skip-fetch)")

    # ── Step 3: Run pod models ────────────────────────────────
    pod_scripts = [
        ("pods/ncaa_baseball/model.py", "NCAA Baseball model"),
        ("pods/nhl/model.py",           "NHL model"),
        ("pods/mlb/model.py",           "MLB model"),
        ("pods/soccer/model.py",        "Soccer model"),
    ]
    for script, label in pod_scripts:
        script_path = BASE_DIR / script
        if script_path.exists():
            ok = run_python(label, script, ["--date", date_str], dry_run=dry_run)
            steps_ok.append((label, ok))
        else:
            logger.info(f"  [PENDING] {label} — model not yet built")

    # ── Step 4: Export to JSON ────────────────────────────────
    ok = run_python(
        "Export signals → docs/data/ JSON",
        "pipeline/export.py",
        ["--date", date_str],
        dry_run=dry_run,
    )
    steps_ok.append(("export", ok))

    # ── Step 5: Git push ──────────────────────────────────────
    if not skip_push:
        if GH_TOKEN:
            ok = git_push_docs(date_str, dry_run=dry_run)
            steps_ok.append(("git_push", ok))
        else:
            logger.warning("  [SKIP] git push — GH_TOKEN not set")
    else:
        logger.info("  [SKIP] git push (--skip-push)")

    # ── Summary ───────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    passed  = sum(1 for _, ok in steps_ok if ok)
    total   = len(steps_ok)
    logger.info(f"{'='*55}")
    logger.info(f"  Pipeline complete — {passed}/{total} steps OK  ({elapsed:.1f}s)")
    for name, ok in steps_ok:
        status = "✓" if ok else "✗"
        logger.info(f"  {status}  {name}")
    logger.info(f"{'='*55}")
    return passed == total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EyeBlackIQ daily pipeline")
    parser.add_argument("--date",       default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    parser.add_argument("--dry-run",    action="store_true", help="Print steps without executing")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip API fetches (use cached)")
    parser.add_argument("--skip-push",  action="store_true", help="Skip git push")
    args = parser.parse_args()

    success = run_pipeline(
        date_str=args.date,
        dry_run=args.dry_run,
        skip_fetch=args.skip_fetch,
        skip_push=args.skip_push,
    )
    sys.exit(0 if success else 1)
