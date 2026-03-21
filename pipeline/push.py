"""
EyeBlackIQ Pipeline — push.py
One command to run all steps: scrape → model → export → grade → publish

Usage: python pipeline/push.py [--date YYYY-MM-DD] [--dry-run]
"""
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="EyeBlackIQ daily pipeline")
    parser.add_argument("--date", default=None, help="Run date YYYY-MM-DD (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="Print steps without executing")
    args = parser.parse_args()

    steps = [
        "scrapers/fetch_lines.py",
        "scrapers/fetch_odds.py",
        "pods/ncaa_baseball/model.py",
        "pods/nhl/model.py",
        "pods/soccer/model.py",
        "pods/mlb/model.py",
        "pipeline/export.py",
        "pipeline/grade.py",
    ]

    for step in steps:
        if args.dry_run:
            print(f"[DRY RUN] Would run: {step}")
        else:
            print(f"Running: {step}")
            # exec_step(step, date=args.date)

if __name__ == "__main__":
    main()
