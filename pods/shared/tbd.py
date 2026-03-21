"""
EyeBlackIQ — tbd.py
Backtesting utilities and walk-forward validation helpers.

This module contains:
  - WalkForwardSplitter: generates train/test splits
  - CalibrationChecker: validates probability calibration (+-2% bins)
  - BacktestRunner: orchestrates walk-forward runs

Note: Full backtest harness lives in core/backtest_harness.py.
This module provides shared utilities for pod-level backtests.
"""
import logging
import numpy as np
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

SLIPPAGE_CENTS = 10  # 10 cents per simulated entry (spec requirement)
MIN_SEASONS    = 3   # Minimum seasons before trusting any team ML backtest
CAL_MAX_ERROR  = 0.02  # +-2% calibration tolerance


@dataclass
class BacktestResult:
    """Results from a single walk-forward backtest run."""
    sport: str
    model_version: str
    season_range: str
    n_bets: int
    wins: int
    losses: int
    units_net: float
    roi_pct: float
    clv_pct: Optional[float]
    cal_max_error: Optional[float]
    go_live_cleared: bool
    notes: str = ""
    calibration_bins: Dict = field(default_factory=dict)

    def __str__(self):
        status = "GO-LIVE CLEARED" if self.go_live_cleared else "NOT CLEARED"
        clv_str = f"{self.clv_pct:.1f}%" if self.clv_pct is not None else "N/A"
        cal_str = f"+-{self.cal_max_error:.1f}%" if self.cal_max_error is not None else "N/A"
        return (
            f"Backtest: {self.sport} {self.model_version} [{self.season_range}]\n"
            f"  Record: {self.wins}W-{self.losses}L ({self.n_bets} bets)\n"
            f"  ROI: {self.roi_pct:+.1f}%  CLV: {clv_str}  Cal: {cal_str}\n"
            f"  {status}"
        )


class WalkForwardSplitter:
    """
    Generates walk-forward train/test splits with no lookahead.

    Usage:
        splitter = WalkForwardSplitter(train_years=3, test_years=1)
        for train_dates, test_dates in splitter.splits(all_dates):
            ...
    """

    def __init__(self, train_years: int = 3, test_years: int = 1):
        self.train_years = train_years
        self.test_years  = test_years

    def splits(self, dates: List[str]) -> List[Tuple[List[str], List[str]]]:
        """
        Generate (train_dates, test_dates) pairs.
        All dates are strings 'YYYY-MM-DD', sorted ascending.
        """
        from datetime import datetime, timedelta
        dates = sorted(set(dates))
        if len(dates) < 10:
            return []

        results = []
        train_delta = timedelta(days=365 * self.train_years)
        test_delta  = timedelta(days=365 * self.test_years)

        first_dt = datetime.strptime(dates[0], "%Y-%m-%d")
        last_dt  = datetime.strptime(dates[-1], "%Y-%m-%d")

        # Walk forward year by year
        test_start = first_dt + train_delta
        while test_start + test_delta <= last_dt + timedelta(days=1):
            test_end = test_start + test_delta
            train_dates = [d for d in dates if datetime.strptime(d, "%Y-%m-%d") < test_start]
            test_dates  = [d for d in dates if test_start <= datetime.strptime(d, "%Y-%m-%d") < test_end]
            if train_dates and test_dates:
                results.append((train_dates, test_dates))
            test_start = test_end

        return results


class CalibrationChecker:
    """
    Validates probability calibration within +-2% across all bins.

    Usage:
        checker = CalibrationChecker(n_bins=10)
        report = checker.check(probs, outcomes)
        if report["max_error"] > CAL_MAX_ERROR:
            print("CALIBRATION FAILED")
    """

    def __init__(self, n_bins: int = 10):
        self.n_bins = n_bins

    def check(self, probs: List[float], outcomes: List[int]) -> Dict:
        """
        Args:
            probs:    List of model win probabilities (0-1)
            outcomes: List of 1 (win) or 0 (loss)

        Returns:
            {
                "max_error": float,   # max abs(predicted - actual) across bins
                "passed": bool,       # True if all bins within +-2%
                "bins": [...],        # Per-bin detail
                "n_total": int
            }
        """
        if len(probs) != len(outcomes):
            raise ValueError("probs and outcomes must be same length")

        probs_arr    = np.array(probs)
        outcomes_arr = np.array(outcomes)

        bins = np.linspace(0, 1, self.n_bins + 1)
        bin_results = []
        max_error = 0.0

        for i in range(len(bins) - 1):
            lo, hi = bins[i], bins[i+1]
            mask = (probs_arr >= lo) & (probs_arr < hi)
            if i == len(bins) - 2:
                mask = (probs_arr >= lo) & (probs_arr <= hi)  # include 1.0 in last bin

            n = mask.sum()
            if n < 5:
                bin_results.append({"bin": f"{lo:.1f}-{hi:.1f}", "n": int(n), "skipped": True})
                continue

            pred_mean = probs_arr[mask].mean()
            actual_rate = outcomes_arr[mask].mean()
            error = abs(pred_mean - actual_rate)
            max_error = max(max_error, error)

            bin_results.append({
                "bin": f"{lo:.1f}-{hi:.1f}",
                "n": int(n),
                "pred_mean": round(float(pred_mean), 3),
                "actual_rate": round(float(actual_rate), 3),
                "error": round(float(error), 3),
                "passed": error <= CAL_MAX_ERROR,
            })

        return {
            "max_error": round(float(max_error), 4),
            "passed": max_error <= CAL_MAX_ERROR,
            "bins": bin_results,
            "n_total": len(probs),
        }
