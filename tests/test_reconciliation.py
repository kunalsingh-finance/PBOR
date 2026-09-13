from __future__ import annotations

import pandas as pd
import pytest

from src.reconciliation import attribution_reconciliation, latest_reconciliation


def test_attribution_reconciliation_flags_diff() -> None:
    monthly = pd.DataFrame(
        [
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "active_return": 0.0493,
                "portfolio_return_twr": 0.08,
            },
        ]
    )
    attribution = pd.DataFrame(
        [
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "active_effect": 0.303,
                "w_p": 0.7,
                "w_b": 0.6,
                "r_p": 0.1,
                "r_b": 0.02,
            },
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "active_effect": -0.0379,
                "w_p": 0.3,
                "w_b": 0.4,
                "r_p": -0.01,
                "r_b": 0.01,
            },
        ]
    )
    recon = attribution_reconciliation(monthly, attribution, tolerance_bps=5.0)
    assert len(recon) == 1
    assert bool(recon.iloc[0]["within_tolerance"]) is False
    assert float(recon.iloc[0]["diff_bps"]) > 5.0


def test_latest_reconciliation_pass() -> None:
    monthly = pd.DataFrame(
        [
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "active_return": 0.01,
                "portfolio_return_twr": 0.1,
            },
        ]
    )
    attribution = pd.DataFrame(
        [
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "active_effect": 0.006,
                "w_p": 0.6,
                "w_b": 0.6,
                "r_p": 0.1,
                "r_b": 0.09,
            },
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "active_effect": 0.004,
                "w_p": 0.4,
                "w_b": 0.4,
                "r_p": 0.1,
                "r_b": 0.09,
            },
        ]
    )
    latest = latest_reconciliation(monthly, attribution, tolerance_bps=5.0)
    assert latest["available"] is True
    assert latest["within_tolerance"] is True


def _two_portfolio_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    monthly = pd.DataFrame([
        {"month_end": "2026-01-31", "portfolio_id": portfolio, "active_return": 0.01,
         "portfolio_return_twr": 0.1}
        for portfolio in ["PF_FAIL", "PF_PASS"]
    ])
    attribution = pd.DataFrame([
        {"month_end": "2026-01-31", "portfolio_id": "PF_FAIL", "active_effect": 0.02,
         "w_p": 1.0, "w_b": 1.0, "r_p": 0.1},
        {"month_end": "2026-01-31", "portfolio_id": "PF_PASS", "active_effect": 0.01,
         "w_p": 1.0, "w_b": 1.0, "r_p": 0.1},
    ])
    return monthly, attribution


def test_latest_reconciliation_cannot_hide_another_portfolios_failure() -> None:
    monthly, attribution = _two_portfolio_frames()
    for ordered in [monthly, monthly.iloc[::-1]]:
        result = latest_reconciliation(ordered, attribution, 5.0)
        assert result["within_tolerance"] is False
        assert result["portfolio_id"] == "PF_FAIL"
        assert result["portfolio_count"] == 2
        assert result["failed_portfolio_ids"] == ["PF_FAIL"]


def test_latest_reconciliation_uses_latest_period_for_each_portfolio() -> None:
    monthly, attribution = _two_portfolio_frames()
    older = monthly.iloc[[1]].assign(month_end="2025-12-31", active_return=0.9)
    result = latest_reconciliation(pd.concat([monthly, older]), attribution, 5.0)
    assert result["failed_portfolio_count"] == 1


@pytest.mark.parametrize("missing_column", ["active_effect", "r_p", "w_p", "w_b"])
def test_nonfinite_attribution_data_cannot_pass_as_zero(missing_column: str) -> None:
    monthly, attribution = _two_portfolio_frames()
    monthly[["active_return", "portfolio_return_twr"]] = 0.0
    attribution[["active_effect", "r_p"]] = 0.0
    attribution.loc[0, missing_column] = float("nan")
    result = attribution_reconciliation(monthly, attribution, 5.0)
    assert not bool(result.loc[0, "data_complete"])
    assert not bool(result.loc[0, "within_tolerance"])


def test_exact_reconciliation_passes_at_zero_tolerance() -> None:
    monthly, attribution = _two_portfolio_frames()
    result = latest_reconciliation(monthly.iloc[[1]], attribution.iloc[[1]], 0.0)
    assert result["within_tolerance"] is True
