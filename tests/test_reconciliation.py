from __future__ import annotations

import pandas as pd

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
