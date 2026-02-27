from __future__ import annotations

import pandas as pd

from src.attribution import brinson_fachler_effects


def test_brinson_effects_reconcile_active() -> None:
    input_frame = pd.DataFrame(
        [
            {
                "month_end": "2026-01-31",
                "portfolio_id": "PF1",
                "benchmark_id": "BM1",
                "sector": "Tech",
                "w_p": 0.7,
                "w_b": 0.6,
                "r_p": 0.04,
                "r_b": 0.03,
            },
            {
                "month_end": "2026-01-31",
                "portfolio_id": "PF1",
                "benchmark_id": "BM1",
                "sector": "Energy",
                "w_p": 0.3,
                "w_b": 0.4,
                "r_p": 0.02,
                "r_b": 0.01,
            },
        ]
    )

    result = brinson_fachler_effects(input_frame)
    active_sum = result["active_effect"].sum()
    expected_active = (input_frame["w_p"] * input_frame["r_p"]).sum() - (input_frame["w_b"] * input_frame["r_b"]).sum()
    assert abs(active_sum - expected_active) < 1e-12
