from __future__ import annotations

from datetime import date

import pandas as pd

from src.dietz import modified_dietz


def test_modified_dietz_no_flows_matches_simple_return() -> None:
    result = modified_dietz(
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        begin_value=100.0,
        end_value=110.0,
        flows=pd.DataFrame(columns=["flow_date", "amount"]),
        end_of_day=True,
    )
    assert round(result.period_return, 8) == 0.1


def test_modified_dietz_end_day_flow_has_zero_weight() -> None:
    flows = pd.DataFrame([{"flow_date": date(2026, 1, 31), "amount": 20.0}])
    result = modified_dietz(
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        begin_value=100.0,
        end_value=120.0,
        flows=flows,
        end_of_day=True,
    )
    assert round(result.period_return, 8) == 0.0
