from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.build_recon_demo_data import build_recon_demo_data
from src.auto_recon import reconcile_cash, reconcile_positions, run_auto_recon


POLICY = {
    "position_recon": {
        "quantity_tolerance": 0,
        "price_tolerance_pct": 0.0005,
        "market_value_tolerance": 100,
        "cash_tolerance": 50,
    }
}


def _position(security_id: str, quantity: float, price: float, market_value: float | None = None) -> dict[str, object]:
    return {
        "asof_date": "2026-01-10",
        "portfolio_id": "PF_TEST",
        "account_id": "ACC_TEST",
        "security_id": security_id,
        "ticker": security_id.replace("SEC_", ""),
        "quantity": quantity,
        "price": price,
        "market_value_base": market_value if market_value is not None else quantity * price,
    }


def _cash(currency: str, balance: float) -> dict[str, object]:
    return {
        "asof_date": "2026-01-10",
        "portfolio_id": "PF_TEST",
        "account_id": "ACC_TEST",
        "currency": currency,
        "cash_balance": balance,
    }


def test_exact_matched_position_returns_matched() -> None:
    internal = pd.DataFrame([_position("SEC_SPY", 100, 10)])
    external = pd.DataFrame([_position("SEC_SPY", 100, 10)])

    result = reconcile_positions(internal, external, POLICY)

    assert result.loc[0, "status"] == "MATCHED"


def test_quantity_difference_returns_quantity_break() -> None:
    internal = pd.DataFrame([_position("SEC_SPY", 100, 10)])
    external = pd.DataFrame([_position("SEC_SPY", 101, 10)])

    result = reconcile_positions(internal, external, POLICY)

    assert result.loc[0, "status"] == "QUANTITY_BREAK"


def test_price_difference_returns_price_break() -> None:
    internal = pd.DataFrame([_position("SEC_AAPL", 100, 100.00)])
    external = pd.DataFrame([_position("SEC_AAPL", 100, 99.90)])

    result = reconcile_positions(internal, external, POLICY)

    assert result.loc[0, "status"] == "PRICE_BREAK"


def test_market_value_difference_returns_market_value_break() -> None:
    internal = pd.DataFrame([_position("SEC_JPM", 100, 10, 1000)])
    external = pd.DataFrame([_position("SEC_JPM", 100, 10, 1200)])

    result = reconcile_positions(internal, external, POLICY)

    assert result.loc[0, "status"] == "MARKET_VALUE_BREAK"


def test_missing_custodian_row_returns_missing_in_custodian() -> None:
    internal = pd.DataFrame([_position("SEC_MSFT", 100, 10)])
    external = pd.DataFrame(columns=internal.columns)

    result = reconcile_positions(internal, external, POLICY)

    assert result.loc[0, "status"] == "MISSING_IN_CUSTODIAN"


def test_missing_internal_row_returns_missing_in_internal() -> None:
    internal = pd.DataFrame(columns=list(_position("SEC_GOOGL", 100, 10).keys()))
    external = pd.DataFrame([_position("SEC_GOOGL", 100, 10)])

    result = reconcile_positions(internal, external, POLICY)

    assert result.loc[0, "status"] == "MISSING_IN_INTERNAL"


def test_exact_cash_match_returns_matched() -> None:
    internal = pd.DataFrame([_cash("USD", 1000)])
    external = pd.DataFrame([_cash("USD", 1000)])

    result = reconcile_cash(internal, external, POLICY)

    assert result.loc[0, "status"] == "MATCHED"


def test_cash_difference_above_tolerance_returns_cash_break() -> None:
    internal = pd.DataFrame([_cash("USD", 1000)])
    external = pd.DataFrame([_cash("USD", 925)])

    result = reconcile_cash(internal, external, POLICY)

    assert result.loc[0, "status"] == "CASH_BREAK"


def test_cash_difference_below_tolerance_returns_within_tolerance() -> None:
    internal = pd.DataFrame([_cash("USD", 1000)])
    external = pd.DataFrame([_cash("USD", 975)])

    result = reconcile_cash(internal, external, POLICY)

    assert result.loc[0, "status"] == "WITHIN_TOLERANCE"


def test_run_auto_recon_returns_combined_position_and_cash_records(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    result = run_auto_recon(tmp_path, POLICY)

    assert len(result) == 7
    assert {"QUANTITY_BREAK", "PRICE_BREAK", "MISSING_IN_CUSTODIAN", "MISSING_IN_INTERNAL", "CASH_BREAK"}.issubset(
        set(result["status"])
    )


def test_matched_gets_workflow_status_closed(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    result = run_auto_recon(tmp_path, POLICY)
    matched = result[result["status"] == "MATCHED"]

    assert not matched.empty
    assert set(matched["workflow_status"]) == {"CLOSED"}


def test_within_tolerance_gets_workflow_status_closed(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)
    internal_cash = pd.read_csv(tmp_path / "internal_cash.csv")
    bank_cash = internal_cash.copy()
    bank_cash["cash_balance"] = bank_cash["cash_balance"] - 25
    bank_cash.to_csv(tmp_path / "bank_cash.csv", index=False)

    result = run_auto_recon(tmp_path, POLICY)
    within = result[result["status"] == "WITHIN_TOLERANCE"]

    assert not within.empty
    assert set(within["workflow_status"]) == {"CLOSED"}


def test_quantity_break_gets_workflow_status_open(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    result = run_auto_recon(tmp_path, POLICY)
    row = result[result["status"] == "QUANTITY_BREAK"].iloc[0]

    assert row["workflow_status"] == "OPEN"


def test_cash_break_gets_workflow_status_open(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    result = run_auto_recon(tmp_path, POLICY)
    row = result[result["status"] == "CASH_BREAK"].iloc[0]

    assert row["workflow_status"] == "OPEN"


def test_age_days_zero_break_gets_sla_bucket_current(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    result = run_auto_recon(tmp_path, POLICY)
    row = result[result["status"] == "QUANTITY_BREAK"].iloc[0]

    assert row["age_days"] == 0
    assert row["sla_bucket"] == "CURRENT"


def test_matched_record_gets_sla_bucket_not_applicable(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    result = run_auto_recon(tmp_path, POLICY)
    row = result[result["status"] == "MATCHED"].iloc[0]

    assert row["sla_bucket"] == "N/A"


def test_action_required_is_populated_for_break(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    result = run_auto_recon(tmp_path, POLICY)
    row = result[result["status"] == "QUANTITY_BREAK"].iloc[0]

    assert row["action_required"] == "Review trade blotter, settlement status, and custodian booking"


def test_action_required_is_no_action_for_matched_record(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    result = run_auto_recon(tmp_path, POLICY)
    row = result[result["status"] == "MATCHED"].iloc[0]

    assert row["action_required"] == "No action required"


def test_generated_demo_files_contain_intentional_breaks(tmp_path: Path) -> None:
    build_recon_demo_data(tmp_path)

    internal_positions = pd.read_csv(tmp_path / "internal_positions.csv")
    custodian_positions = pd.read_csv(tmp_path / "custodian_positions.csv")
    internal_cash = pd.read_csv(tmp_path / "internal_cash.csv")
    bank_cash = pd.read_csv(tmp_path / "bank_cash.csv")

    assert set(internal_positions["asof_date"]) == {"2026-01-10"}
    assert set(custodian_positions["asof_date"]) == {"2026-01-10"}
    assert set(internal_cash["asof_date"]) == {"2026-01-10"}
    assert set(bank_cash["asof_date"]) == {"2026-01-10"}

    spy_internal = internal_positions.loc[internal_positions["ticker"] == "SPY"].iloc[0]
    spy_custodian = custodian_positions.loc[custodian_positions["ticker"] == "SPY"].iloc[0]
    assert spy_internal["quantity"] == 16500
    assert spy_custodian["quantity"] == 16625
    assert spy_internal["price"] == spy_custodian["price"]

    aapl_internal = internal_positions.loc[internal_positions["ticker"] == "AAPL"].iloc[0]
    aapl_custodian = custodian_positions.loc[custodian_positions["ticker"] == "AAPL"].iloc[0]
    assert aapl_internal["quantity"] == aapl_custodian["quantity"]
    assert aapl_internal["price"] == 185.00
    assert aapl_custodian["price"] == 184.70

    assert "MSFT" in set(internal_positions["ticker"])
    assert "MSFT" not in set(custodian_positions["ticker"])
    assert "GOOGL" in set(custodian_positions["ticker"])
    assert "GOOGL" not in set(internal_positions["ticker"])
    assert float(internal_cash.loc[0, "cash_balance"]) == 25000
    assert float(bank_cash.loc[0, "cash_balance"]) == 24200
