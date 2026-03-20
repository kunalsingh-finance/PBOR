from __future__ import annotations

import math

import pandas as pd

from src.returns import compute_returns


def test_compute_returns_clamps_benchmark_month_to_asof() -> None:
    inputs = {
        "prices.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "security_id": "SEC1", "price": 10.0, "source": "unit"},
                {"date": "2026-03-02", "security_id": "SEC1", "price": 11.0, "source": "unit"},
            ]
        ),
        "transactions.csv": pd.DataFrame(
            [
                {
                    "date": "2026-03-01",
                    "portfolio_id": "PF1",
                    "security_id": None,
                    "txn_type": "CONTRIB",
                    "quantity": 0.0,
                    "price": 0.0,
                    "fees": 0.0,
                    "cash_amount": 1000.0,
                },
                {
                    "date": "2026-03-01",
                    "portfolio_id": "PF1",
                    "security_id": "SEC1",
                    "txn_type": "BUY",
                    "quantity": 100.0,
                    "price": 10.0,
                    "fees": 0.0,
                    "cash_amount": None,
                },
            ]
        ),
        "security_master.csv": pd.DataFrame(
            [
                {"security_id": "SEC1", "currency": "USD", "sector": "Tech"},
            ]
        ),
        "fx_rates.csv": pd.DataFrame(columns=["date", "ccy_pair", "rate", "source"]),
        "benchmark_weights.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "benchmark_id": "BM1", "sector": "Tech", "weight": 1.0},
                {"date": "2026-03-02", "benchmark_id": "BM1", "sector": "Tech", "weight": 1.0},
                {"date": "2026-03-03", "benchmark_id": "BM1", "sector": "Tech", "weight": 1.0},
            ]
        ),
        "benchmark_returns.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "benchmark_id": "BM1", "sector": "Tech", "return": 0.01},
                {"date": "2026-03-02", "benchmark_id": "BM1", "sector": "Tech", "return": 0.02},
                {"date": "2026-03-03", "benchmark_id": "BM1", "sector": "Tech", "return": 0.03},
            ]
        ),
    }
    policy = {
        "base_currency": "USD",
        "cash_flow_timing": "end_of_day",
        "benchmark_id_default": "BM1",
    }

    _, _, monthly_returns, _ = compute_returns(inputs=inputs, policy=policy, asof_date=pd.Timestamp("2026-03-02"))

    assert len(monthly_returns) == 1
    row = monthly_returns.iloc[0]
    expected_benchmark = (1.0 + 0.01) * (1.0 + 0.02) - 1.0

    assert str(row["month_end"]) == "2026-03-02"
    assert math.isclose(float(row["benchmark_return"]), expected_benchmark, rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(float(row["active_return_arithmetic"]), 0.1 - expected_benchmark, rel_tol=0.0, abs_tol=1e-12)


def test_compute_returns_uses_default_benchmark_only() -> None:
    inputs = {
        "prices.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "security_id": "SEC1", "price": 10.0, "source": "unit"},
                {"date": "2026-03-02", "security_id": "SEC1", "price": 11.0, "source": "unit"},
            ]
        ),
        "transactions.csv": pd.DataFrame(
            [
                {
                    "date": "2026-03-01",
                    "portfolio_id": "PF1",
                    "security_id": None,
                    "txn_type": "CONTRIB",
                    "quantity": 0.0,
                    "price": 0.0,
                    "fees": 0.0,
                    "cash_amount": 1000.0,
                },
                {
                    "date": "2026-03-01",
                    "portfolio_id": "PF1",
                    "security_id": "SEC1",
                    "txn_type": "BUY",
                    "quantity": 100.0,
                    "price": 10.0,
                    "fees": 0.0,
                    "cash_amount": None,
                },
            ]
        ),
        "security_master.csv": pd.DataFrame(
            [
                {"security_id": "SEC1", "currency": "USD", "sector": "Tech"},
            ]
        ),
        "fx_rates.csv": pd.DataFrame(columns=["date", "ccy_pair", "rate", "source"]),
        "benchmark_weights.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "benchmark_id": "BM1", "sector": "Tech", "weight": 1.0},
                {"date": "2026-03-02", "benchmark_id": "BM1", "sector": "Tech", "weight": 1.0},
                {"date": "2026-03-01", "benchmark_id": "BM2", "sector": "Tech", "weight": 1.0},
                {"date": "2026-03-02", "benchmark_id": "BM2", "sector": "Tech", "weight": 1.0},
            ]
        ),
        "benchmark_returns.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "benchmark_id": "BM1", "sector": "Tech", "return": 0.01},
                {"date": "2026-03-02", "benchmark_id": "BM1", "sector": "Tech", "return": 0.02},
                {"date": "2026-03-01", "benchmark_id": "BM2", "sector": "Tech", "return": 0.10},
                {"date": "2026-03-02", "benchmark_id": "BM2", "sector": "Tech", "return": 0.10},
            ]
        ),
    }
    policy = {
        "base_currency": "USD",
        "cash_flow_timing": "end_of_day",
        "benchmark_id_default": "BM1",
    }

    _, _, monthly_returns, _ = compute_returns(inputs=inputs, policy=policy, asof_date=pd.Timestamp("2026-03-02"))

    assert len(monthly_returns) == 1
    row = monthly_returns.iloc[0]
    expected_benchmark = (1.0 + 0.01) * (1.0 + 0.02) - 1.0

    assert math.isclose(float(row["benchmark_return"]), expected_benchmark, rel_tol=0.0, abs_tol=1e-12)


def test_compute_returns_uses_portfolio_benchmark_map() -> None:
    inputs = {
        "prices.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "security_id": "SEC1", "price": 10.0, "source": "unit"},
                {"date": "2026-03-02", "security_id": "SEC1", "price": 11.0, "source": "unit"},
            ]
        ),
        "transactions.csv": pd.DataFrame(
            [
                {
                    "date": "2026-03-01",
                    "portfolio_id": "PF1",
                    "security_id": None,
                    "txn_type": "CONTRIB",
                    "quantity": 0.0,
                    "price": 0.0,
                    "fees": 0.0,
                    "cash_amount": 1000.0,
                },
                {
                    "date": "2026-03-01",
                    "portfolio_id": "PF1",
                    "security_id": "SEC1",
                    "txn_type": "BUY",
                    "quantity": 100.0,
                    "price": 10.0,
                    "fees": 0.0,
                    "cash_amount": None,
                },
                {
                    "date": "2026-03-01",
                    "portfolio_id": "PF2",
                    "security_id": None,
                    "txn_type": "CONTRIB",
                    "quantity": 0.0,
                    "price": 0.0,
                    "fees": 0.0,
                    "cash_amount": 1000.0,
                },
                {
                    "date": "2026-03-01",
                    "portfolio_id": "PF2",
                    "security_id": "SEC1",
                    "txn_type": "BUY",
                    "quantity": 100.0,
                    "price": 10.0,
                    "fees": 0.0,
                    "cash_amount": None,
                },
            ]
        ),
        "security_master.csv": pd.DataFrame(
            [
                {"security_id": "SEC1", "currency": "USD", "sector": "Tech"},
            ]
        ),
        "fx_rates.csv": pd.DataFrame(columns=["date", "ccy_pair", "rate", "source"]),
        "benchmark_weights.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "benchmark_id": "BM1", "sector": "Tech", "weight": 1.0},
                {"date": "2026-03-02", "benchmark_id": "BM1", "sector": "Tech", "weight": 1.0},
                {"date": "2026-03-01", "benchmark_id": "BM2", "sector": "Tech", "weight": 1.0},
                {"date": "2026-03-02", "benchmark_id": "BM2", "sector": "Tech", "weight": 1.0},
            ]
        ),
        "benchmark_returns.csv": pd.DataFrame(
            [
                {"date": "2026-03-01", "benchmark_id": "BM1", "sector": "Tech", "return": 0.01},
                {"date": "2026-03-02", "benchmark_id": "BM1", "sector": "Tech", "return": 0.02},
                {"date": "2026-03-01", "benchmark_id": "BM2", "sector": "Tech", "return": 0.10},
                {"date": "2026-03-02", "benchmark_id": "BM2", "sector": "Tech", "return": 0.10},
            ]
        ),
    }
    policy = {
        "base_currency": "USD",
        "cash_flow_timing": "end_of_day",
        "benchmark_id_default": "BM1",
        "portfolio_benchmark_map": {
            "PF1": "BM1",
            "PF2": "BM2",
        },
    }

    _, _, monthly_returns, _ = compute_returns(inputs=inputs, policy=policy, asof_date=pd.Timestamp("2026-03-02"))

    expected = {
        "PF1": (1.0 + 0.01) * (1.0 + 0.02) - 1.0,
        "PF2": (1.0 + 0.10) * (1.0 + 0.10) - 1.0,
    }
    actual = {
        str(row["portfolio_id"]): float(row["benchmark_return"])
        for _, row in monthly_returns.sort_values("portfolio_id").iterrows()
    }

    assert set(actual.keys()) == {"PF1", "PF2"}
    assert math.isclose(actual["PF1"], expected["PF1"], rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(actual["PF2"], expected["PF2"], rel_tol=0.0, abs_tol=1e-12)
