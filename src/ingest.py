from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

REQUIRED_COLUMNS: dict[str, list[str]] = {
    "security_master.csv": [
        "security_id",
        "ticker",
        "name",
        "asset_class",
        "sector",
        "currency",
    ],
    "prices.csv": ["date", "security_id", "price", "price_currency", "source"],
    "fx_rates.csv": ["date", "ccy_pair", "rate", "source"],
    "transactions.csv": [
        "date",
        "portfolio_id",
        "security_id",
        "quantity",
        "price",
        "fees",
        "txn_type",
    ],
    "holdings_reported.csv": [
        "date",
        "portfolio_id",
        "security_id",
        "quantity",
        "market_value_base",
    ],
    "benchmark_weights.csv": ["date", "benchmark_id", "sector", "weight"],
    "benchmark_returns.csv": ["date", "benchmark_id", "sector", "return"],
}

DATE_COLUMNS: dict[str, list[str]] = {
    "prices.csv": ["date"],
    "fx_rates.csv": ["date"],
    "transactions.csv": ["date"],
    "holdings_reported.csv": ["date"],
    "benchmark_weights.csv": ["date"],
    "benchmark_returns.csv": ["date"],
}


def load_policy(policy_path: Path) -> dict[str, Any]:
    with policy_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _validate_columns(file_name: str, frame: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS[file_name] if col not in frame.columns]
    if missing:
        raise ValueError(f"{file_name} missing required columns: {missing}")


def load_inputs(data_dir: Path) -> dict[str, pd.DataFrame]:
    loaded: dict[str, pd.DataFrame] = {}
    missing_files = [name for name in REQUIRED_COLUMNS if not (data_dir / name).exists()]
    if missing_files:
        raise FileNotFoundError(f"Missing required files in {data_dir}: {missing_files}")

    for file_name in REQUIRED_COLUMNS:
        frame = pd.read_csv(data_dir / file_name)
        _validate_columns(file_name, frame)
        for date_col in DATE_COLUMNS.get(file_name, []):
            frame[date_col] = pd.to_datetime(frame[date_col], errors="raise").dt.date
        loaded[file_name] = frame

    loaded["transactions.csv"]["txn_type"] = loaded["transactions.csv"]["txn_type"].str.upper()
    return loaded


def initialize_db(db_path: Path, ddl_path: Path, views_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(ddl_path.read_text(encoding="utf-8"))
    conn.executescript(views_path.read_text(encoding="utf-8"))
    return conn


def _reset_tables(conn: sqlite3.Connection, table_names: list[str]) -> None:
    for table in table_names:
        conn.execute(f"DELETE FROM {table}")
    conn.commit()


def load_tables(conn: sqlite3.Connection, inputs: dict[str, pd.DataFrame]) -> None:
    table_map = {
        "security_master.csv": "dim_security",
        "prices.csv": "fact_prices",
        "fx_rates.csv": "fact_fx_rates",
        "transactions.csv": "fact_transactions",
        "holdings_reported.csv": "fact_holdings_reported",
        "benchmark_weights.csv": "fact_benchmark_weights",
        "benchmark_returns.csv": "fact_benchmark_returns",
    }
    portfolio_ids = pd.concat(
        [
            inputs["transactions.csv"][["portfolio_id"]],
            inputs["holdings_reported.csv"][["portfolio_id"]],
        ],
        ignore_index=True,
    ).drop_duplicates()
    portfolio_ids["portfolio_name"] = portfolio_ids["portfolio_id"]

    ordered_tables = [
        "dim_security",
        "dim_portfolio",
        "fact_prices",
        "fact_fx_rates",
        "fact_transactions",
        "fact_holdings_reported",
        "fact_benchmark_weights",
        "fact_benchmark_returns",
    ]
    _reset_tables(conn, ordered_tables)

    inputs["security_master.csv"].to_sql("dim_security", conn, if_exists="append", index=False)
    portfolio_ids.to_sql("dim_portfolio", conn, if_exists="append", index=False)
    for csv_name, table_name in table_map.items():
        if csv_name == "security_master.csv":
            continue
        inputs[csv_name].to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()


def ingest_qa_summary(inputs: dict[str, pd.DataFrame], base_currency: str) -> pd.DataFrame:
    security_master = inputs["security_master.csv"]
    prices = inputs["prices.csv"]
    transactions = inputs["transactions.csv"]

    duplicate_prices = prices.duplicated(subset=["date", "security_id", "source"]).sum()
    unknown_security_txn = (
        transactions["security_id"].notna()
        & ~transactions["security_id"].isin(security_master["security_id"])
    ).sum()
    missing_currency = security_master["currency"].isna().sum()
    unsupported_base = (security_master["currency"] == base_currency).sum() == 0

    rows = [
        {
            "check_name": "duplicate_prices",
            "status": "FAIL" if duplicate_prices else "PASS",
            "issue_count": int(duplicate_prices),
        },
        {
            "check_name": "unknown_security_transactions",
            "status": "FAIL" if unknown_security_txn else "PASS",
            "issue_count": int(unknown_security_txn),
        },
        {
            "check_name": "missing_security_currency",
            "status": "FAIL" if missing_currency else "PASS",
            "issue_count": int(missing_currency),
        },
        {
            "check_name": "base_currency_present_in_master",
            "status": "FAIL" if unsupported_base else "PASS",
            "issue_count": int(unsupported_base),
        },
    ]
    return pd.DataFrame(rows)
