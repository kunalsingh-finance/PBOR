from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
import shutil
import sqlite3

import pandas as pd
import pytest

from src.review import (
    NOTE_MAX_LENGTH,
    OWNER_MAX_LENGTH,
    REVIEW_COLUMNS,
    exception_key,
    load_review_history,
    load_reviews,
    save_review,
)


@pytest.fixture
def exception_row() -> dict[str, object]:
    return {
        "asof_date": "2026-01-09", "portfolio_id": "PF1", "account_id": "ACC1",
        "security_id": "SEC_SPY", "currency": None, "break_type": "QUANTITY_BREAK",
        "internal_quantity": 100, "external_quantity": 110.0,
        "internal_price": 42.25, "external_price": 42.25,
        "internal_market_value": 4225, "external_market_value": 4647.5,
    }


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "portfolio.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE financial_results (status TEXT, nav REAL)")
        conn.execute("INSERT INTO financial_results VALUES ('FAIL', 1234567.89)")
    return path


def test_key_is_stable_across_sql_pandas_types_and_presentation_changes(exception_row: dict) -> None:
    equivalent = {
        **exception_row, "asof_date": date(2026, 1, 9), "currency": pd.NA,
        "internal_quantity": 100.0, "external_quantity": "110.00", "internal_cash": float("nan"),
        "owner": "Another analyst", "severity": "HIGH", "review_status": "Escalated",
    }
    assert exception_key(exception_row) == exception_key(pd.Series(equivalent))
    assert len(exception_key(exception_row)) == 64


@pytest.mark.parametrize("field,value", [
    ("asof_date", "2026-01-10"), ("portfolio_id", "PF2"), ("account_id", "ACC2"),
    ("security_id", "SEC_AAPL"), ("currency", "EUR"), ("break_type", "PRICE_BREAK"),
    ("internal_quantity", 101), ("external_quantity", 111), ("internal_price", 42.26),
    ("external_price", 42.26), ("internal_market_value", 4226),
    ("external_market_value", 4648.5), ("internal_cash", 1), ("external_cash", 1),
])
def test_changed_break_gets_new_identity(exception_row: dict, field: str, value: object) -> None:
    assert exception_key(exception_row) != exception_key({**exception_row, field: value})


def test_cash_identity_supports_missing_security(exception_row: dict) -> None:
    cash = {**exception_row, "security_id": pd.NA, "currency": "USD", "break_type": "CASH_BREAK"}
    assert exception_key(cash) == exception_key({**cash, "security_id": None})


def test_key_preserves_exact_observation_precision(exception_row: dict) -> None:
    first = {**exception_row, "internal_quantity": Decimal("1.23456789012345678901234567890")}
    changed = {**first, "internal_quantity": Decimal("1.23456789012345678901234567891")}
    assert exception_key(first) != exception_key(changed)


@pytest.mark.parametrize("changes", [
    {"portfolio_id": None}, {"account_id": ""}, {"asof_date": "invalid"},
    {"security_id": None, "currency": None}, {"external_quantity": float("inf")},
    {"internal_quantity": "not a number"},
])
def test_invalid_identity_is_rejected(exception_row: dict, changes: dict) -> None:
    with pytest.raises(ValueError):
        exception_key({**exception_row, **changes})


def test_load_is_read_only_before_first_review(db_path: Path) -> None:
    before = db_path.read_bytes()
    assert list(load_reviews(db_path).columns) == REVIEW_COLUMNS
    assert load_review_history(db_path).empty
    assert db_path.read_bytes() == before


def test_latest_reviews_preserve_history_and_utc_time(db_path: Path, exception_row: dict) -> None:
    key = exception_key(exception_row)
    other_key = exception_key({**exception_row, "account_id": "ACC2"})
    save_review(db_path, key, "  Jane Lee  ", "Investigating", "  Checked the trade blotter.  ")
    save_review(db_path, other_key, "Alex", "Escalated", "Requested the missing custody file.")
    save_review(db_path, key, "Jane Lee", "Evidence recorded", "Received trade confirmation; awaiting source correction.")

    history = load_review_history(db_path)
    assert history["review_id"].tolist() == [1, 2, 3]
    assert history["owner"].iloc[0] == "Jane Lee"
    assert history["note"].iloc[0] == "Checked the trade blotter."
    assert history["review_status"].iloc[0] == "Investigating"
    for value in history["updated_at"]:
        assert datetime.fromisoformat(value).utcoffset() == timedelta(0)
    latest = load_reviews(db_path).set_index("exception_key")
    assert len(latest) == 2
    assert latest.loc[key, "review_status"] == "Evidence recorded"
    assert latest.loc[other_key, "review_status"] == "Escalated"
    changed_key = exception_key({**exception_row, "external_quantity": 120})
    assert changed_key not in latest.index


@pytest.mark.parametrize("changes", [
    {"exception_id": "arbitrary-row-id"}, {"owner": "  "}, {"owner": None},
    {"owner": "a" * (OWNER_MAX_LENGTH + 1)}, {"review_status": "Resolved"},
    {"review_status": "CLOSED"}, {"note": ""}, {"note": "checked"},
    {"note": "." * 20}, {"note": None}, {"note": "a" * (NOTE_MAX_LENGTH + 1)},
])
def test_invalid_review_does_not_write(db_path: Path, exception_row: dict, changes: dict) -> None:
    values = {"exception_id": exception_key(exception_row), "owner": "Jane Lee",
              "review_status": "Investigating", "note": "Requested booking evidence."}
    before = db_path.read_bytes()
    with pytest.raises(ValueError):
        save_review(db_path, **{**values, **changes})
    assert db_path.read_bytes() == before


def test_missing_database_is_never_created(tmp_path: Path, exception_row: dict) -> None:
    missing = tmp_path / "missing.db"
    for reader in (load_reviews, load_review_history):
        with pytest.raises(FileNotFoundError, match="Run the pipeline first"):
            reader(missing)
    with pytest.raises(FileNotFoundError, match="Run the pipeline first"):
        save_review(missing, exception_key(exception_row), "Jane Lee", "Investigating", "Checked booking evidence.")
    assert not missing.exists()


def test_review_cannot_mutate_financial_results_or_rewrite_history(db_path: Path, exception_row: dict) -> None:
    save_review(db_path, exception_key(exception_row), "Jane Lee", "Evidence recorded", "Confirmed source needs correction.")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT * FROM financial_results").fetchall() == [("FAIL", 1234567.89)]
        for statement in ("UPDATE exception_reviews SET owner = 'Someone else'", "DELETE FROM exception_reviews"):
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                conn.execute(statement)
    assert len(load_review_history(db_path)) == 1


def test_journal_survives_pipeline_rerun(tmp_path: Path, exception_row: dict) -> None:
    from src.run_month_end import run_month_end

    root = Path(__file__).resolve().parents[1]
    for directory in ("data", "sql"):
        shutil.copytree(root / directory, tmp_path / directory)
    shutil.copy2(root / "policy.yaml", tmp_path / "policy.yaml")
    path = tmp_path / "pbor_lite.db"
    run_month_end(project_root=tmp_path, asof_date="2026-01-10")
    with sqlite3.connect(path) as conn:
        table_names = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )]
        financial_results = {table: conn.execute(f'SELECT * FROM "{table}"').fetchall() for table in table_names}
    save_review(path, exception_key(exception_row), "Jane Lee", "Investigating", "Requested custody booking evidence.")
    with sqlite3.connect(path) as conn:
        for table, expected in financial_results.items():
            assert conn.execute(f'SELECT * FROM "{table}"').fetchall() == expected
    before = load_review_history(path)
    run_month_end(project_root=tmp_path, asof_date="2026-01-10")
    pd.testing.assert_frame_equal(load_review_history(path), before)
