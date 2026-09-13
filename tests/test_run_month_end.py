from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pandas as pd

from src.run_month_end import _recon_metrics, _sla_count, run_month_end


def test_run_month_end_creates_rows(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    shutil.copy(root / "policy.yaml", tmp_path / "policy.yaml")
    shutil.copytree(root / "sql", tmp_path / "sql")
    shutil.copytree(root / "data", tmp_path / "data")
    summary = run_month_end(project_root=tmp_path, asof_date="2026-01-10")
    assert summary["positions_rows"] > 0
    assert summary["monthly_returns_rows"] > 0
    export_path = Path(str(summary["exports_path"]))
    assert export_path.exists()
    assert (export_path / "onepager.md").exists()
    assert (export_path / "summary.json").exists()
    assert (export_path / "onepager.pdf").exists()
    assert (export_path / "tearsheet.png").exists()
    assert (export_path / "controls_table.png").exists()
    assert (export_path / "attribution_reconciliation.csv").exists()
    assert summary["ready_for_signoff"] is False  # Custodian/bank feeds were not supplied.
    with sqlite3.connect(tmp_path / "pbor_lite.db") as conn:
        persisted = pd.read_sql_query("SELECT * FROM pbor_monthly_returns", conn)
    exported = pd.read_csv(export_path / "monthly_returns.csv")
    for column in ["portfolio_return_arithmetic", "active_return_arithmetic"]:
        pd.testing.assert_series_equal(persisted[column], exported[column], check_exact=False)
    summary = json.loads((export_path / "summary.json").read_text(encoding="utf-8"))
    workbook_names = [name for name in summary["files"] if str(name).lower().endswith(".xlsx")]
    assert len(workbook_names) >= 1
    assert (export_path / workbook_names[0]).exists()
    assert "controls_table.png" in summary["files"]


def test_recon_metrics_only_count_open_workflow_exceptions() -> None:
    recon = pd.DataFrame([
        {"status": "QUANTITY_BREAK", "workflow_status": "CLOSED", "severity": "HIGH", "sla_bucket": "BREACHED"},
        {"status": "CASH_BREAK", "workflow_status": "OPEN", "severity": "HIGH", "sla_bucket": "BREACHED"},
        {"status": "MATCHED", "workflow_status": None, "severity": "INFO", "sla_bucket": "N/A"},
    ])
    metrics = _recon_metrics(recon)
    assert metrics["recon_open_exceptions"] == 1
    assert metrics["recon_high_severity_exceptions"] == 1
    assert _sla_count(recon, "BREACHED") == 1


def test_existing_monthly_table_is_migrated_to_preserve_arithmetic_returns(tmp_path: Path) -> None:
    from src.ingest import initialize_db

    root = Path(__file__).resolve().parents[1]
    db_path = tmp_path / "old.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE pbor_monthly_returns (month_end TEXT, portfolio_id TEXT, portfolio_return_twr REAL, "
                     "portfolio_return_dietz REAL, dietz_denominator REAL, benchmark_return REAL, active_return REAL)")
    conn = initialize_db(db_path, root / "sql/ddl.sql", root / "sql/views.sql")
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(pbor_monthly_returns)")}
        assert {"portfolio_return_arithmetic", "active_return_arithmetic"} <= columns
    finally:
        conn.close()
