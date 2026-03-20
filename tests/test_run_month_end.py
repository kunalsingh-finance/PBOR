from __future__ import annotations

import json
from pathlib import Path

from src.run_month_end import run_month_end


def test_run_month_end_creates_rows() -> None:
    root = Path(__file__).resolve().parents[1]
    summary = run_month_end(project_root=root, asof_date="2026-01-10")
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
    summary = json.loads((export_path / "summary.json").read_text(encoding="utf-8"))
    workbook_names = [name for name in summary["files"] if str(name).lower().endswith(".xlsx")]
    assert len(workbook_names) >= 1
    assert (export_path / workbook_names[0]).exists()
    assert "controls_table.png" in summary["files"]
