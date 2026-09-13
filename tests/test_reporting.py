from __future__ import annotations

import tempfile
import json
import unittest
import warnings
from pathlib import Path

import pandas as pd
import pytest

from src.export import _build_onepager_markdown, _period_return_rows, _risk_metrics, export_outputs
from src.qa import format_flow_summary_line
from src.report import generate_tear_sheet
from src.signoff import build_signoff_summary


def _sample_daily_returns() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-01-02",
                "portfolio_id": "PF1",
                "daily_return": 0.01,
                "benchmark_return": 0.001,
                "external_flow_base": 100000.0,
                "portfolio_value_base": 1000000.0,
            },
            {
                "date": "2026-01-03",
                "portfolio_id": "PF1",
                "daily_return": -0.002,
                "benchmark_return": 0.0,
                "external_flow_base": -5000.0,
                "portfolio_value_base": 1010000.0,
            },
            {
                "date": "2026-01-10",
                "portfolio_id": "PF1",
                "daily_return": 0.004,
                "benchmark_return": 0.001,
                "external_flow_base": 0.0,
                "portfolio_value_base": 1020000.0,
            },
        ]
    )


def _sample_monthly(active_return: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "portfolio_return_twr": 0.0612,
                "portfolio_return_dietz": 0.0697,
                "benchmark_return": 0.0119,
                "active_return": active_return,
                "active_return_arithmetic": active_return,
                "portfolio_return_arithmetic": 0.0549,
            }
        ]
    )


def _sample_attribution(active_effect_scale: float = 1.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "benchmark_id": "BM1",
                "sector": "Tech",
                "w_p": 0.30,
                "w_b": 0.60,
                "r_p": 0.12,
                "r_b": 0.03,
                "allocation_effect": -0.00067 * active_effect_scale,
                "selection_effect": 0.07211 * active_effect_scale,
                "interaction_effect": -0.03605 * active_effect_scale,
                "active_effect": 0.03539 * active_effect_scale,
            },
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "benchmark_id": "BM1",
                "sector": "Energy",
                "w_p": 0.21,
                "w_b": 0.40,
                "r_p": 0.09,
                "r_b": 0.02,
                "allocation_effect": 0.00063 * active_effect_scale,
                "selection_effect": 0.03468 * active_effect_scale,
                "interaction_effect": -0.01647 * active_effect_scale,
                "active_effect": 0.01884 * active_effect_scale,
            },
            {
                "month_end": "2026-01-10",
                "portfolio_id": "PF1",
                "benchmark_id": "BM1",
                "sector": "Cash",
                "w_p": 0.49,
                "w_b": 0.00,
                "r_p": 0.00,
                "r_b": 0.00,
                "allocation_effect": -0.00581 * active_effect_scale,
                "selection_effect": 0.0,
                "interaction_effect": 0.0,
                "active_effect": -0.00581 * active_effect_scale,
            },
        ]
    )


class ReportingTests(unittest.TestCase):
    def test_flow_line_exact_format(self) -> None:
        line = format_flow_summary_line(_sample_daily_returns())
        self.assertEqual(
            line,
            "Net flow (MTD): $95,000 | Largest flow: $100,000 on 2026-01-02 | Flows present: Yes",
        )

    def test_onepager_markdown_includes_total_row(self) -> None:
        onepager = _build_onepager_markdown(
            asof_date="2026-01-10",
            daily_returns=_sample_daily_returns(),
            monthly_returns=_sample_monthly(active_return=0.04842),
            attribution=_sample_attribution(active_effect_scale=1.0),
            breaks=pd.DataFrame(columns=["break_type", "severity", "details"]),
            ingest_qa=pd.DataFrame(columns=["check_name", "status", "issue_count"]),
            reconciliation_tolerance_bps=5.0,
            cash_return_source="0%",
        )
        self.assertIn("- Total | Alloc `", onepager)
        self.assertIn("## Risk Metrics (Annualized)", onepager)
        self.assertIn("## Linked Multi-Period Returns", onepager)
        self.assertIn("## Analyst Commentary", onepager)
        self.assertIn("As-of (data):", onepager)
        self.assertIn("Generated:", onepager)
        self.assertIn("Analysis window:", onepager)
        self.assertIn("MTD window:", onepager)

    def test_onepager_gating_withholds_attribution_when_recon_fails(self) -> None:
        onepager = _build_onepager_markdown(
            asof_date="2026-01-10",
            daily_returns=_sample_daily_returns(),
            monthly_returns=_sample_monthly(active_return=0.0100),
            attribution=_sample_attribution(active_effect_scale=4.0),
            breaks=pd.DataFrame(columns=["break_type", "severity", "details"]),
            ingest_qa=pd.DataFrame(columns=["check_name", "status", "issue_count"]),
            reconciliation_tolerance_bps=5.0,
            cash_return_source="0%",
        )
        self.assertIn("Attribution withheld pending reconciliation.", onepager)

    def test_generate_tear_sheet_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                png_path, pdf_path = generate_tear_sheet(
                    output_dir=Path(tmp),
                    asof_date="2026-01-10",
                    daily_returns=_sample_daily_returns(),
                    monthly_returns=_sample_monthly(active_return=0.04842),
                    attribution=_sample_attribution(active_effect_scale=1.0),
                    breaks=pd.DataFrame(columns=["break_type", "severity", "details"]),
                    reconciliation_tolerance_bps=5.0,
                    cash_return_source="0%",
                )
            self.assertTrue(png_path.exists())
            self.assertTrue(pdf_path.exists())
            self.assertGreater(png_path.stat().st_size, 1000)
            self.assertGreater(pdf_path.stat().st_size, 1000)
            save_warnings = [str(w.message).lower() for w in caught]
            self.assertFalse(any("constrained_layout" in msg for msg in save_warnings))


if __name__ == "__main__":
    unittest.main()


def test_export_separates_selected_performance_from_global_signoff(tmp_path: Path) -> None:
    daily = pd.concat([
        _sample_daily_returns(),
        _sample_daily_returns().assign(portfolio_id="PF2", daily_return=0.02, benchmark_return=0.001),
    ], ignore_index=True)
    monthly = pd.concat([
        _sample_monthly(0.04842), _sample_monthly(0.04842).assign(portfolio_id="PF2"),
    ], ignore_index=True)
    attribution = pd.concat([
        _sample_attribution(), _sample_attribution().assign(portfolio_id="PF2"),
    ], ignore_index=True)
    breaks = pd.DataFrame(columns=["break_type", "severity", "details"])
    signoff = build_signoff_summary(breaks, pd.DataFrame(), {
        "available": True, "within_tolerance": True, "weights_ok": True, "portfolio_return_ok": True,
    })
    target = export_outputs(
        output_root=tmp_path,
        asof_date="2026-01-10",
        daily_returns=daily,
        monthly_returns=monthly,
        attribution=attribution,
        breaks=breaks,
        ingest_qa=pd.DataFrame(columns=["check_name", "status", "issue_count"]),
        signoff_summary=signoff,
        ready_for_signoff=True,  # The actual final control result must take precedence.
    )
    summary = json.loads((target / "summary.json").read_text(encoding="utf-8"))
    assert summary["attribution_reconciliation"]["within_tolerance"] is True
    assert summary["data_status"] == "Under Review"
    assert summary["ready_for_signoff"] is False
    assert summary["signoff_ready"] is False
    assert summary["performance_portfolio_id"] == "PF2"
    assert summary["linked_returns"][0]["portfolio"] == pytest.approx(1.02 ** 3 - 1)
    assert summary["linked_returns"][0]["days"] == 3
    assert summary["risk_metrics_annualized"]["volatility"] == pytest.approx(0.0)
    assert len(pd.read_csv(target / "daily_returns.csv")) == 6
    workbook = next(target.glob("*.xlsx"))
    excel_summary = pd.read_excel(workbook, sheet_name="Summary").set_index("metric")["value"]
    assert excel_summary["performance_portfolio_id"] == "PF2"
    assert float(excel_summary["mtd_portfolio_return"]) == pytest.approx(1.02 ** 3 - 1)
    markdown = (target / "onepager.md").read_text(encoding="utf-8")
    assert "Performance portfolio: `PF2`" in markdown
    assert "Reporting pack readiness (all portfolios and control areas): **Under Review**" in markdown
    assert "- Portfolio `PF1`" not in markdown


def test_performance_helpers_reject_combined_portfolio_histories() -> None:
    daily = pd.concat([_sample_daily_returns(), _sample_daily_returns().assign(portfolio_id="PF2")])
    with pytest.raises(ValueError, match="Select one portfolio"):
        _period_return_rows(daily)
    with pytest.raises(ValueError, match="Select one portfolio"):
        _risk_metrics(daily, "0%")


def test_tearsheet_card_uses_global_readiness(tmp_path: Path) -> None:
    from unittest.mock import patch
    from src.report import _draw_card

    with patch("src.report._draw_card", wraps=_draw_card) as card:
        generate_tear_sheet(
            output_dir=tmp_path,
            asof_date="2026-01-10",
            daily_returns=_sample_daily_returns(),
            monthly_returns=_sample_monthly(0.04842),
            attribution=_sample_attribution(),
            breaks=pd.DataFrame(columns=["break_type", "severity", "details"]),
            ready_for_signoff=False,
        )
    status_card = card.call_args_list[0].args
    assert status_card[5:7] == ("Pack Readiness", "Under Review")
