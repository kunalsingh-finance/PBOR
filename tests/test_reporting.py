from __future__ import annotations

import tempfile
import unittest
import warnings
from pathlib import Path

import pandas as pd

from src.export import _build_onepager_markdown
from src.qa import format_flow_summary_line
from src.report import generate_tear_sheet


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
