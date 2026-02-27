from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd


def _pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def _pct_or_na(x: object) -> str:
    try:
        value = float(x)
    except (TypeError, ValueError):
        return "N/A"
    return f"{value * 100:.2f}%" if math.isfinite(value) else "N/A"


def _ratio_or_na(x: object) -> str:
    try:
        value = float(x)
    except (TypeError, ValueError):
        return "N/A"
    return f"{value:.2f}" if math.isfinite(value) else "N/A"


def show_results(project_root: Path, month: str) -> None:
    out_dir = project_root / "outputs" / month
    if not out_dir.exists():
        raise FileNotFoundError(f"Output folder not found: {out_dir}")

    monthly = pd.read_csv(out_dir / "monthly_returns.csv")
    attribution = pd.read_csv(out_dir / "attribution.csv")
    recon = pd.read_csv(out_dir / "attribution_reconciliation.csv")
    breaks = pd.read_csv(out_dir / "breaks.csv")
    ingest = pd.read_csv(out_dir / "qa_ingest_summary.csv")
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))

    print(f"PBOR-Lite results for {month}")
    print("=" * 40)
    data_status = summary.get("data_status", "N/A")
    dataset_label = summary.get("dataset_label", "N/A")
    data_asof_date = summary.get("data_asof_date", summary.get("asof_date", "N/A"))
    generated_at_et = summary.get("generated_at_et", "N/A")
    market_last_closed = summary.get("market_last_closed_session")
    analysis_window = summary.get("analysis_window", {})
    mtd_window = summary.get("mtd_window", {})
    cash_return_source = summary.get("cash_return_source", "0%")
    print(f"Data status: {data_status}")
    print(f"Dataset: {dataset_label}")
    print(f"As-of (data): {data_asof_date}")
    print(f"Generated: {generated_at_et}")
    if market_last_closed:
        print(f"Market last closed session: {market_last_closed}")
    if analysis_window:
        print(
            f"Analysis window: {analysis_window.get('start', 'N/A')} to {analysis_window.get('end', 'N/A')} "
            f"({analysis_window.get('obs_rows', 0)} obs rows | {analysis_window.get('trading_days', 0)} trading days)"
        )
    print(
        f"MTD window: {mtd_window.get('start', 'N/A')} to {mtd_window.get('end', 'N/A')} "
        f"({mtd_window.get('obs_rows', 0)} obs rows | {mtd_window.get('trading_days', 0)} trading days)"
    )
    if data_status != "Validated":
        print("PRELIMINARY - DO NOT DISTRIBUTE")

    print("\nPerformance:")
    if monthly.empty:
        print("- No monthly rows generated.")
    else:
        label = "Provisional" if data_status != "Validated" else "Official"
        for _, row in monthly.iterrows():
            active_twr = _pct(float(row["active_return"]))
            active_arith = (
                _pct(float(row["active_return_arithmetic"]))
                if "active_return_arithmetic" in row.index
                else "N/A"
            )
            print(
                f"- {label} {row['portfolio_id']}: "
                f"TWR {_pct(float(row['portfolio_return_twr']))}, "
                f"Dietz {_pct(float(row['portfolio_return_dietz']))}, "
                f"Benchmark {_pct(float(row['benchmark_return']))}, "
                f"Active (TWR) {active_twr}, "
                f"Active (arith, MTD) {active_arith}"
            )

    linked_returns = summary.get("linked_returns", [])
    if linked_returns:
        print("\nLinked returns:")
        for row in linked_returns:
            print(
                f"- {row.get('period', 'N/A')}: "
                f"Portfolio {_pct(float(row.get('portfolio', 0.0)))}, "
                f"Benchmark {_pct(float(row.get('benchmark', 0.0)))}, "
                f"Active (TWR) {_pct(float(row.get('active_twr', 0.0)))} "
                f"({int(row.get('days', 0))} days)"
            )

    risk = summary.get("risk_metrics_annualized", {})
    if risk:
        te = risk.get("tracking_error")
        ir = risk.get("information_ratio")
        sharpe = risk.get("sharpe")
        vol = risk.get("volatility")
        print("\nRisk (annualized):")
        print(f"- Tracking error: {_pct_or_na(te)}")
        print(f"- Information ratio: {_ratio_or_na(ir)}")
        print(f"- Sharpe ratio: {_ratio_or_na(sharpe)}")
        print(f"- Volatility: {_pct_or_na(vol)}")

    tolerance = float(summary.get("reconciliation_tolerance_bps", 5.0))
    print("\nAttribution reconciliation:")
    if recon.empty:
        print("- Reconciliation data unavailable.")
        within_tol = False
    else:
        latest = recon.sort_values("month_end").iloc[-1]
        within_tol = bool(latest["within_tolerance"])
        print(
            f"- Attribution sum {_pct(float(latest['attribution_sum']))}, "
            f"Active (arith, MTD) {_pct(float(latest['active_return_reference']))}, "
            f"Diff {float(latest['diff_bps']):.1f} bps "
            f"(threshold < {tolerance:.1f} bps)"
        )
        print(
            f"- Weight sums: sum_w_p={float(latest['w_p_sum']):.6f}, sum_w_b={float(latest['w_b_sum']):.6f}"
        )
        print(
            f"- Portfolio return check: sector-based {_pct(float(latest['portfolio_return_from_sectors']))} "
            f"vs reference {_pct(float(latest['portfolio_return_reference']))} "
            f"(diff {float(latest['portfolio_return_diff_bps']):.1f} bps)"
        )

    print("\nAttribution (largest effects):")
    if attribution.empty:
        print("- No attribution rows generated.")
    elif not within_tol:
        print("- Attribution withheld pending reconciliation.")
    else:
        latest_month = attribution["month_end"].max()
        top = attribution[attribution["month_end"] == latest_month].assign(
            active_bps=lambda x: x["active_effect"] * 10000.0
        ).sort_values(
            "active_bps", ascending=False
        )
        for _, row in top.head(3).iterrows():
            print(
                f"- {row['sector']}: active {float(row['active_bps']):.1f} bps "
                f"(alloc {float(row['allocation_effect']) * 10000:.1f} bps, "
                f"sel {float(row['selection_effect']) * 10000:.1f} bps, "
                f"int {float(row['interaction_effect']) * 10000:.1f} bps)"
            )

    print("\nBreaks:")
    qa_stats = summary.get("qa_stats", {})
    print(
        "- QA stats: "
        f"Return outliers={int(qa_stats.get('return_outliers', 0))}, "
        f"NAV jump/zero-flow={int(qa_stats.get('nav_jump_zero_flow_flags', 0))}, "
        f"Missing prices={int(qa_stats.get('missing_prices', 0))}, "
        f"Net flow window=${float(qa_stats.get('net_flow_window', 0.0)):,.0f}, "
        f"Net flow MTD=${float(qa_stats.get('net_flow_mtd', 0.0)):,.0f}, "
        f"Largest flow={qa_stats.get('largest_flow_date', 'N/A')}"
    )
    if breaks.empty:
        print("- Additional breaks: none.")
    else:
        counts = breaks.groupby(["severity", "break_type"]).size().sort_values(ascending=False)
        for (severity, break_type), count in counts.items():
            print(f"- {severity} {break_type}: {int(count)}")
        outlier_rows = breaks[breaks["break_type"] == "RETURN_OUTLIER"]
        if not outlier_rows.empty:
            print("\nOutlier explanations:")
            for _, row in outlier_rows.iterrows():
                print(f"- {row['details']} | cause: {row.get('root_cause', 'N/A')}")

    print("\nIngest QA:")
    failed = ingest[ingest["status"] == "FAIL"]
    if failed.empty:
        print("- All ingest checks passed.")
    else:
        for _, row in failed.iterrows():
            print(f"- {row['check_name']} failed ({int(row['issue_count'])} issues)")

    print("\nHow to read this:")
    print("- TWR is the linked monthly return excluding external-flow distortion.")
    print("- Dietz is a cash-flow-weighted approximation of monthly return.")
    print("- Headline active uses TWR; attribution reconciles to arithmetic active (MTD).")
    print("- Attribution splits active return into allocation, selection, interaction.")
    print(f"- Cash return policy source: {cash_return_source}.")
    print("- Breaks are data/control exceptions you should investigate and resolve.")

    print("\nReport pack:")
    workbook_name = next((name for name in summary.get("files", []) if str(name).lower().endswith(".xlsx")), "report.xlsx")
    print(f"- {out_dir / 'onepager.pdf'}")
    print(f"- {out_dir / workbook_name}")
    print(f"- {out_dir / 'tearsheet.png'}")
    print(f"- {out_dir / 'onepager.md'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show PBOR-Lite output summary for a month.")
    parser.add_argument("--month", required=True, help="Output month folder (YYYY-MM).")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to PBOR-Lite project root.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    show_results(project_root=Path(args.project_root), month=args.month)


if __name__ == "__main__":
    main()
