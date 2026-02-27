from __future__ import annotations

import argparse
import sqlite3
from datetime import timezone
from pathlib import Path

import pandas as pd

from pbor.date_source import derive_date_context
from .attribution import compute_monthly_attribution
from .export import export_outputs
from .ingest import initialize_db, ingest_qa_summary, load_inputs, load_policy, load_tables
from .qa import run_break_checks
from .reconciliation import latest_reconciliation
from .report import generate_tear_sheet
from .returns import compute_returns


def _replace_table(conn: sqlite3.Connection, table_name: str, frame: pd.DataFrame) -> None:
    conn.execute(f"DELETE FROM {table_name}")
    if not frame.empty:
        frame.to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()


def run_month_end(
    project_root: Path,
    asof_date: str,
    data_dir: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, object]:
    project_root = project_root.resolve()
    data_dir = data_dir or (project_root / "data")
    db_path = db_path or (project_root / "pbor_lite.db")
    sql_dir = project_root / "sql"

    policy = load_policy(project_root / "policy.yaml")
    requested_asof = pd.to_datetime(asof_date).date()
    asof = requested_asof
    reconciliation_tolerance_bps = float(policy.get("attribution_reconciliation_tolerance_bps", 5.0))
    cash_return_source = str(policy.get("cash_return_source", "0%"))
    cash_return_annual_rates = policy.get("cash_return_annual_rates", {})

    inputs = load_inputs(data_dir)
    ingest_qa = ingest_qa_summary(inputs, base_currency=str(policy["base_currency"]))

    conn = initialize_db(
        db_path=db_path,
        ddl_path=sql_dir / "ddl.sql",
        views_path=sql_dir / "views.sql",
    )
    load_tables(conn, inputs)

    positions, daily_returns, monthly_returns, _ = compute_returns(
        inputs=inputs,
        policy=policy,
        asof_date=pd.to_datetime(asof),
    )
    attribution = compute_monthly_attribution(
        positions=positions,
        security_master=inputs["security_master.csv"],
        benchmark_weights=inputs["benchmark_weights.csv"],
        benchmark_returns=inputs["benchmark_returns.csv"],
        benchmark_id_default=str(policy.get("benchmark_id_default", "BM1")),
        transactions=inputs["transactions.csv"],
        cash_return_source=cash_return_source,
        cash_return_annual_rates=cash_return_annual_rates if isinstance(cash_return_annual_rates, dict) else {},
    )
    breaks = run_break_checks(
        asof_date=asof,
        policy=policy,
        inputs=inputs,
        positions=positions,
        daily_returns=daily_returns,
        monthly_returns=monthly_returns,
    )
    recon_latest = latest_reconciliation(
        monthly_returns=monthly_returns,
        attribution=attribution,
        tolerance_bps=reconciliation_tolerance_bps,
    )
    if recon_latest["available"] and not recon_latest["within_tolerance"]:
        extra = [
            {
                "asof_date": asof,
                "portfolio_id": recon_latest["portfolio_id"],
                "break_type": "ATTRIBUTION_RECONCILIATION_FAIL",
                "severity": "HIGH",
                "details": (
                    f"Attribution diff {recon_latest['diff_bps']:.1f} bps exceeds "
                    f"{reconciliation_tolerance_bps:.1f} bps."
                ),
                "root_cause": "Attribution and performance return bases are not aligned.",
                "resolution": "Align attribution window and arithmetic return definition before publishing.",
            }
        ]
        if not recon_latest["portfolio_return_ok"]:
            extra.append(
                {
                    "asof_date": asof,
                    "portfolio_id": recon_latest["portfolio_id"],
                    "break_type": "SECTOR_CONTRIBUTION_RECONCILIATION_FAIL",
                    "severity": "HIGH",
                    "details": (
                        f"Sector-vs-portfolio return diff {recon_latest['portfolio_return_diff_bps']:.1f} bps exceeds "
                        f"{reconciliation_tolerance_bps:.1f} bps."
                    ),
                    "root_cause": "Sector contribution construction is not consistent with portfolio return base.",
                    "resolution": "Rebuild sector returns/weights from the same period and valuation base.",
                }
            )
        breaks = pd.concat([breaks, pd.DataFrame(extra)], ignore_index=True)

    _replace_table(conn, "pbor_daily_positions", positions)
    _replace_table(conn, "pbor_daily_returns", daily_returns)
    monthly_for_db = monthly_returns[
        [
            "month_end",
            "portfolio_id",
            "portfolio_return_twr",
            "portfolio_return_dietz",
            "dietz_denominator",
            "benchmark_return",
            "active_return",
        ]
    ].copy()
    _replace_table(conn, "pbor_monthly_returns", monthly_for_db)
    _replace_table(conn, "pbor_attribution_monthly", attribution)
    _replace_table(conn, "pbor_breaks", breaks)

    date_ctx = derive_date_context(
        daily_returns=daily_returns,
        exchange="XNYS",
        now_utc=pd.Timestamp.now(tz=timezone.utc).to_pydatetime(),
        cache_dir=project_root / "data" / "calendar_cache",
        clamp_to_market=True,
    )
    window_ctx = {
        "asof_date": date_ctx["asof_date"],
        "analysis_window": date_ctx["analysis_window"],
        "mtd_window": date_ctx["mtd_window"],
    }
    asof_effective = str(window_ctx["asof_date"])
    market_asof = date_ctx.get("market_last_closed_session")

    export_dir = export_outputs(
        output_root=project_root / "outputs",
        asof_date=asof_effective,
        daily_returns=daily_returns,
        monthly_returns=monthly_returns,
        attribution=attribution,
        breaks=breaks,
        ingest_qa=ingest_qa,
        reconciliation_tolerance_bps=reconciliation_tolerance_bps,
        cash_return_source=cash_return_source,
        date_context=date_ctx,
    )
    png_path, pdf_path = generate_tear_sheet(
        output_dir=export_dir,
        asof_date=asof_effective,
        daily_returns=daily_returns,
        monthly_returns=monthly_returns,
        attribution=attribution,
        breaks=breaks,
        reconciliation_tolerance_bps=reconciliation_tolerance_bps,
        cash_return_source=cash_return_source,
        date_context=date_ctx,
    )
    conn.close()

    return {
        "asof_requested": str(requested_asof),
        "asof_market": str(market_asof) if market_asof is not None else "N/A",
        "asof_data": str(date_ctx.get("data_asof_date", "N/A")),
        "asof_effective": asof_effective,
        "positions_rows": len(positions),
        "daily_returns_rows": len(daily_returns),
        "monthly_returns_rows": len(monthly_returns),
        "attribution_rows": len(attribution),
        "break_rows": len(breaks),
        "exports_path": str(export_dir),
        "tearsheet_png": str(png_path),
        "tearsheet_pdf": str(pdf_path),
        "attribution_reconciliation_tolerance_bps": reconciliation_tolerance_bps,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PBOR-Lite month-end pipeline.")
    parser.add_argument("--asof", required=True, help="As-of date (YYYY-MM-DD).")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to PBOR-Lite project root.",
    )
    parser.add_argument("--data-dir", default=None, help="Optional override for input CSV directory.")
    parser.add_argument("--db-path", default=None, help="Optional override for SQLite DB path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_month_end(
        project_root=Path(args.project_root),
        asof_date=args.asof,
        data_dir=Path(args.data_dir) if args.data_dir else None,
        db_path=Path(args.db_path) if args.db_path else None,
    )
    print("PBOR-Lite run completed.")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
