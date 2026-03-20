from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from openpyxl.utils import get_column_letter

from pbor.date_source import derive_date_context
from .qa import flow_summary_stats, format_flow_summary_line
from .reconciliation import attribution_reconciliation, latest_reconciliation


def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _bps(value: float) -> str:
    return f"{value * 10000:.1f} bps"


def _dataset_label(daily_returns: pd.DataFrame, attribution: pd.DataFrame) -> str:
    if len(daily_returns) <= 31 or len(attribution) <= 10:
        return "Demo dataset"
    return "Production-scale dataset"


def _window_rows(daily_returns: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if daily_returns.empty:
        return daily_returns.copy()
    frame = daily_returns.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
    except Exception:
        return frame
    return frame[(frame["date"] >= start) & (frame["date"] <= end)].copy()


def _parse_cash_return_annualized(cash_return_source: str) -> float:
    source = str(cash_return_source).strip().upper()
    if source in {"0%", "0", "ZERO", "NONE", ""}:
        return 0.0
    if source.endswith("%"):
        try:
            return float(source[:-1]) / 100.0
        except ValueError:
            return 0.0
    return 0.0


def _linked_return(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return float((1.0 + series.astype(float).fillna(0.0)).prod() - 1.0)


def _period_return_rows(daily_returns: pd.DataFrame) -> list[dict[str, float | int | str]]:
    if daily_returns.empty:
        return []
    perf = daily_returns.copy().sort_values("date")
    perf["date"] = pd.to_datetime(perf["date"])
    asof = perf["date"].max()
    qtd_start = asof.to_period("Q").start_time
    ytd_start = pd.Timestamp(year=asof.year, month=1, day=1)
    periods = [
        ("MTD", perf["date"].dt.to_period("M") == asof.to_period("M")),
        ("QTD", perf["date"] >= qtd_start),
        ("YTD", perf["date"] >= ytd_start),
    ]
    rows: list[dict[str, float | int | str]] = []
    for label, mask in periods:
        block = perf[mask].copy()
        if block.empty:
            continue
        portfolio = _linked_return(block["daily_return"])
        benchmark = _linked_return(block["benchmark_return"])
        rows.append(
            {
                "period": label,
                "portfolio": portfolio,
                "benchmark": benchmark,
                "active_twr": portfolio - benchmark,
                "days": int(len(block)),
            }
        )
    return rows


def _risk_metrics(daily_returns: pd.DataFrame, cash_return_source: str) -> dict[str, float]:
    if daily_returns.empty:
        return {
            "tracking_error": float("nan"),
            "information_ratio": float("nan"),
            "sharpe": float("nan"),
            "volatility": float("nan"),
        }
    perf = daily_returns.copy().sort_values("date")
    portfolio = perf["daily_return"].astype(float).fillna(0.0)
    benchmark = perf["benchmark_return"].astype(float).fillna(0.0)
    active = portfolio - benchmark
    rf_annual = _parse_cash_return_annualized(cash_return_source)
    rf_daily = (1.0 + rf_annual) ** (1.0 / 252.0) - 1.0
    excess = portfolio - rf_daily
    active_mean_ann = float(active.mean()) * 252.0
    active_std_ann = float(active.std(ddof=0)) * math.sqrt(252.0)
    vol_ann = float(portfolio.std(ddof=0)) * math.sqrt(252.0)
    excess_mean_ann = float(excess.mean()) * 252.0
    sharpe = excess_mean_ann / vol_ann if vol_ann > 1e-12 else float("nan")
    info_ratio = active_mean_ann / active_std_ann if active_std_ann > 1e-12 else float("nan")
    return {
        "tracking_error": active_std_ann,
        "information_ratio": info_ratio,
        "sharpe": sharpe,
        "volatility": vol_ann,
    }


def _fmt_ratio(value: float) -> str:
    return f"{value:.2f}" if math.isfinite(value) else "N/A"


def _fmt_pct_metric(value: float) -> str:
    return f"{value * 100:.2f}%" if math.isfinite(value) else "N/A"


def _json_float_or_none(value: float) -> float | None:
    return float(value) if math.isfinite(float(value)) else None


def _analyst_commentary(month_attr: pd.DataFrame, recon: dict[str, object], cash_return_source: str) -> str:
    if not recon["available"] or not recon["within_tolerance"] or month_attr.empty:
        return "Attribution withheld pending reconciliation; performance remains preliminary."
    selection_bps = float(month_attr["selection_effect"].sum()) * 10000.0
    interaction_bps = float(month_attr["interaction_effect"].sum()) * 10000.0
    allocation_bps = float(month_attr["allocation_effect"].sum()) * 10000.0
    cash_rows = month_attr[month_attr["sector"].astype(str).str.lower() == "cash"]
    cash_drag_bps = float(cash_rows["active_effect"].sum()) * 10000.0 if not cash_rows.empty else 0.0
    cash_weight = float(cash_rows["w_p"].iloc[0]) * 100.0 if not cash_rows.empty else 0.0
    return (
        f"Selection {selection_bps:+.1f} bps, interaction {interaction_bps:+.1f} bps, "
        f"allocation {allocation_bps:+.1f} bps; cash drag {cash_drag_bps:+.1f} bps at {cash_weight:.1f}% weight."
    )


def _cash_policy_line(cash_return_source: str, benchmark_cash_weight: float) -> str:
    source = str(cash_return_source).strip().upper()
    if source in {"0%", "0", "ZERO", "NONE"}:
        source_text = "0% (policy convention)"
    else:
        source_text = f"{source} (policy-configured annualized proxy)"
    return f"Cash return source = `{source_text}`; benchmark cash weight = `{benchmark_cash_weight:.1f}%`."


def _break_count(breaks: pd.DataFrame, break_type: str) -> int:
    if breaks.empty:
        return 0
    return int((breaks["break_type"] == break_type).sum())


def _outlier_explanations(breaks: pd.DataFrame) -> list[str]:
    if breaks.empty:
        return []
    outliers = breaks[breaks["break_type"] == "RETURN_OUTLIER"].copy()
    if outliers.empty:
        return []
    lines: list[str] = []
    for _, row in outliers.iterrows():
        lines.append(f"{row['details']} | cause: {row.get('root_cause', 'N/A')}")
    return lines


def _build_onepager_markdown(
    asof_date: str,
    daily_returns: pd.DataFrame,
    monthly_returns: pd.DataFrame,
    attribution: pd.DataFrame,
    breaks: pd.DataFrame,
    ingest_qa: pd.DataFrame,
    reconciliation_tolerance_bps: float,
    cash_return_source: str,
    date_context: dict[str, object] | None = None,
) -> str:
    recon = latest_reconciliation(
        monthly_returns=monthly_returns,
        attribution=attribution,
        tolerance_bps=reconciliation_tolerance_bps,
    )
    lines: list[str] = []
    window_ctx = date_context or derive_date_context(daily_returns=daily_returns, clamp_to_market=True)
    asof_effective = str(window_ctx["asof_date"])
    data_asof_date = str(window_ctx.get("data_asof_date", asof_effective))
    generated_at_et = str(window_ctx.get("generated_at_et", "N/A"))
    market_last_closed = window_ctx.get("market_last_closed_session")
    analysis_window = window_ctx["analysis_window"]
    mtd_window = window_ctx["mtd_window"]
    mtd_rows = _window_rows(
        daily_returns=daily_returns,
        start_date=str(mtd_window["start"]),
        end_date=str(mtd_window["end"]),
    )
    flow_analysis = flow_summary_stats(
        daily_returns=daily_returns,
        start_date=str(analysis_window["start"]),
        end_date=str(analysis_window["end"]),
    )
    flow_mtd = flow_summary_stats(
        daily_returns=daily_returns,
        start_date=str(mtd_window["start"]),
        end_date=str(mtd_window["end"]),
    )
    period_rows = _period_return_rows(daily_returns)
    risk = _risk_metrics(daily_returns=daily_returns, cash_return_source=cash_return_source)
    lines.append("# PBOR-Lite One-Pager")
    lines.append("")
    lines.append(f"As-of (data): {data_asof_date}")
    lines.append(f"Generated: {generated_at_et}")
    if market_last_closed:
        lines.append(f"Market last closed session: {market_last_closed}")
    lines.append(f"Dataset: {_dataset_label(daily_returns=daily_returns, attribution=attribution)}")
    lines.append("Data note: market data + synthetic transaction ledger for demonstration.")
    lines.append(
        "Analysis window: "
        f"{analysis_window['start']} to {analysis_window['end']} "
        f"({int(analysis_window['obs_rows'])} obs rows | {int(analysis_window['trading_days'])} trading days)"
    )
    lines.append(
        "MTD window: "
        f"{mtd_window['start']} to {mtd_window['end']} "
        f"({int(mtd_window['obs_rows'])} obs rows | {int(mtd_window['trading_days'])} trading days)"
    )
    lines.append("")

    if monthly_returns.empty:
        lines.append("No monthly returns available for this run.")
        lines.append("")
    else:
        lines.append("## Monthly Performance")
        lines.append("")
        for _, row in monthly_returns.sort_values("month_end").iterrows():
            metric_line = (
                f"- Portfolio `{row['portfolio_id']}` | "
                f"TWR `{_pct(float(row['portfolio_return_twr']))}` | "
                f"Modified Dietz `{_pct(float(row['portfolio_return_dietz']))}` | "
                f"Benchmark `{_pct(float(row['benchmark_return']))}` | "
                f"Active (TWR) `{_pct(float(row['active_return']))}`"
            )
            if "active_return_arithmetic" in row.index:
                metric_line += f" | Active (arith, MTD) `{_pct(float(row['active_return_arithmetic']))}`"
            lines.append(metric_line)
        lines.append("")
    lines.append("## Linked Multi-Period Returns")
    lines.append("")
    if not period_rows:
        lines.append("- No daily return history available.")
    else:
        for row in period_rows:
            lines.append(
                f"- `{row['period']}` ({int(row['days'])} days): "
                f"Portfolio `{_pct(float(row['portfolio']))}` | "
                f"Benchmark `{_pct(float(row['benchmark']))}` | "
                f"Active (TWR) `{_pct(float(row['active_twr']))}`"
            )
    lines.append("")

    lines.append("## Risk Metrics (Annualized)")
    lines.append("")
    lines.append(f"- Tracking error: `{_fmt_pct_metric(float(risk['tracking_error']))}`")
    lines.append(f"- Information ratio: `{_fmt_ratio(float(risk['information_ratio']))}`")
    lines.append(f"- Sharpe ratio: `{_fmt_ratio(float(risk['sharpe']))}`")
    lines.append(f"- Volatility: `{_fmt_pct_metric(float(risk['volatility']))}`")
    lines.append("")

    lines.append("## Attribution Reconciliation Gate")
    lines.append("")
    if recon["available"]:
        diff_ok = float(recon["diff_bps"]) < reconciliation_tolerance_bps
        weights_ok = bool(recon["weights_ok"])
        sector_ok = bool(recon["portfolio_return_ok"])
        all_ok = diff_ok and weights_ok and sector_ok
        status_line = "Validated" if all_ok else "Data Under Review"
        lines.append(f"- Status: `{status_line}`")
        lines.append(f"- Attribution-Active diff: `{float(recon['diff_bps']):.1f} bps`")
        lines.append(f"- Sum weights: `Wp={float(recon['w_p_sum']):.2f}`, `Wb={float(recon['w_b_sum']):.2f}`")
        lines.append(f"- Sector->Portfolio diff: `{float(recon['portfolio_return_diff_bps']):.1f} bps`")
    else:
        lines.append("- Attribution reconciliation unavailable for this run.")
    lines.append("")

    lines.append("## Attribution")
    lines.append("")
    if attribution.empty:
        lines.append("- No attribution rows generated.")
    elif not bool(recon["within_tolerance"]):
        lines.append("- Attribution withheld pending reconciliation.")
    else:
        latest_month = recon["month_end"]
        month_attr = attribution[attribution["month_end"] == latest_month].copy()
        month_attr["active_effect_bps"] = month_attr["active_effect"] * 10000.0
        month_attr = month_attr.sort_values(["active_effect_bps", "sector"], ascending=[False, True], kind="mergesort")
        lines.append(f"- Attribution reconciles to Active (arith, MTD) = `{_pct(float(recon['active_return']))}`.")
        for _, row in month_attr.iterrows():
            lines.append(
                f"- Sector `{row['sector']}` | "
                f"Wp `{float(row['w_p']) * 100:.1f}%` vs Wb `{float(row['w_b']) * 100:.1f}%` | "
                f"Alloc `{_bps(float(row['allocation_effect']))}` | "
                f"Sel `{_bps(float(row['selection_effect']))}` | "
                f"Int `{_bps(float(row['interaction_effect']))}` | "
                f"Active `{_bps(float(row['active_effect']))}`"
            )
        lines.append(
            f"- Total | Alloc `{_bps(float(month_attr['allocation_effect'].sum()))}` | "
            f"Sel `{_bps(float(month_attr['selection_effect'].sum()))}` | "
            f"Int `{_bps(float(month_attr['interaction_effect'].sum()))}` | "
            f"Active `{_bps(float(month_attr['active_effect'].sum()))}`"
        )
        selection_bps = float(month_attr["selection_effect"].sum()) * 10000.0
        interaction_bps = float(month_attr["interaction_effect"].sum()) * 10000.0
        cash_drag_bps = (
            float(month_attr.loc[month_attr["sector"].astype(str).str.lower() == "cash", "active_effect"].sum()) * 10000.0
        )
        lines.append(
            f"- Driver: Stock selection (`{selection_bps:+.1f} bps`) offset by interaction "
            f"(`{interaction_bps:+.1f} bps`) and cash drag (`{cash_drag_bps:+.1f} bps`)."
        )
        cash_row = month_attr[month_attr["sector"].astype(str).str.lower() == "cash"]
        cash_w_b = float(cash_row.iloc[0]["w_b"]) * 100.0 if not cash_row.empty else 0.0
        lines.append(f"- Cash methodology: {_cash_policy_line(cash_return_source=cash_return_source, benchmark_cash_weight=cash_w_b)}")
        lines.append("")
        lines.append("Top contributors:")
        top_positive = (
            month_attr[month_attr["active_effect_bps"] > 0]
            .sort_values(["active_effect_bps", "sector"], ascending=[False, True], kind="mergesort")
            .head(3)
        )
        if top_positive.empty:
            lines.append("- None")
        else:
            for _, row in top_positive.iterrows():
                lines.append(f"- {row['sector']}: `{_bps(float(row['active_effect']))}`")
        lines.append("Top detractors:")
        top_negative = (
            month_attr[month_attr["active_effect_bps"] < 0]
            .sort_values(["active_effect_bps", "sector"], ascending=[True, True], kind="mergesort")
            .head(3)
        )
        if top_negative.empty:
            lines.append("- None")
        else:
            for _, row in top_negative.iterrows():
                lines.append(f"- {row['sector']}: `{_bps(float(row['active_effect']))}`")
        lines.append("")
        lines.append("## Analyst Commentary")
        lines.append("")
        lines.append(f"- {_analyst_commentary(month_attr=month_attr, recon=recon, cash_return_source=cash_return_source)}")
    lines.append("")

    lines.append("## Return Outlier Explanations")
    lines.append("")
    outlier_lines = _outlier_explanations(breaks)
    if not outlier_lines:
        lines.append("- No return outliers detected.")
    else:
        for line in outlier_lines:
            lines.append(f"- {line}")
    lines.append("")

    lines.append("## Breaks / QA")
    lines.append("")
    lines.append(f"- Return outliers: `{_break_count(breaks, 'RETURN_OUTLIER')}`")
    lines.append(f"- NAV jump / zero-flow flags: `{_break_count(breaks, 'NAV_JUMP_ZERO_FLOW')}`")
    lines.append(f"- Missing prices: `{_break_count(breaks, 'MISSING_PRICE')}`")
    lines.append(f"- Net flow (Analysis window): `${float(flow_analysis['net_flow']):,.0f}`")
    lines.append(
        f"- {format_flow_summary_line(mtd_rows, start_date=None, end_date=None, window_label='MTD')}"
    )
    if breaks.empty:
        lines.append("- Additional breaks: none.")
    else:
        break_counts = breaks.groupby(["severity", "break_type"], as_index=False).size().sort_values(
            ["severity", "size"], ascending=[True, False]
        )
        for _, row in break_counts.iterrows():
            lines.append(f"- `{row['severity']}` `{row['break_type']}`: {int(row['size'])}")
    lines.append("")

    fail_checks = ingest_qa[ingest_qa["status"] == "FAIL"]
    lines.append("## Ingest QA")
    lines.append("")
    if fail_checks.empty:
        lines.append("- All ingest checks passed.")
    else:
        for _, row in fail_checks.iterrows():
            lines.append(f"- `{row['check_name']}` failed with {int(row['issue_count'])} issues.")
    lines.append("")

    lines.append("## Files")
    lines.append("")
    lines.append("- `daily_returns.csv`: daily portfolio and benchmark return series.")
    lines.append("- `monthly_returns.csv`: monthly TWR, Dietz, benchmark, active (TWR), and arithmetic return fields.")
    lines.append("- `attribution.csv`: sector-level Brinson-Fachler effects.")
    lines.append("- `attribution_reconciliation.csv`: attribution-to-active pass/fail control.")
    lines.append("- `breaks.csv`: detected data/logic breaks with severity and notes.")
    lines.append("- `qa_ingest_summary.csv`: ingestion validation checks.")
    lines.append("- `report*.xlsx`: workbook with Summary, Returns, Attribution, and Breaks tabs.")
    lines.append("- `onepager.pdf`: one-page executive tear sheet.")
    return "\n".join(lines) + "\n"


def _build_summary_table(
    asof_date: str,
    daily_returns: pd.DataFrame,
    monthly_returns: pd.DataFrame,
    attribution: pd.DataFrame,
    breaks: pd.DataFrame,
    ingest_qa: pd.DataFrame,
    recon_latest: dict[str, object],
    cash_return_source: str,
    date_context: dict[str, object] | None = None,
) -> pd.DataFrame:
    window_ctx = date_context or derive_date_context(daily_returns=daily_returns, clamp_to_market=True)
    asof_effective = str(window_ctx["asof_date"])
    data_asof_date = str(window_ctx.get("data_asof_date", asof_effective))
    generated_at_utc = str(window_ctx.get("generated_at_utc", "N/A"))
    generated_at_et = str(window_ctx.get("generated_at_et", "N/A"))
    market_last_closed = window_ctx.get("market_last_closed_session")
    analysis_window = window_ctx["analysis_window"]
    mtd_window = window_ctx["mtd_window"]
    period_rows = _period_return_rows(daily_returns)
    risk = _risk_metrics(daily_returns=daily_returns, cash_return_source=cash_return_source)
    flow_window = flow_summary_stats(
        daily_returns=daily_returns,
        start_date=str(analysis_window["start"]),
        end_date=str(analysis_window["end"]),
    )
    flow_mtd = flow_summary_stats(
        daily_returns=daily_returns,
        start_date=str(mtd_window["start"]),
        end_date=str(mtd_window["end"]),
    )
    rows: list[dict[str, object]] = [
        {"metric": "asof_date", "value": asof_effective},
        {"metric": "data_asof_date", "value": data_asof_date},
        {"metric": "generated_at_utc", "value": generated_at_utc},
        {"metric": "generated_at_et", "value": generated_at_et},
        {"metric": "market_last_closed_session", "value": market_last_closed},
    ]
    if not monthly_returns.empty:
        first = monthly_returns.sort_values("month_end").iloc[-1]
        rows.extend(
            [
                {"metric": "portfolio_id", "value": first["portfolio_id"]},
                {"metric": "portfolio_twr", "value": float(first["portfolio_return_twr"])},
                {"metric": "portfolio_dietz", "value": float(first["portfolio_return_dietz"])},
                {"metric": "benchmark_return", "value": float(first["benchmark_return"])},
                {"metric": "active_return_twr", "value": float(first["active_return"])},
            ]
        )
        if "active_return_arithmetic" in first.index:
            rows.append({"metric": "active_return_arithmetic", "value": float(first["active_return_arithmetic"])})
    rows.extend(
        [
            {"metric": "attribution_sum", "value": float(recon_latest["attribution_sum"])},
            {"metric": "attribution_diff_bps", "value": float(recon_latest["diff_bps"])},
            {"metric": "attribution_within_tolerance", "value": bool(recon_latest["within_tolerance"])},
            {"metric": "daily_rows", "value": int(len(daily_returns))},
            {"metric": "monthly_rows", "value": int(len(monthly_returns))},
            {"metric": "attribution_rows", "value": int(len(attribution))},
            {"metric": "break_rows", "value": int(len(breaks))},
            {"metric": "ingest_fail_checks", "value": int((ingest_qa["status"] == "FAIL").sum())},
            {"metric": "return_outliers", "value": _break_count(breaks, "RETURN_OUTLIER")},
            {"metric": "nav_jump_zero_flow_flags", "value": _break_count(breaks, "NAV_JUMP_ZERO_FLOW")},
            {"metric": "missing_prices", "value": _break_count(breaks, "MISSING_PRICE")},
            {"metric": "analysis_window_start", "value": str(analysis_window["start"])},
            {"metric": "analysis_window_end", "value": str(analysis_window["end"])},
            {"metric": "analysis_obs_rows", "value": int(analysis_window["obs_rows"])},
            {"metric": "analysis_trading_days", "value": int(analysis_window["trading_days"])},
            {"metric": "mtd_window_start", "value": str(mtd_window["start"])},
            {"metric": "mtd_window_end", "value": str(mtd_window["end"])},
            {"metric": "mtd_obs_rows", "value": int(mtd_window["obs_rows"])},
            {"metric": "mtd_trading_days", "value": int(mtd_window["trading_days"])},
            {"metric": "net_flow_window", "value": float(flow_window["net_flow"])},
            {"metric": "net_flow_mtd", "value": float(flow_mtd["net_flow"])},
            {"metric": "largest_flow_date", "value": flow_mtd["largest_flow_date"]},
            {"metric": "largest_flow_amount", "value": float(flow_mtd["largest_flow_amount"])},
            {"metric": "tracking_error_ann", "value": float(risk["tracking_error"])},
            {"metric": "information_ratio_ann", "value": float(risk["information_ratio"])},
            {"metric": "sharpe_ratio_ann", "value": float(risk["sharpe"])},
            {"metric": "volatility_ann", "value": float(risk["volatility"])},
        ]
    )
    for row in period_rows:
        key = str(row["period"]).lower()
        rows.extend(
            [
                {"metric": f"{key}_portfolio_return", "value": float(row["portfolio"])},
                {"metric": f"{key}_benchmark_return", "value": float(row["benchmark"])},
                {"metric": f"{key}_active_return_twr", "value": float(row["active_twr"])},
                {"metric": f"{key}_trading_days", "value": int(row["days"])},
            ]
        )
    return pd.DataFrame(rows)


def _autofit_columns(writer: pd.ExcelWriter, sheet_name: str, frame: pd.DataFrame) -> None:
    sheet = writer.book[sheet_name]
    for idx, col in enumerate(frame.columns, start=1):
        max_len = max(len(str(col)), frame[col].astype(str).map(len).max() if not frame.empty else 0)
        sheet.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 80)


def _export_excel_report(
    target: Path,
    asof_date: str,
    daily_returns: pd.DataFrame,
    monthly_returns: pd.DataFrame,
    attribution: pd.DataFrame,
    attribution_recon: pd.DataFrame,
    breaks: pd.DataFrame,
    ingest_qa: pd.DataFrame,
    recon_latest: dict[str, object],
    cash_return_source: str,
    date_context: dict[str, object] | None = None,
) -> Path:
    summary = _build_summary_table(
        asof_date=asof_date,
        daily_returns=daily_returns,
        monthly_returns=monthly_returns,
        attribution=attribution,
        breaks=breaks,
        ingest_qa=ingest_qa,
        recon_latest=recon_latest,
        cash_return_source=cash_return_source,
        date_context=date_context,
    )

    def _write_workbook(path: Path) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            summary.to_excel(writer, index=False, sheet_name="Summary")
            monthly_returns.to_excel(writer, index=False, sheet_name="MonthlyReturns")
            daily_returns.to_excel(writer, index=False, sheet_name="DailyReturns")
            attribution.to_excel(writer, index=False, sheet_name="Attribution")
            attribution_recon.to_excel(writer, index=False, sheet_name="AttrReconciliation")
            breaks.to_excel(writer, index=False, sheet_name="Breaks")
            ingest_qa.to_excel(writer, index=False, sheet_name="IngestQA")

            _autofit_columns(writer, "Summary", summary)
            _autofit_columns(writer, "MonthlyReturns", monthly_returns)
            _autofit_columns(writer, "DailyReturns", daily_returns)
            _autofit_columns(writer, "Attribution", attribution)
            _autofit_columns(writer, "AttrReconciliation", attribution_recon)
            _autofit_columns(writer, "Breaks", breaks)
            _autofit_columns(writer, "IngestQA", ingest_qa)

    report_path = target / "report.xlsx"
    try:
        _write_workbook(report_path)
        return report_path
    except PermissionError:
        alt_name = f"report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        alt_path = target / alt_name
        _write_workbook(alt_path)
        return alt_path


def _export_controls_table_image(
    target: Path,
    recon_latest: dict[str, object],
) -> Path:
    controls_path = target / "controls_table.png"
    fig, ax = plt.subplots(figsize=(6.8, 2.2))
    ax.axis("off")
    rows = [
        ["Attribution-Active diff", f"{float(recon_latest['diff_bps']):.1f} bps"],
        ["Sum weights", f"Wp={float(recon_latest['w_p_sum']):.2f}, Wb={float(recon_latest['w_b_sum']):.2f}"],
        ["Sector->Portfolio diff", f"{float(recon_latest['portfolio_return_diff_bps']):.1f} bps"],
    ]
    table = ax.table(
        cellText=rows,
        colLabels=["Control", "Value"],
        loc="center",
        cellLoc="left",
        colWidths=[0.52, 0.48],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.0, 1.25)
    for (r, _), cell in table.get_celld().items():
        cell.set_linewidth(0.35)
        cell.set_edgecolor("#6D7785")
        if r == 0:
            cell.set_facecolor("#F1F5F9")
    status = "Validated" if bool(recon_latest.get("within_tolerance", False)) else "Data Under Review"
    ax.set_title(f"Controls Check ({status})", fontsize=11, loc="left", pad=6)
    fig.savefig(controls_path, dpi=180)
    plt.close(fig)
    return controls_path


def export_outputs(
    output_root: Path,
    asof_date: str,
    daily_returns: pd.DataFrame,
    monthly_returns: pd.DataFrame,
    attribution: pd.DataFrame,
    breaks: pd.DataFrame,
    ingest_qa: pd.DataFrame,
    reconciliation_tolerance_bps: float = 5.0,
    cash_return_source: str = "0%",
    date_context: dict[str, object] | None = None,
) -> Path:
    window_ctx = date_context or derive_date_context(daily_returns=daily_returns, clamp_to_market=True)
    asof_effective = str(window_ctx["asof_date"])
    data_asof_date = str(window_ctx.get("data_asof_date", asof_effective))
    generated_at_utc = str(window_ctx.get("generated_at_utc", "N/A"))
    generated_at_et = str(window_ctx.get("generated_at_et", "N/A"))
    market_last_closed = window_ctx.get("market_last_closed_session")
    analysis_window = window_ctx["analysis_window"]
    mtd_window = window_ctx["mtd_window"]
    flow_window = flow_summary_stats(
        daily_returns=daily_returns,
        start_date=str(analysis_window["start"]),
        end_date=str(analysis_window["end"]),
    )
    flow_mtd = flow_summary_stats(
        daily_returns=daily_returns,
        start_date=str(mtd_window["start"]),
        end_date=str(mtd_window["end"]),
    )

    month_folder = pd.to_datetime(asof_effective).strftime("%Y-%m")
    target = output_root / month_folder
    target.mkdir(parents=True, exist_ok=True)

    attribution_recon = attribution_reconciliation(
        monthly_returns=monthly_returns,
        attribution=attribution,
        tolerance_bps=reconciliation_tolerance_bps,
    )
    recon_latest = latest_reconciliation(
        monthly_returns=monthly_returns,
        attribution=attribution,
        tolerance_bps=reconciliation_tolerance_bps,
    )

    daily_returns.to_csv(target / "daily_returns.csv", index=False)
    monthly_returns.to_csv(target / "monthly_returns.csv", index=False)
    attribution.to_csv(target / "attribution.csv", index=False)
    attribution_recon.to_csv(target / "attribution_reconciliation.csv", index=False)
    breaks.to_csv(target / "breaks.csv", index=False)
    ingest_qa.to_csv(target / "qa_ingest_summary.csv", index=False)

    onepager_md = _build_onepager_markdown(
        asof_date=asof_effective,
        daily_returns=daily_returns,
        monthly_returns=monthly_returns,
        attribution=attribution,
        breaks=breaks,
        ingest_qa=ingest_qa,
        reconciliation_tolerance_bps=reconciliation_tolerance_bps,
        cash_return_source=cash_return_source,
        date_context=window_ctx,
    )
    (target / "onepager.md").write_text(onepager_md, encoding="utf-8")
    report_workbook = _export_excel_report(
        target=target,
        asof_date=asof_effective,
        daily_returns=daily_returns,
        monthly_returns=monthly_returns,
        attribution=attribution,
        attribution_recon=attribution_recon,
        breaks=breaks,
        ingest_qa=ingest_qa,
        recon_latest=recon_latest,
        cash_return_source=cash_return_source,
        date_context=window_ctx,
    )
    controls_table_image = _export_controls_table_image(
        target=target,
        recon_latest=recon_latest,
    )

    period_rows = _period_return_rows(daily_returns)
    risk = _risk_metrics(daily_returns=daily_returns, cash_return_source=cash_return_source)
    summary_payload = {
        "asof_date": asof_effective,
        "data_asof_date": data_asof_date,
        "generated_at_utc": generated_at_utc,
        "generated_at_et": generated_at_et,
        "market_last_closed_session": market_last_closed,
        "reconciliation_tolerance_bps": float(reconciliation_tolerance_bps),
        "data_status": "Validated" if bool(recon_latest["within_tolerance"]) else "Data Under Review",
        "dataset_label": _dataset_label(daily_returns=daily_returns, attribution=attribution),
        "data_note": "market data + synthetic transaction ledger for demonstration",
        "analysis_window": {
            "start": str(analysis_window["start"]),
            "end": str(analysis_window["end"]),
            "obs_rows": int(analysis_window["obs_rows"]),
            "trading_days": int(analysis_window["trading_days"]),
        },
        "mtd_window": {
            "start": str(mtd_window["start"]),
            "end": str(mtd_window["end"]),
            "obs_rows": int(mtd_window["obs_rows"]),
            "trading_days": int(mtd_window["trading_days"]),
        },
        "cash_return_source": str(cash_return_source),
        "linked_returns": [
            {
                "period": str(r["period"]),
                "portfolio": float(r["portfolio"]),
                "benchmark": float(r["benchmark"]),
                "active_twr": float(r["active_twr"]),
                "days": int(r["days"]),
            }
            for r in period_rows
        ],
        "risk_metrics_annualized": {
            "tracking_error": _json_float_or_none(float(risk["tracking_error"])),
            "information_ratio": _json_float_or_none(float(risk["information_ratio"])),
            "sharpe": _json_float_or_none(float(risk["sharpe"])),
            "volatility": _json_float_or_none(float(risk["volatility"])),
        },
        "attribution_reconciliation": {
            "attribution_sum": float(recon_latest["attribution_sum"]),
            "active_return_arithmetic": float(recon_latest["active_return"]),
            "diff_bps": float(recon_latest["diff_bps"]),
            "w_p_sum": float(recon_latest["w_p_sum"]),
            "w_b_sum": float(recon_latest["w_b_sum"]),
            "weights_ok": bool(recon_latest["weights_ok"]),
            "portfolio_return_reference": float(recon_latest["portfolio_return_reference"]),
            "portfolio_return_from_sectors": float(recon_latest["portfolio_return_from_sectors"]),
            "portfolio_return_diff_bps": float(recon_latest["portfolio_return_diff_bps"]),
            "portfolio_return_ok": bool(recon_latest["portfolio_return_ok"]),
            "within_tolerance": bool(recon_latest["within_tolerance"]),
        },
        "rows": {
            "daily_returns": int(len(daily_returns)),
            "monthly_returns": int(len(monthly_returns)),
            "attribution": int(len(attribution)),
            "breaks": int(len(breaks)),
        },
        "qa_stats": {
            "return_outliers": _break_count(breaks, "RETURN_OUTLIER"),
            "nav_jump_zero_flow_flags": _break_count(breaks, "NAV_JUMP_ZERO_FLOW"),
            "missing_prices": _break_count(breaks, "MISSING_PRICE"),
            "net_flow_window": float(flow_window["net_flow"]),
            "net_flow_mtd": float(flow_mtd["net_flow"]),
            "largest_flow_date": flow_mtd["largest_flow_date"],
            "largest_flow_amount": float(flow_mtd["largest_flow_amount"]),
            "flows_present": bool(flow_mtd["flows_present"]),
        },
        "files": [
            "daily_returns.csv",
            "monthly_returns.csv",
            "attribution.csv",
            "attribution_reconciliation.csv",
            "breaks.csv",
            "qa_ingest_summary.csv",
            "onepager.md",
            report_workbook.name,
            controls_table_image.name,
            "tearsheet.png",
            "onepager.pdf",
        ],
    }
    (target / "summary.json").write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return target
