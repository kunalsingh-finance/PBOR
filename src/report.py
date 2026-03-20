from __future__ import annotations

import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from pbor.date_source import derive_date_context
from .qa import format_flow_summary_line
from .reconciliation import latest_reconciliation

matplotlib.rcParams["font.family"] = "DejaVu Sans"
matplotlib.rcParams["font.size"] = 9
if "text.parse_math" in matplotlib.rcParams:
    matplotlib.rcParams["text.parse_math"] = False


def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _bps(value: float) -> str:
    return f"{value * 10000:.1f} bps"


def _portfolio_summary(monthly_returns: pd.DataFrame) -> str:
    if monthly_returns.empty:
        return "No monthly performance rows."
    row = monthly_returns.sort_values("month_end").iloc[-1]
    active_arith = (
        f"  |  Active (arith, MTD) {_pct(float(row['active_return_arithmetic']))}"
        if "active_return_arithmetic" in row.index
        else ""
    )
    return (
        f"{row['portfolio_id']}  |  "
        f"TWR {_pct(float(row['portfolio_return_twr']))}  |  "
        f"Dietz {_pct(float(row['portfolio_return_dietz']))}  |  "
        f"BM {_pct(float(row['benchmark_return']))}  |  "
        f"Active(TWR) {_pct(float(row['active_return']))}"
        f"{active_arith}"
    )


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


def _analyst_commentary(month_attr: pd.DataFrame, recon: dict[str, object], cash_return_source: str) -> str:
    if not recon["available"] or not recon["within_tolerance"] or month_attr.empty:
        return "Analyst commentary: Attribution withheld pending reconciliation; performance remains preliminary."
    selection_bps = float(month_attr["selection_effect"].sum()) * 10000.0
    interaction_bps = float(month_attr["interaction_effect"].sum()) * 10000.0
    allocation_bps = float(month_attr["allocation_effect"].sum()) * 10000.0
    cash_rows = month_attr[month_attr["sector"].astype(str).str.lower() == "cash"]
    cash_drag_bps = float(cash_rows["active_effect"].sum()) * 10000.0 if not cash_rows.empty else 0.0
    cash_weight = float(cash_rows["w_p"].iloc[0]) * 100.0 if not cash_rows.empty else 0.0
    return (
        f"Analyst commentary: Selection {selection_bps:+.1f} bps, interaction {interaction_bps:+.1f} bps, "
        f"allocation {allocation_bps:+.1f} bps; cash drag {cash_drag_bps:+.1f} bps at {cash_weight:.1f}% weight."
    )


def _control_lines(recon: dict[str, object], tolerance_bps: float) -> tuple[str, list[str]]:
    if not recon["available"]:
        return "Status: Data Under Review", ["Controls unavailable for this run."]

    diff_ok = float(recon["diff_bps"]) < tolerance_bps
    weights_ok = bool(recon["weights_ok"])
    sector_ok = bool(recon["portfolio_return_ok"])
    all_ok = diff_ok and weights_ok and sector_ok

    status = "Status: Validated" if all_ok else "Status: Data Under Review"
    lines = [
        f"Attribution-Active diff: {float(recon['diff_bps']):.1f} bps",
        f"Sum weights: Wp={float(recon['w_p_sum']):.2f}, Wb={float(recon['w_b_sum']):.2f}",
        f"Sector->Portfolio diff: {float(recon['portfolio_return_diff_bps']):.1f} bps",
    ]
    return status, lines


def _plot_cumulative_performance(ax: plt.Axes, daily_returns: pd.DataFrame) -> None:
    ax.set_title("Cumulative Portfolio vs Benchmark Return", fontsize=11)
    if daily_returns.empty:
        ax.text(0.5, 0.5, "No daily return data", ha="center", va="center")
        return

    perf = daily_returns.copy().sort_values("date")
    perf["date"] = pd.to_datetime(perf["date"])
    perf["portfolio_cum"] = (1.0 + perf["daily_return"].fillna(0.0)).cumprod() - 1.0
    perf["benchmark_cum"] = (1.0 + perf["benchmark_return"].fillna(0.0)).cumprod() - 1.0

    ax.plot(perf["date"], perf["portfolio_cum"] * 100, label="Portfolio", linewidth=1.8)
    ax.plot(perf["date"], perf["benchmark_cum"] * 100, label="Benchmark", linewidth=1.4)
    ax.set_ylabel("Cumulative Return (%)")
    if len(perf) >= 90:
        locator = mdates.MonthLocator(interval=2)
        formatter = mdates.DateFormatter("%Y-%m")
        ax.tick_params(axis="x", labelrotation=35, labelsize=8, pad=8)
    else:
        locator = mdates.AutoDateLocator(minticks=3, maxticks=6)
        formatter = mdates.DateFormatter("%Y-%m-%d")
        ax.tick_params(axis="x", labelrotation=45, labelsize=8, pad=8)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    ax.tick_params(axis="y", labelsize=8.5)
    ax.grid(alpha=0.3)
    ax.legend(loc="upper left", bbox_to_anchor=(0.0, 1.0), fontsize=8, framealpha=0.9)


def _attribution_top_lines(attribution: pd.DataFrame) -> tuple[list[str], list[str]]:
    if attribution.empty:
        return [], []
    latest_month = attribution["month_end"].max()
    month_attr = attribution[attribution["month_end"] == latest_month].copy()
    top = (
        month_attr[month_attr["active_effect"] > 0]
        .sort_values(["active_effect", "sector"], ascending=[False, True])
        .head(2)
    )
    bottom = (
        month_attr[month_attr["active_effect"] < 0]
        .sort_values(["active_effect", "sector"], ascending=[True, True])
        .head(1)
    )

    contrib_lines = [f"{r['sector']}: {_bps(float(r['active_effect']))}" for _, r in top.iterrows()]
    detract_lines = [f"{r['sector']}: {_bps(float(r['active_effect']))}" for _, r in bottom.iterrows()]
    return contrib_lines, detract_lines


def _plot_attribution(
    ax: plt.Axes,
    attribution: pd.DataFrame,
    recon: dict[str, object],
    tolerance_bps: float,
) -> pd.DataFrame:
    title = "Attribution Active Effect by Sector (bps)"
    if recon["available"] and recon["within_tolerance"]:
        title = f"{title}\nReconciles to Active (arith, MTD) = {_pct(float(recon['active_return']))}"
    ax.set_title(title, fontsize=10.2, pad=8)
    if not recon["available"]:
        ax.axis("off")
        ax.text(0.0, 0.7, "No attribution window available.", fontsize=10)
        return pd.DataFrame()
    if not recon["within_tolerance"]:
        ax.axis("off")
        ax.text(0.0, 0.7, "Attribution withheld pending reconciliation.", fontsize=10, weight="bold")
        ax.text(
            0.0,
            0.5,
            f"Diff {float(recon['diff_bps']):.1f} bps exceeds {tolerance_bps:.1f} bps threshold.",
            fontsize=9,
        )
        return pd.DataFrame()

    latest_month = recon["month_end"]
    month_attr = attribution[attribution["month_end"] == latest_month].copy()
    if month_attr.empty:
        ax.axis("off")
        ax.text(0.5, 0.5, "No attribution rows for latest month.", ha="center", va="center")
        return month_attr

    month_attr["active_bps"] = month_attr["active_effect"] * 10000.0
    month_attr = month_attr.sort_values(["active_bps", "sector"], ascending=[True, True], kind="mergesort")
    labels = [str(r["sector"]) for _, r in month_attr.iterrows()]
    bars = ax.barh(labels, month_attr["active_bps"])
    for bar in bars:
        bar.set_alpha(0.85)
    ax.tick_params(axis="x", labelsize=8)
    ax.tick_params(axis="y", labelsize=8)
    ax.set_xlabel("Active Effect (bps)")
    ax.grid(axis="x", alpha=0.3)
    return month_attr


def _plot_attribution_component_table(ax: plt.Axes, month_attr: pd.DataFrame, recon: dict[str, object]) -> str | None:
    ax.axis("off")
    if not recon["available"]:
        ax.text(0.0, 0.5, "Component table unavailable (no reconciliation window).", fontsize=9, va="center")
        return None
    if not recon["within_tolerance"] or month_attr.empty:
        ax.text(0.0, 0.5, "Component table hidden until reconciliation passes.", fontsize=9, va="center")
        return None

    table_source = month_attr.sort_values(["active_bps", "sector"], ascending=[False, True], kind="mergesort").copy()
    if table_source.empty:
        ax.text(0.0, 0.5, "No attribution rows to display.", fontsize=9, va="center")
        return None

    cols = ["Alloc", "Sel", "Int", "Total"]
    cell_text: list[list[str]] = []
    row_labels: list[str] = []
    for _, row in table_source.iterrows():
        row_labels.append(str(row["sector"]))
        cell_text.append(
            [
                f"{float(row['allocation_effect']) * 10000.0:.1f}",
                f"{float(row['selection_effect']) * 10000.0:.1f}",
                f"{float(row['interaction_effect']) * 10000.0:.1f}",
                f"{float(row['active_effect']) * 10000.0:.1f}",
            ]
        )

    row_labels.append("Total")
    cell_text.append(
        [
            f"{float(table_source['allocation_effect'].sum()) * 10000.0:.1f}",
            f"{float(table_source['selection_effect'].sum()) * 10000.0:.1f}",
            f"{float(table_source['interaction_effect'].sum()) * 10000.0:.1f}",
            f"{float(table_source['active_effect'].sum()) * 10000.0:.1f}",
        ]
    )

    selection_bps = float(table_source["selection_effect"].sum()) * 10000.0
    interaction_bps = float(table_source["interaction_effect"].sum()) * 10000.0
    cash_drag_bps = (
        float(table_source.loc[table_source["sector"].astype(str).str.lower() == "cash", "active_effect"].sum()) * 10000.0
    )
    driver_line = (
        f"Driver: Selection {selection_bps:+.1f} bps | "
        f"Interaction {interaction_bps:+.1f} bps | "
        f"Cash {cash_drag_bps:+.1f} bps"
    )

    ax.set_title("Attribution Components (bps)", fontsize=9.5, loc="left", pad=2)
    table = ax.table(
        cellText=cell_text,
        rowLabels=row_labels,
        colLabels=cols,
        loc="upper center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.0, 1.2)
    for cell in table.get_celld().values():
        cell.set_linewidth(0.35)
        cell.set_edgecolor("#6D7785")
    return driver_line


def _cash_methodology_note(month_attr: pd.DataFrame, cash_return_source: str) -> str:
    source = str(cash_return_source).strip().upper()
    if month_attr.empty or "sector" not in month_attr.columns:
        benchmark_weight = 0.0
    else:
        cash_rows = month_attr[month_attr["sector"].astype(str).str.lower() == "cash"]
        benchmark_weight = float(cash_rows["w_b"].iloc[0]) * 100.0 if not cash_rows.empty else 0.0
    if source in {"0%", "0", "ZERO", "NONE"}:
        policy_text = "Cash return source = 0% (policy convention)."
    else:
        policy_text = f"Cash return source = {source} (policy-configured annualized proxy)."
    return f"{policy_text} Benchmark cash weight = {benchmark_weight:.1f}%."


def _qa_lines(
    breaks: pd.DataFrame,
    daily_returns: pd.DataFrame,
    analysis_window: dict[str, object],
    mtd_window: dict[str, object],
) -> list[str]:
    def _count(break_type: str) -> int:
        if breaks.empty:
            return 0
        return int((breaks["break_type"] == break_type).sum())

    lines = [
        f"Return outliers: {_count('RETURN_OUTLIER')}",
        f"NAV jump / zero-flow flags: {_count('NAV_JUMP_ZERO_FLOW')}",
        f"Missing prices: {_count('MISSING_PRICE')}",
        "Net flow (Analysis window): "
        + (
            f"${float(_window_rows(daily_returns, str(analysis_window['start']), str(analysis_window['end']))['external_flow_base'].sum()):,.0f}"
            if "external_flow_base" in daily_returns.columns
            else "$0"
        ),
        format_flow_summary_line(
            _window_rows(
                daily_returns=daily_returns,
                start_date=str(mtd_window["start"]),
                end_date=str(mtd_window["end"]),
            ),
            window_label="MTD",
        ),
    ]
    return lines


def generate_tear_sheet(
    output_dir: Path,
    asof_date: str,
    daily_returns: pd.DataFrame,
    monthly_returns: pd.DataFrame,
    attribution: pd.DataFrame,
    breaks: pd.DataFrame,
    reconciliation_tolerance_bps: float = 5.0,
    cash_return_source: str = "0%",
    date_context: dict[str, object] | None = None,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    png_path = output_dir / "tearsheet.png"
    pdf_path = output_dir / "onepager.pdf"

    recon = latest_reconciliation(
        monthly_returns=monthly_returns,
        attribution=attribution,
        tolerance_bps=reconciliation_tolerance_bps,
    )
    data_label = _dataset_label(daily_returns=daily_returns, attribution=attribution)
    window_ctx = date_context or derive_date_context(daily_returns=daily_returns, clamp_to_market=True)
    asof_effective = str(window_ctx["asof_date"])
    data_asof_date = str(window_ctx.get("data_asof_date", asof_effective))
    generated_at_et = str(window_ctx.get("generated_at_et", "N/A"))
    market_last_closed = window_ctx.get("market_last_closed_session")
    analysis_window = window_ctx["analysis_window"]
    mtd_window = window_ctx["mtd_window"]
    period_rows = _period_return_rows(daily_returns)
    risk = _risk_metrics(daily_returns=daily_returns, cash_return_source=cash_return_source)
    status_line, control_lines = _control_lines(recon=recon, tolerance_bps=reconciliation_tolerance_bps)

    fig = plt.figure(figsize=(12, 8.5), constrained_layout=True)
    engine = fig.get_layout_engine()
    if engine:
        engine.set(h_pad=6 / 72, hspace=0.12)
    gs = fig.add_gridspec(4, 2, height_ratios=[0.90, 1.02, 0.52, 0.90], width_ratios=[1.25, 1.0])

    ax_title = fig.add_subplot(gs[0, :])
    ax_perf = fig.add_subplot(gs[1:3, 0])
    right_spec = gs[1:3, 1].subgridspec(3, 1, height_ratios=[3.2, 1.6, 0.6], hspace=0.24)
    ax_attr = fig.add_subplot(right_spec[0])
    ax_attr_table = fig.add_subplot(right_spec[1])
    ax_driver = fig.add_subplot(right_spec[2])
    ax_driver.axis("off")
    ax_qa = fig.add_subplot(gs[3, :])

    ax_title.axis("off")
    ax_title.text(0.0, 0.94, "PBOR-Lite Monthly Tear Sheet", fontsize=18, weight="bold")
    ax_title.text(0.0, 0.78, f"As-of (data): {data_asof_date}  |  {data_label}", fontsize=11)
    ax_title.text(0.0, 0.66, f"Generated: {generated_at_et}", fontsize=9.6)
    if market_last_closed:
        ax_title.text(0.0, 0.54, f"Market last closed session: {market_last_closed}", fontsize=9.6)
    ax_title.text(
        0.0,
        0.42,
        (
            "Analysis window: "
            f"{analysis_window['start']} to {analysis_window['end']} "
            f"({int(analysis_window['obs_rows'])} obs rows | {int(analysis_window['trading_days'])} trading days)"
        ),
        fontsize=9.6,
    )
    ax_title.text(
        0.0,
        0.30,
        (
            "MTD window: "
            f"{mtd_window['start']} to {mtd_window['end']} "
            f"({int(mtd_window['obs_rows'])} obs rows | {int(mtd_window['trading_days'])} trading days)"
        ),
        fontsize=9.6,
    )
    ax_title.text(0.0, 0.18, _portfolio_summary(monthly_returns), fontsize=10)
    if period_rows:
        period_line = "Linked returns: " + " | ".join(
            f"{str(r['period'])} {_pct(float(r['portfolio']))} / {_pct(float(r['benchmark']))} / {_pct(float(r['active_twr']))}"
            for r in period_rows
        )
        ax_title.text(0.0, 0.07, period_line, fontsize=8.7)
    ax_title.text(0.0, 0.00, status_line, fontsize=10.2, weight="bold")
    ax_title.text(
        0.66,
        0.75,
        "\n".join(control_lines),
        fontsize=9.0,
        va="top",
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "#F5F7FA", "edgecolor": "#BCC7D4"},
    )
    ax_title.text(
        0.66,
        0.43,
        (
            f"Risk (ann): TE {_fmt_pct_metric(float(risk['tracking_error']))} | "
            f"IR {_fmt_ratio(float(risk['information_ratio']))}\n"
            f"Sharpe {_fmt_ratio(float(risk['sharpe']))} | Vol {_fmt_pct_metric(float(risk['volatility']))}"
        ),
        fontsize=8.8,
        va="top",
        bbox={"boxstyle": "round,pad=0.30", "facecolor": "#F8FAFC", "edgecolor": "#D2D9E3"},
    )

    if "Data Under Review" in status_line:
        ax_title.text(
            1.0,
            0.90,
            "PRELIMINARY - DO NOT DISTRIBUTE",
            fontsize=10,
            weight="bold",
            color="crimson",
            ha="right",
        )

    _plot_cumulative_performance(ax_perf, daily_returns=daily_returns)
    month_attr = _plot_attribution(
        ax_attr,
        attribution=attribution,
        recon=recon,
        tolerance_bps=reconciliation_tolerance_bps,
    )
    driver_line = _plot_attribution_component_table(ax_attr_table, month_attr=month_attr, recon=recon)
    if driver_line:
        ax_driver.text(
            0.0,
            0.5,
            driver_line,
            transform=ax_driver.transAxes,
            fontsize=8.1,
            ha="left",
            va="center",
            clip_on=True,
        )

    ax_qa.axis("off")
    ax_qa.set_title("QA / Break Summary", fontsize=11, loc="left", pad=6)
    lines = _qa_lines(
        breaks=breaks,
        daily_returns=daily_returns,
        analysis_window=analysis_window,
        mtd_window=mtd_window,
    )
    contrib, detract = _attribution_top_lines(attribution if bool(recon["within_tolerance"]) else pd.DataFrame())
    if contrib:
        lines.append(f"Top contributors: {contrib[0]}" + (f"; {contrib[1]}" if len(contrib) > 1 else ""))
    if detract:
        lines.append(f"Top detractor: {detract[0]}")
    lines.append(_analyst_commentary(month_attr=month_attr, recon=recon, cash_return_source=cash_return_source))
    qa_text = "\n".join(f"- {line}" for line in lines[:10])
    ax_qa.text(0.0, 0.95, qa_text if qa_text else "- No QA lines", fontsize=9.3, va="top")
    fig.savefig(png_path, dpi=180)
    fig.savefig(pdf_path)
    plt.close(fig)
    return png_path, pdf_path
