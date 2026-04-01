from __future__ import annotations

import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import FancyBboxPatch

from pbor.date_source import derive_date_context
from .qa import format_flow_summary_line
from .reconciliation import latest_reconciliation

matplotlib.rcParams["font.family"] = "DejaVu Sans"
matplotlib.rcParams["font.size"] = 9
if "text.parse_math" in matplotlib.rcParams:
    matplotlib.rcParams["text.parse_math"] = False

FIG_BG = "#08111F"
PANEL_BG = "#0F1B2D"
PANEL_EDGE = "#1E3657"
GRID_COLOR = "#29415F"
TEXT_MAIN = "#E6EEF8"
TEXT_MUTED = "#90A4C2"
ACCENT = "#5EEAD4"
ACCENT_ALT = "#60A5FA"
POSITIVE = "#34D399"
NEGATIVE = "#FB7185"
WARNING = "#FBBF24"
NEUTRAL = "#CBD5E1"


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
        return "Bundled sample dataset"
    return "Public market data sample"


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


def _display_portfolio_view(
    daily_returns: pd.DataFrame,
    monthly_returns: pd.DataFrame,
    attribution: pd.DataFrame,
    breaks: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None, bool]:
    if monthly_returns.empty or "portfolio_id" not in monthly_returns.columns:
        return daily_returns, monthly_returns, attribution, breaks, None, False

    ordered = monthly_returns.copy()
    ordered["month_end_sort"] = pd.to_datetime(ordered["month_end"], errors="coerce")
    latest_row = ordered.sort_values(["month_end_sort", "portfolio_id"]).iloc[-1]
    portfolio_id = str(latest_row["portfolio_id"])
    multi_portfolio = ordered["portfolio_id"].astype(str).nunique() > 1

    daily_view = daily_returns
    if not daily_returns.empty and "portfolio_id" in daily_returns.columns:
        daily_view = daily_returns[daily_returns["portfolio_id"].astype(str) == portfolio_id].copy()

    monthly_view = monthly_returns[monthly_returns["portfolio_id"].astype(str) == portfolio_id].copy()

    attribution_view = attribution
    if not attribution.empty and "portfolio_id" in attribution.columns:
        attribution_view = attribution[attribution["portfolio_id"].astype(str) == portfolio_id].copy()

    breaks_view = breaks
    if not breaks.empty and "portfolio_id" in breaks.columns:
        breaks_view = breaks[
            breaks["portfolio_id"].isna() | (breaks["portfolio_id"].astype(str) == portfolio_id)
        ].copy()

    return daily_view, monthly_view, attribution_view, breaks_view, portfolio_id, multi_portfolio


def _style_panel(ax: plt.Axes, title: str) -> None:
    ax.set_facecolor(PANEL_BG)
    for spine in ax.spines.values():
        spine.set_color(PANEL_EDGE)
        spine.set_linewidth(1.0)
    ax.tick_params(colors=TEXT_MUTED)
    ax.xaxis.label.set_color(TEXT_MUTED)
    ax.yaxis.label.set_color(TEXT_MUTED)
    ax.title.set_color(TEXT_MAIN)
    ax.set_title(title, fontsize=11, color=TEXT_MAIN, loc="left", pad=10, weight="bold")


def _draw_card(
    ax: plt.Axes,
    x: float,
    y: float,
    w: float,
    h: float,
    label: str,
    value: str,
    accent_color: str,
    note: str | None = None,
) -> None:
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.014,rounding_size=0.03",
        linewidth=1.0,
        edgecolor=PANEL_EDGE,
        facecolor="#0B1627",
        transform=ax.transAxes,
    )
    ax.add_patch(patch)
    ax.text(x + 0.04 * w, y + 0.70 * h, label, color=TEXT_MUTED, fontsize=8.4, transform=ax.transAxes)
    ax.text(
        x + 0.04 * w,
        y + 0.30 * h,
        value,
        color=accent_color,
        fontsize=14,
        weight="bold",
        transform=ax.transAxes,
    )
    if note:
        ax.text(
            x + 0.04 * w,
            y + 0.08 * h,
            note,
            color=TEXT_MUTED,
            fontsize=7.6,
            transform=ax.transAxes,
        )


def _analyst_commentary(month_attr: pd.DataFrame, recon: dict[str, object], cash_return_source: str) -> str:
    if not recon["available"] or not recon["within_tolerance"] or month_attr.empty:
        return "Analyst commentary: Attribution withheld pending reconciliation; results remain under review."
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
        return "Status: Under Review", ["Controls unavailable for this run."]

    diff_ok = float(recon["diff_bps"]) < tolerance_bps
    weights_ok = bool(recon["weights_ok"])
    sector_ok = bool(recon["portfolio_return_ok"])
    all_ok = diff_ok and weights_ok and sector_ok

    status = "Status: Controls Passed" if all_ok else "Status: Under Review"
    lines = [
        f"Attribution-Active diff: {float(recon['diff_bps']):.1f} bps",
        f"Sum weights: Wp={float(recon['w_p_sum']):.2f}, Wb={float(recon['w_b_sum']):.2f}",
        f"Sector->Portfolio diff: {float(recon['portfolio_return_diff_bps']):.1f} bps",
    ]
    return status, lines


def _plot_cumulative_performance(ax: plt.Axes, daily_returns: pd.DataFrame) -> None:
    _style_panel(ax, "Cumulative Return")
    if daily_returns.empty:
        ax.text(0.5, 0.5, "No daily return data", ha="center", va="center", color=TEXT_MUTED)
        return

    perf = daily_returns.copy().sort_values("date")
    perf["date"] = pd.to_datetime(perf["date"])
    perf["portfolio_cum"] = (1.0 + perf["daily_return"].fillna(0.0)).cumprod() - 1.0
    perf["benchmark_cum"] = (1.0 + perf["benchmark_return"].fillna(0.0)).cumprod() - 1.0

    portfolio_line = perf["portfolio_cum"] * 100.0
    benchmark_line = perf["benchmark_cum"] * 100.0
    ax.fill_between(perf["date"], portfolio_line, color=ACCENT_ALT, alpha=0.12)
    ax.fill_between(perf["date"], benchmark_line, color=ACCENT, alpha=0.08)
    ax.plot(perf["date"], portfolio_line, label="Portfolio", linewidth=2.2, color=ACCENT_ALT)
    ax.plot(perf["date"], benchmark_line, label="Benchmark", linewidth=1.8, color=ACCENT)
    ax.axhline(0.0, color=GRID_COLOR, linewidth=1.0, alpha=0.8)
    ax.set_ylabel("Return (%)")
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
    ax.tick_params(axis="y", labelsize=8.5, colors=TEXT_MUTED)
    ax.grid(alpha=0.25, color=GRID_COLOR)
    legend = ax.legend(loc="upper left", bbox_to_anchor=(0.0, 1.02), fontsize=8, framealpha=0.0)
    for text in legend.get_texts():
        text.set_color(TEXT_MUTED)

    ax.annotate(
        f"{float(portfolio_line.iloc[-1]):.1f}%",
        xy=(perf["date"].iloc[-1], portfolio_line.iloc[-1]),
        xytext=(8, 0),
        textcoords="offset points",
        color=ACCENT_ALT,
        fontsize=8.5,
        va="center",
    )
    ax.annotate(
        f"{float(benchmark_line.iloc[-1]):.1f}%",
        xy=(perf["date"].iloc[-1], benchmark_line.iloc[-1]),
        xytext=(8, -12),
        textcoords="offset points",
        color=ACCENT,
        fontsize=8.5,
        va="center",
    )


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
    _style_panel(ax, "Active Effect by Sector")
    title = "Attribution Active Effect by Sector (bps)"
    if recon["available"] and recon["within_tolerance"]:
        title = f"{title}\nReconciles to Active (arith, MTD) = {_pct(float(recon['active_return']))}"
    ax.set_title(title, fontsize=10.2, pad=8, color=TEXT_MAIN, loc="left", weight="bold")
    if not recon["available"]:
        ax.axis("off")
        ax.text(0.0, 0.7, "No attribution window available.", fontsize=10, color=TEXT_MUTED)
        return pd.DataFrame()
    if not recon["within_tolerance"]:
        ax.axis("off")
        ax.text(0.0, 0.7, "Attribution withheld pending reconciliation.", fontsize=10, weight="bold", color=WARNING)
        ax.text(
            0.0,
            0.5,
            f"Diff {float(recon['diff_bps']):.1f} bps exceeds {tolerance_bps:.1f} bps threshold.",
            fontsize=9,
            color=TEXT_MUTED,
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
    colors = [POSITIVE if value >= 0 else NEGATIVE for value in month_attr["active_bps"]]
    bars = ax.barh(labels, month_attr["active_bps"], color=colors, alpha=0.88)
    for bar in bars:
        bar.set_alpha(0.9)
    ax.axvline(0.0, color=GRID_COLOR, linewidth=1.0, alpha=0.9)
    ax.tick_params(axis="x", labelsize=8, colors=TEXT_MUTED)
    ax.tick_params(axis="y", labelsize=8, colors=TEXT_MUTED)
    ax.set_xlabel("Active Effect (bps)")
    ax.grid(axis="x", alpha=0.25, color=GRID_COLOR)
    for bar, value in zip(bars, month_attr["active_bps"]):
        offset = 6 if value >= 0 else -6
        align = "left" if value >= 0 else "right"
        ax.text(
            value + offset,
            bar.get_y() + bar.get_height() / 2.0,
            f"{value:.1f}",
            va="center",
            ha=align,
            fontsize=8,
            color=TEXT_MAIN,
        )
    return month_attr


def _plot_attribution_component_table(ax: plt.Axes, month_attr: pd.DataFrame, recon: dict[str, object]) -> str | None:
    ax.axis("off")
    ax.set_facecolor(PANEL_BG)
    if not recon["available"]:
        ax.text(0.0, 0.5, "Component table unavailable.", fontsize=9, va="center", color=TEXT_MUTED)
        return None
    if not recon["within_tolerance"] or month_attr.empty:
        ax.text(0.0, 0.5, "Component table hidden until reconciliation passes.", fontsize=9, va="center", color=TEXT_MUTED)
        return None

    table_source = month_attr.sort_values(["active_bps", "sector"], ascending=[False, True], kind="mergesort").copy()
    if table_source.empty:
        ax.text(0.0, 0.5, "No attribution rows to display.", fontsize=9, va="center", color=TEXT_MUTED)
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

    ax.set_title("Attribution Components (bps)", fontsize=9.5, loc="left", pad=2, color=TEXT_MAIN)
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
    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_linewidth(0.35)
        cell.set_edgecolor(PANEL_EDGE)
        cell.set_facecolor("#0B1627")
        cell.get_text().set_color(TEXT_MAIN)
        if row_idx == 0:
            cell.set_facecolor("#12233B")
            cell.get_text().set_weight("bold")
        if col_idx == -1:
            cell.set_facecolor("#101D31")
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

    (
        display_daily_returns,
        display_monthly_returns,
        display_attribution,
        display_breaks,
        display_portfolio_id,
        multi_portfolio,
    ) = _display_portfolio_view(
        daily_returns=daily_returns,
        monthly_returns=monthly_returns,
        attribution=attribution,
        breaks=breaks,
    )

    recon = latest_reconciliation(
        monthly_returns=display_monthly_returns,
        attribution=display_attribution,
        tolerance_bps=reconciliation_tolerance_bps,
    )
    data_label = _dataset_label(daily_returns=display_daily_returns, attribution=display_attribution)
    window_ctx = date_context or derive_date_context(daily_returns=display_daily_returns, clamp_to_market=True)
    asof_effective = str(window_ctx["asof_date"])
    data_asof_date = str(window_ctx.get("data_asof_date", asof_effective))
    generated_at_et = str(window_ctx.get("generated_at_et", "N/A"))
    market_last_closed = window_ctx.get("market_last_closed_session")
    analysis_window = window_ctx["analysis_window"]
    mtd_window = window_ctx["mtd_window"]
    period_rows = _period_return_rows(display_daily_returns)
    risk = _risk_metrics(daily_returns=display_daily_returns, cash_return_source=cash_return_source)
    status_line, control_lines = _control_lines(recon=recon, tolerance_bps=reconciliation_tolerance_bps)
    latest_month_row = (
        display_monthly_returns.sort_values("month_end").iloc[-1] if not display_monthly_returns.empty else None
    )
    status_value = "Controls Passed" if "Controls Passed" in status_line else "Under Review"
    status_color = POSITIVE if status_value == "Controls Passed" else WARNING
    active_value = (
        _pct(float(latest_month_row["active_return"])) if latest_month_row is not None else "N/A"
    )
    tracking_error_value = _fmt_pct_metric(float(risk["tracking_error"]))
    break_count_value = str(int(len(display_breaks)))
    portfolio_note = (
        "Personal project sample from a multi-portfolio run"
        if multi_portfolio
        else "Personal project sample from a single-portfolio run"
    )

    fig = plt.figure(figsize=(13.4, 8.6), constrained_layout=True)
    fig.patch.set_facecolor(FIG_BG)
    engine = fig.get_layout_engine()
    if engine:
        engine.set(h_pad=8 / 72, hspace=0.14)
    gs = fig.add_gridspec(4, 2, height_ratios=[1.05, 1.0, 0.55, 0.88], width_ratios=[1.3, 1.0])

    ax_title = fig.add_subplot(gs[0, :])
    ax_perf = fig.add_subplot(gs[1:3, 0])
    right_spec = gs[1:3, 1].subgridspec(3, 1, height_ratios=[3.2, 1.6, 0.6], hspace=0.24)
    ax_attr = fig.add_subplot(right_spec[0])
    ax_attr_table = fig.add_subplot(right_spec[1])
    ax_driver = fig.add_subplot(right_spec[2])
    ax_driver.axis("off")
    ax_qa = fig.add_subplot(gs[3, :])

    ax_title.axis("off")
    ax_title.add_patch(
        FancyBboxPatch(
            (0.0, 0.0),
            1.0,
            1.0,
            boxstyle="round,pad=0.012,rounding_size=0.03",
            linewidth=1.1,
            edgecolor=PANEL_EDGE,
            facecolor=PANEL_BG,
            transform=ax_title.transAxes,
        )
    )
    ax_title.text(0.04, 0.86, "PBOR-Lite", fontsize=12, color=ACCENT, weight="bold")
    ax_title.text(0.04, 0.70, "Month-End Tear Sheet", fontsize=20, color=TEXT_MAIN, weight="bold")
    ax_title.text(
        0.04,
        0.56,
        f"{display_portfolio_id or 'N/A'}  |  As-of {data_asof_date}  |  {data_label}",
        fontsize=10.6,
        color=TEXT_MUTED,
    )
    ax_title.text(0.04, 0.45, portfolio_note, fontsize=9.0, color=TEXT_MUTED)
    ax_title.text(0.04, 0.31, f"Generated: {generated_at_et}", fontsize=9.2, color=TEXT_MUTED)
    if market_last_closed:
        ax_title.text(0.04, 0.21, f"Market last closed session: {market_last_closed}", fontsize=9.2, color=TEXT_MUTED)
    ax_title.text(
        0.04,
        0.11,
        (
            "Analysis window: "
            f"{analysis_window['start']} to {analysis_window['end']} "
            f"({int(analysis_window['obs_rows'])} obs rows | {int(analysis_window['trading_days'])} trading days)"
        ),
        fontsize=8.9,
        color=TEXT_MUTED,
    )
    ax_title.text(
        0.04,
        0.03,
        (
            "MTD window: "
            f"{mtd_window['start']} to {mtd_window['end']} "
            f"({int(mtd_window['obs_rows'])} obs rows | {int(mtd_window['trading_days'])} trading days)"
        ),
        fontsize=8.9,
        color=TEXT_MUTED,
    )
    ax_title.text(0.62, 0.88, _portfolio_summary(display_monthly_returns), fontsize=9.2, color=TEXT_MAIN)
    if period_rows:
        period_line = "Linked returns: " + " | ".join(
            f"{str(r['period'])} {_pct(float(r['portfolio']))} / {_pct(float(r['benchmark']))} / {_pct(float(r['active_twr']))}"
            for r in period_rows
        )
        ax_title.text(0.62, 0.78, period_line, fontsize=8.0, color=TEXT_MUTED)

    _draw_card(ax_title, 0.62, 0.49, 0.16, 0.20, "Status", status_value, status_color)
    _draw_card(ax_title, 0.80, 0.49, 0.16, 0.20, "Active TWR", active_value, ACCENT_ALT)
    _draw_card(ax_title, 0.62, 0.22, 0.16, 0.20, "Tracking Error", tracking_error_value, ACCENT)
    _draw_card(ax_title, 0.80, 0.22, 0.16, 0.20, "Breaks", break_count_value, WARNING if len(display_breaks) else POSITIVE)

    ax_title.text(
        0.62,
        0.06,
        "\n".join(control_lines),
        fontsize=8.6,
        color=TEXT_MUTED,
        va="bottom",
    )

    if "Under Review" in status_line:
        ax_title.text(
            0.96,
            0.92,
            "FOR REVIEW ONLY",
            fontsize=10,
            weight="bold",
            color=NEGATIVE,
            ha="right",
        )

    _plot_cumulative_performance(ax_perf, daily_returns=display_daily_returns)
    month_attr = _plot_attribution(
        ax_attr,
        attribution=display_attribution,
        recon=recon,
        tolerance_bps=reconciliation_tolerance_bps,
    )
    driver_line = _plot_attribution_component_table(ax_attr_table, month_attr=month_attr, recon=recon)
    if driver_line:
        ax_driver.set_facecolor(PANEL_BG)
        ax_driver.text(
            0.0,
            0.5,
            driver_line,
            transform=ax_driver.transAxes,
            fontsize=8.1,
            ha="left",
            va="center",
            clip_on=True,
            color=TEXT_MUTED,
        )

    ax_qa.axis("off")
    ax_qa.add_patch(
        FancyBboxPatch(
            (0.0, 0.0),
            1.0,
            1.0,
            boxstyle="round,pad=0.012,rounding_size=0.03",
            linewidth=1.1,
            edgecolor=PANEL_EDGE,
            facecolor=PANEL_BG,
            transform=ax_qa.transAxes,
        )
    )
    ax_qa.set_title("QA and Commentary", fontsize=11, loc="left", pad=8, color=TEXT_MAIN, weight="bold")
    lines = _qa_lines(
        breaks=display_breaks,
        daily_returns=display_daily_returns,
        analysis_window=analysis_window,
        mtd_window=mtd_window,
    )
    contrib, detract = _attribution_top_lines(display_attribution if bool(recon["within_tolerance"]) else pd.DataFrame())
    if contrib:
        lines.append(f"Top contributors: {contrib[0]}" + (f"; {contrib[1]}" if len(contrib) > 1 else ""))
    if detract:
        lines.append(f"Top detractor: {detract[0]}")
    lines.append(_analyst_commentary(month_attr=month_attr, recon=recon, cash_return_source=cash_return_source))
    left_block = "\n".join(f"- {line}" for line in lines[:5])
    right_block = "\n".join(f"- {line}" for line in lines[5:10])
    ax_qa.text(0.03, 0.90, left_block if left_block else "- No QA lines", fontsize=9.1, va="top", color=TEXT_MAIN)
    ax_qa.text(0.53, 0.90, right_block, fontsize=9.1, va="top", color=TEXT_MAIN)
    fig.savefig(png_path, dpi=180, facecolor=fig.get_facecolor())
    fig.savefig(pdf_path, facecolor=fig.get_facecolor())
    plt.close(fig)
    return png_path, pdf_path
