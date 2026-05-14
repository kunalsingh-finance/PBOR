"""Dashboard for Portfolio Reconciliation & Reporting Control Engine data in pbor_lite.db."""

from __future__ import annotations

import math
import sqlite3
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
import yaml

DB_PATH = Path(__file__).resolve().parents[1] / "pbor_lite.db"
POLICY_PATH = Path(__file__).resolve().parents[1] / "policy.yaml"
NON_EXCEPTION_STATUSES = {"MATCHED", "WITHIN_TOLERANCE"}

st.set_page_config(
    page_title="Portfolio Reconciliation & Reporting Control Engine",
    page_icon=":bar_chart:",
    layout="wide",
)


def _show_empty_message() -> None:
    st.markdown(
        """
        <div style="padding: 0.9rem 1rem; border-radius: 0.6rem; background: #F3F4F6; color: #374151; border: 1px solid #D1D5DB;">
            No data available. Run month-end first.
        </div>
        """,
        unsafe_allow_html=True,
    )


def _fmt_pct(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.2f}%"


def _fmt_bps(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 10000:.1f}"


def _fmt_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.2f}"


def _safe_float(value: object) -> float:
    if value is None or pd.isna(value):
        return math.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _fmt_date(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return pd.to_datetime(value).strftime("%Y-%m-%d")


@st.cache_data(ttl=300, show_spinner=False)
def load_monthly_returns() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        frame = pd.read_sql_query(
            """
            SELECT month_end, portfolio_id, portfolio_return_twr, portfolio_return_dietz,
                   dietz_denominator, benchmark_return, active_return
            FROM pbor_monthly_returns
            ORDER BY month_end
            """,
            conn,
        )
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame["month_end"] = pd.to_datetime(frame["month_end"])
    return frame


@st.cache_data(ttl=300, show_spinner=False)
def load_attribution() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        frame = pd.read_sql_query(
            """
            SELECT month_end, portfolio_id, benchmark_id, sector, w_p, w_b, r_p, r_b,
                   allocation_effect, selection_effect, interaction_effect, active_effect
            FROM pbor_attribution_monthly
            ORDER BY month_end, sector
            """,
            conn,
        )
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame["month_end"] = pd.to_datetime(frame["month_end"])
    return frame


@st.cache_data(ttl=300, show_spinner=False)
def load_breaks() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        frame = pd.read_sql_query(
            """
            SELECT asof_date, portfolio_id, break_type, severity, details, root_cause, resolution
            FROM pbor_breaks
            ORDER BY asof_date DESC, severity DESC, break_type
            """,
            conn,
        )
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame["asof_date"] = pd.to_datetime(frame["asof_date"])
    return frame


@st.cache_data(ttl=300, show_spinner=False)
def load_recon_exceptions() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        try:
            frame = pd.read_sql_query(
                """
                SELECT asof_date, portfolio_id, account_id, security_id, ticker, currency,
                       break_type, status, severity, internal_quantity, external_quantity,
                       quantity_diff, internal_price, external_price, price_diff,
                       internal_market_value, external_market_value, market_value_diff,
                       internal_cash, external_cash, cash_diff, root_cause, resolution_note,
                       owner, age_days
                FROM pbor_recon_exceptions
                ORDER BY asof_date DESC, severity DESC, status, break_type
                """,
                conn,
            )
        except sqlite3.OperationalError:
            frame = pd.DataFrame()
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame["asof_date"] = pd.to_datetime(frame["asof_date"])
    return frame


@st.cache_data(ttl=300, show_spinner=False)
def load_daily_returns() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        frame = pd.read_sql_query(
            """
            SELECT date, portfolio_id, portfolio_value_base, external_flow_base, daily_return, benchmark_return
            FROM pbor_daily_returns
            ORDER BY date
            """,
            conn,
        )
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame["date"] = pd.to_datetime(frame["date"])
    return frame


@st.cache_data(ttl=300, show_spinner=False)
def load_policy() -> dict[str, object]:
    if not POLICY_PATH.exists():
        return {}
    return yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8")) or {}


@st.cache_data(ttl=300, show_spinner=False)
def load_table_counts() -> dict[str, int]:
    conn = sqlite3.connect(DB_PATH)
    try:
        counts: dict[str, int] = {}
        for table in [
            "pbor_daily_positions",
            "pbor_daily_returns",
            "pbor_monthly_returns",
            "pbor_attribution_monthly",
            "pbor_breaks",
            "pbor_recon_exceptions",
        ]:
            try:
                result = pd.read_sql_query(f"SELECT COUNT(*) AS row_count FROM {table}", conn)
                counts[table] = int(result["row_count"].iloc[0])
            except sqlite3.OperationalError:
                counts[table] = 0
    finally:
        conn.close()
    return counts


def _build_return_chart(frame: pd.DataFrame, cumulative: bool) -> alt.Chart:
    chart_data = frame[["month_end", "portfolio_return_twr", "benchmark_return"]].copy().fillna(0.0)
    chart_data = chart_data.rename(
        columns={
            "portfolio_return_twr": "Portfolio TWR",
            "benchmark_return": "Benchmark",
        }
    )
    if cumulative:
        chart_data["Portfolio TWR"] = (1.0 + chart_data["Portfolio TWR"]).cumprod() - 1.0
        chart_data["Benchmark"] = (1.0 + chart_data["Benchmark"]).cumprod() - 1.0

    plot_data = chart_data.melt("month_end", var_name="Series", value_name="Return")
    return (
        alt.Chart(plot_data)
        .mark_line(point=True)
        .encode(
            x=alt.X("month_end:T", title="Month End"),
            y=alt.Y("Return:Q", title="Return", axis=alt.Axis(format="%")),
            color=alt.Color("Series:N", title="Series"),
            tooltip=[
                alt.Tooltip("month_end:T", title="Month End"),
                alt.Tooltip("Series:N", title="Series"),
                alt.Tooltip("Return:Q", title="Return", format=".2%"),
            ],
        )
        .properties(height=360)
    )


def _style_active_returns(display_frame: pd.DataFrame) -> pd.io.formats.style.Styler:
    def active_color(value: object) -> str:
        if isinstance(value, str):
            raw = value.replace("%", "").replace(",", "").strip()
            if not raw or raw == "-":
                return ""
            try:
                numeric = float(raw)
            except ValueError:
                return ""
            if numeric > 0:
                return "color: #166534; font-weight: 600;"
            if numeric < 0:
                return "color: #B91C1C; font-weight: 600;"
        return ""

    return display_frame.style.applymap(active_color, subset=["Active Return"])


def render_monthly_returns_tab(monthly_returns: pd.DataFrame) -> None:
    st.subheader("Monthly Returns")
    if monthly_returns.empty:
        _show_empty_message()
        return

    months = monthly_returns["month_end"].drop_duplicates().sort_values().tolist()
    if len(months) == 1:
        range_value = (months[0], months[0])
        st.caption(f"Date range: {pd.Timestamp(months[0]).strftime('%Y-%m-%d')}")
    else:
        range_value = st.select_slider(
            "Date range",
            options=months,
            value=(months[0], months[-1]),
            format_func=lambda x: pd.Timestamp(x).strftime("%Y-%m-%d"),
        )
    cumulative = st.toggle("Cumulative Return", value=False)
    filtered = monthly_returns[
        (monthly_returns["month_end"] >= pd.Timestamp(range_value[0]))
        & (monthly_returns["month_end"] <= pd.Timestamp(range_value[1]))
    ].copy()

    display = pd.DataFrame(
        {
            "Month End": filtered["month_end"].dt.strftime("%Y-%m-%d"),
            "TWR": filtered["portfolio_return_twr"].map(_fmt_pct),
            "Mod. Dietz": filtered["portfolio_return_dietz"].map(_fmt_pct),
            "Benchmark": filtered["benchmark_return"].map(_fmt_pct),
            "Active Return": filtered["active_return"].map(_fmt_pct),
        }
    )
    st.dataframe(_style_active_returns(display), use_container_width=True, hide_index=True)
    st.altair_chart(_build_return_chart(filtered, cumulative=cumulative), use_container_width=True)


def _build_attribution_chart(frame: pd.DataFrame) -> alt.Chart:
    plot = frame[["sector", "allocation_effect", "selection_effect", "interaction_effect"]].copy()
    plot = plot.rename(
        columns={
            "allocation_effect": "Allocation Effect",
            "selection_effect": "Selection Effect",
            "interaction_effect": "Interaction Effect",
        }
    )
    plot = plot.melt("sector", var_name="Effect", value_name="bps")
    plot["bps"] = plot["bps"] * 10000.0
    return (
        alt.Chart(plot)
        .mark_bar()
        .encode(
            x=alt.X("sector:N", title="Sector"),
            xOffset="Effect:N",
            y=alt.Y("bps:Q", title="Effect (bps)"),
            color=alt.Color("Effect:N", title="Effect"),
            tooltip=[
                alt.Tooltip("sector:N", title="Sector"),
                alt.Tooltip("Effect:N", title="Effect"),
                alt.Tooltip("bps:Q", title="bps", format=".1f"),
            ],
        )
        .properties(height=380)
    )


def render_attribution_tab(attribution: pd.DataFrame, monthly_returns: pd.DataFrame) -> None:
    st.subheader("Attribution Waterfall")
    if attribution.empty or monthly_returns.empty:
        _show_empty_message()
        return

    month_options = attribution["month_end"].drop_duplicates().sort_values(ascending=False).tolist()
    selected_month = st.selectbox(
        "Month",
        options=month_options,
        format_func=lambda x: pd.Timestamp(x).strftime("%Y-%m-%d"),
    )
    month_attr = attribution[attribution["month_end"] == pd.Timestamp(selected_month)].copy()
    month_return = monthly_returns[monthly_returns["month_end"] == pd.Timestamp(selected_month)].copy()

    st.altair_chart(_build_attribution_chart(month_attr), use_container_width=True)

    active_return = _safe_float(month_return["active_return"].iloc[0]) if not month_return.empty else math.nan
    total_active = _safe_float(month_attr["active_effect"].sum())
    diff_bps = abs(total_active - active_return) * 10000.0 if not math.isnan(active_return) else math.nan
    if math.isnan(diff_bps):
        status_color = "#92400E"
        status_label = "UNDER REVIEW"
    else:
        passed = bool(diff_bps < 5.0)
        status_color = "#166534" if passed else "#B91C1C"
        status_label = "PASS" if passed else "FAIL"
    summary_cols = st.columns(3)
    summary_cols[0].metric("Total Active Effect (bps)", _fmt_bps(total_active))
    summary_cols[1].metric("Portfolio Active Return (bps)", _fmt_bps(active_return))
    summary_cols[2].markdown(
        f"""
        <div style="padding: 0.6rem 0.9rem; border-radius: 0.6rem; background: #F9FAFB; border: 1px solid #E5E7EB;">
            <div style="font-size: 0.85rem; color: #4B5563;">Reconciliation Status</div>
            <div style="font-size: 1.1rem; font-weight: 700; color: {status_color};">{status_label}</div>
            <div style="font-size: 0.85rem; color: #4B5563;">Diff: {_fmt_bps(diff_bps)} bps</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("Raw attribution data"):
        raw = pd.DataFrame(
            {
                "Sector": month_attr["sector"],
                "w_p": month_attr["w_p"].map(_fmt_pct),
                "w_b": month_attr["w_b"].map(_fmt_pct),
                "r_p": month_attr["r_p"].map(_fmt_pct),
                "r_b": month_attr["r_b"].map(_fmt_pct),
                "Alloc": month_attr["allocation_effect"].map(_fmt_bps),
                "Select": month_attr["selection_effect"].map(_fmt_bps),
                "Interact": month_attr["interaction_effect"].map(_fmt_bps),
                "Active": month_attr["active_effect"].map(_fmt_bps),
            }
        )
        st.dataframe(raw, use_container_width=True, hide_index=True)


def render_breaks_tab(breaks: pd.DataFrame) -> None:
    st.subheader("QA Breaks")
    if breaks.empty:
        st.success("No QA breaks in the current run.")
        return

    st.error(f"{len(breaks)} QA breaks in the current run.")

    display = pd.DataFrame(
        {
            "As-Of Date": breaks["asof_date"].dt.strftime("%Y-%m-%d"),
            "Portfolio": breaks["portfolio_id"].fillna("-"),
            "Break Type": breaks["break_type"],
            "Severity": breaks["severity"],
            "Details": breaks["details"],
        }
    )

    def severity_style(value: object) -> str:
        if value == "HIGH":
            return "background-color: #FEE2E2; color: #991B1B; font-weight: 600;"
        if value == "MEDIUM":
            return "background-color: #FEF3C7; color: #92400E; font-weight: 600;"
        if value == "LOW":
            return "background-color: #E5E7EB; color: #374151; font-weight: 600;"
        return ""

    st.dataframe(
        display.style.applymap(severity_style, subset=["Severity"]),
        use_container_width=True,
        hide_index=True,
    )
    st.download_button(
        "Download Breaks CSV",
        data=display.to_csv(index=False).encode("utf-8"),
        file_name="pbor_breaks.csv",
        mime="text/csv",
    )


def render_auto_recon_tab(recon_exceptions: pd.DataFrame) -> None:
    st.subheader("Auto Reconciliation")
    if recon_exceptions.empty:
        st.info("No auto-reconciliation records available. Run month-end with --recon-data-dir.")
        return

    frame = recon_exceptions.copy()
    status = frame["status"].astype(str)
    total_records = int(len(frame))
    matched_records = int(status.eq("MATCHED").sum())
    open_exceptions = int((~status.isin(NON_EXCEPTION_STATUSES)).sum())
    high_exceptions = int(frame["severity"].astype(str).str.upper().eq("HIGH").sum())
    cash_break_amount = float(
        pd.to_numeric(frame.loc[status.eq("CASH_BREAK"), "cash_diff"], errors="coerce").abs().sum()
    )
    market_value_break_amount = float(
        pd.to_numeric(frame.loc[~status.isin(NON_EXCEPTION_STATUSES), "market_value_diff"], errors="coerce")
        .abs()
        .sum()
    )
    match_rate = matched_records / total_records if total_records else 0.0
    ready_for_signoff = high_exceptions == 0

    metric_cols = st.columns(4)
    metric_cols[0].metric("Match Rate %", f"{match_rate * 100:.1f}%")
    metric_cols[1].metric("Total Records", f"{total_records:,}")
    metric_cols[2].metric("Open Exceptions", f"{open_exceptions:,}")
    metric_cols[3].metric("High Severity Exceptions", f"{high_exceptions:,}")

    value_cols = st.columns(3)
    value_cols[0].metric("Cash Break Amount", _fmt_number(cash_break_amount))
    value_cols[1].metric("Market Value Break Amount", _fmt_number(market_value_break_amount))
    value_cols[2].metric("Ready for Sign-Off", "Yes" if ready_for_signoff else "No")
    st.caption("Readiness here is recon-only unless QA and attribution controls are reviewed in the full month-end pack.")

    filter_cols = st.columns(3)
    severity_options = sorted(frame["severity"].dropna().astype(str).unique().tolist())
    status_options = sorted(frame["status"].dropna().astype(str).unique().tolist())
    break_type_options = sorted(frame["break_type"].dropna().astype(str).unique().tolist())
    selected_severity = filter_cols[0].selectbox("Severity", ["All"] + severity_options)
    selected_status = filter_cols[1].selectbox("Status", ["All"] + status_options)
    selected_break_type = filter_cols[2].selectbox("Break Type", ["All"] + break_type_options)

    filtered = frame.copy()
    if selected_severity != "All":
        filtered = filtered[filtered["severity"].astype(str) == selected_severity]
    if selected_status != "All":
        filtered = filtered[filtered["status"].astype(str) == selected_status]
    if selected_break_type != "All":
        filtered = filtered[filtered["break_type"].astype(str) == selected_break_type]

    column_labels = {
        "asof_date": "As-Of Date",
        "portfolio_id": "Portfolio",
        "account_id": "Account",
        "security_id": "Security",
        "ticker": "Ticker",
        "currency": "Currency",
        "break_type": "Break Type",
        "status": "Status",
        "severity": "Severity",
        "internal_quantity": "Internal Qty",
        "external_quantity": "External Qty",
        "quantity_diff": "Qty Diff",
        "internal_price": "Internal Price",
        "external_price": "External Price",
        "price_diff": "Price Diff",
        "internal_market_value": "Internal MV",
        "external_market_value": "External MV",
        "market_value_diff": "MV Diff",
        "internal_cash": "Internal Cash",
        "external_cash": "External Cash",
        "cash_diff": "Cash Diff",
        "root_cause": "Root Cause",
        "resolution_note": "Resolution Note",
        "owner": "Owner",
        "age_days": "Age Days",
    }
    display = filtered[list(column_labels.keys())].rename(columns=column_labels)
    if not display.empty:
        display["As-Of Date"] = pd.to_datetime(display["As-Of Date"]).dt.strftime("%Y-%m-%d")
        display = display.fillna("-").replace({"None": "-", "nan": "-"})
    st.dataframe(display, use_container_width=True, hide_index=True)
    st.download_button(
        "Download Recon Exceptions CSV",
        data=filtered.to_csv(index=False).encode("utf-8"),
        file_name="recon_exceptions.csv",
        mime="text/csv",
    )


def render_policy_metadata_tab(monthly_returns: pd.DataFrame, policy: dict[str, object], table_counts: dict[str, int]) -> None:
    st.subheader("Policy & Run Metadata")
    if monthly_returns.empty:
        _show_empty_message()
        return

    latest = monthly_returns.sort_values("month_end").iloc[-1]
    left_col, right_col = st.columns(2)

    sofr_rate = policy.get("cash_return_annual_rates", {}).get("SOFR", "-") if isinstance(policy.get("cash_return_annual_rates", {}), dict) else "-"
    policy_rows = pd.DataFrame(
        {
            "Setting": [
                "Base Currency",
                "Benchmark",
                "Cash Return Source",
                "SOFR Rate",
                "Large CF Threshold",
                "Recon Tolerance",
            ],
            "Value": [
                policy.get("base_currency", "-"),
                policy.get("benchmark_id_default", "-"),
                policy.get("cash_return_source", "-"),
                _fmt_pct(float(sofr_rate)) if sofr_rate != "-" else "-",
                str(policy.get("large_cash_flow_threshold", "-")),
                f"{policy.get('attribution_reconciliation_tolerance_bps', '-')} bps",
            ],
        }
    )

    metadata_rows = pd.DataFrame(
        {
            "Metric": [
                "Last As-Of Date",
                "Portfolio ID",
                "Months of History",
                "TWR (latest month)",
                "Active Return (latest month)",
                "Total Rows in DB",
            ],
            "Value": [
                _fmt_date(latest["month_end"]),
                latest["portfolio_id"],
                str(int(len(monthly_returns))),
                _fmt_pct(latest["portfolio_return_twr"]),
                _fmt_pct(latest["active_return"]),
                str(int(sum(table_counts.values()))),
            ],
        }
    )

    with left_col:
        st.markdown("**Policy Settings**")
        st.dataframe(policy_rows, use_container_width=True, hide_index=True)

    with right_col:
        st.markdown("**Last Run Summary**")
        st.dataframe(metadata_rows, use_container_width=True, hide_index=True)


def _build_drawdown_chart(frame: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(frame)
        .mark_area(color="#B91C1C", opacity=0.35)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("drawdown:Q", title="Drawdown", axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("drawdown:Q", title="Drawdown", format=".2%"),
            ],
        )
        .properties(height=250)
    )


def _build_risk_chart(frame: pd.DataFrame) -> alt.Chart:
    plot = frame[["date", "rolling_vol_30d"]].dropna().copy()
    return (
        alt.Chart(plot)
        .mark_line(color="#1D4ED8")
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("rolling_vol_30d:Q", title="30D Volatility", axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("rolling_vol_30d:Q", title="30D Vol", format=".2%"),
            ],
        )
        .properties(height=250)
    )


def render_drawdown_risk_tab(daily_returns: pd.DataFrame, policy: dict[str, object]) -> None:
    st.subheader("Drawdown & Risk")
    if daily_returns.empty:
        _show_empty_message()
        return

    frame = daily_returns.copy().sort_values("date")
    frame["daily_return"] = pd.to_numeric(frame["daily_return"], errors="coerce").fillna(0.0)
    frame["portfolio_cum"] = (1.0 + frame["daily_return"]).cumprod()
    frame["running_max"] = frame["portfolio_cum"].cummax()
    frame["drawdown"] = frame["portfolio_cum"] / frame["running_max"] - 1.0
    frame["rolling_vol_30d"] = frame["daily_return"].rolling(30).std() * math.sqrt(252)

    annualized_return = frame["daily_return"].mean() * 252
    annualized_vol = frame["daily_return"].std() * math.sqrt(252)
    sofr_rate = 0.0
    cash_rates = policy.get("cash_return_annual_rates", {})
    if isinstance(cash_rates, dict):
        try:
            sofr_rate = float(cash_rates.get("SOFR", 0.0))
        except (TypeError, ValueError):
            sofr_rate = 0.0
    sharpe = (annualized_return - sofr_rate) / annualized_vol if annualized_vol and not math.isnan(annualized_vol) else math.nan

    metric_cols = st.columns(3)
    metric_cols[0].metric("Max Drawdown", _fmt_pct(frame["drawdown"].min()))
    metric_cols[1].metric("30D Vol (latest)", _fmt_pct(frame["rolling_vol_30d"].dropna().iloc[-1] if frame["rolling_vol_30d"].notna().any() else math.nan))
    metric_cols[2].metric("Sharpe Ratio", f"{sharpe:.2f}" if not math.isnan(sharpe) else "-")

    st.altair_chart(_build_drawdown_chart(frame), use_container_width=True)
    st.altair_chart(_build_risk_chart(frame), use_container_width=True)


monthly_returns = load_monthly_returns()
attribution = load_attribution()
breaks = load_breaks()
recon_exceptions = load_recon_exceptions()
daily_returns = load_daily_returns()
policy = load_policy()
table_counts = load_table_counts()

with st.sidebar:
    st.title("Portfolio Reconciliation & Reporting Control Engine")
    st.caption("Reconciliation & reporting controls")
    if monthly_returns.empty:
        st.write("Last data refresh date: -")
    else:
        st.write(f"Last data refresh date: {_fmt_date(monthly_returns['month_end'].max())}")
    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.write("DB path being used")
    st.code(str(DB_PATH))

st.title("Portfolio Reconciliation & Reporting Control Engine")
st.caption("Read-only view of the current PBOR database.")

tabs = st.tabs(
    [
        "Monthly Returns",
        "Attribution Waterfall",
        "QA Breaks",
        "Auto Reconciliation",
        "Policy & Run Metadata",
        "Drawdown & Risk",
    ]
)

with tabs[0]:
    render_monthly_returns_tab(monthly_returns)

with tabs[1]:
    render_attribution_tab(attribution, monthly_returns)

with tabs[2]:
    render_breaks_tab(breaks)

with tabs[3]:
    render_auto_recon_tab(recon_exceptions)

with tabs[4]:
    render_policy_metadata_tab(monthly_returns, policy, table_counts)

with tabs[5]:
    render_drawdown_risk_tab(daily_returns, policy)
