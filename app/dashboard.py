"""PBOR's local portfolio analytics and reconciliation workspace."""
from __future__ import annotations

import io
import json
import sqlite3
import sys
import zipfile
from contextlib import closing
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.reconciliation import attribution_reconciliation
from src.review import REVIEW_STATUSES, exception_key, load_review_history, load_reviews, save_review

DB_PATH = ROOT / "pbor_lite.db"
TABLES = {
    "monthly": "pbor_monthly_returns", "daily": "pbor_daily_returns",
    "attribution": "pbor_attribution_monthly", "breaks": "pbor_breaks",
    "recon": "pbor_recon_exceptions", "signoff": "pbor_signoff_summary",
}
COLORS = ["#3b82f6", "#94a3b8", "#f59e0b"]
st.set_page_config(page_title="PBOR · Portfolio Control", page_icon="◈", layout="wide")


def pct(value: object) -> str:
    return "—" if pd.isna(value) else f"{float(value):.2%}"


def number(value: object, digits: int = 0) -> str:
    return "—" if pd.isna(value) else f"{float(value):,.{digits}f}"


def read_workspace() -> dict[str, pd.DataFrame]:
    """Read one coherent database snapshot without creating an empty database."""
    with closing(sqlite3.connect(f"{DB_PATH.as_uri()}?mode=ro", uri=True)) as conn:
        conn.execute("BEGIN")
        return {key: pd.read_sql_query(f"SELECT * FROM {table}", conn) for key, table in TABLES.items()}


def run_sample() -> None:
    from scripts.build_recon_demo_data import build_recon_demo_data
    from src.run_month_end import run_month_end

    with st.spinner("Calculating performance, matching positions and preparing reports…"):
        recon_dir = ROOT / "data" / "recon_demo"
        # Preserve corrections made to an existing sample feed.
        if not all((recon_dir / name).exists() for name in
                   ["internal_positions.csv", "custodian_positions.csv", "internal_cash.csv", "bank_cash.csv"]):
            build_recon_demo_data(recon_dir)
        result = run_month_end(ROOT, "2026-01-10", recon_data_dir=recon_dir)
        st.session_state["last_run"] = result
    st.rerun()


def scoped(frame: pd.DataFrame, portfolio: str) -> pd.DataFrame:
    return frame[frame["portfolio_id"].eq(portfolio)].copy()


def download_csv(label: str, frame: pd.DataFrame, name: str, key: str) -> None:
    st.download_button(label, frame.to_csv(index=False).encode("utf-8"), name, "text/csv", key=key)


def line_chart(frame: pd.DataFrame, date: str, fields: list[str], title: str = "Return") -> None:
    plot = frame[[date] + fields].melt(date, var_name="Series", value_name="Value")
    chart = alt.Chart(plot).mark_line(strokeWidth=2.5).encode(
        x=alt.X(f"{date}:T", title=None, scale=alt.Scale(type="utc"),
                axis=alt.Axis(format="%d %b", tickCount=min(8, len(frame)))),
        y=alt.Y("Value:Q", title=title, axis=alt.Axis(format=".1%")),
        color=alt.Color("Series:N", scale=alt.Scale(domain=fields, range=COLORS[:len(fields)]), legend=alt.Legend(orient="bottom", title=None)),
        tooltip=[alt.Tooltip(f"{date}:T", title="Date", timeUnit="utcyearmonthdate", format="%d %b %Y"), "Series:N", alt.Tooltip("Value:Q", format=".2%")],
    ).properties(height=310)
    st.altair_chart(chart, width="stretch")


def performance(monthly: pd.DataFrame, daily: pd.DataFrame) -> None:
    st.subheader("Portfolio performance")
    ordered = daily.sort_values("date").copy()
    # The opening valuation has no prior investment base. Anchor it at 1;
    # missing returns after that opening observation must still break the line.
    if not ordered.empty and pd.isna(ordered.iloc[0]["daily_return"]):
        ordered.loc[ordered.index[0], "daily_return"] = 0.0
    # Missing returns remain missing; do not turn a missing benchmark into a zero return.
    ordered["Portfolio"] = (1 + ordered["daily_return"]).cumprod(skipna=False) - 1
    ordered["Benchmark"] = (1 + ordered["benchmark_return"]).cumprod(skipna=False) - 1
    if ordered["benchmark_return"].isna().any():
        st.warning("Benchmark coverage is incomplete. Benchmark comparison is unavailable after the first missing observation.")
    line_chart(ordered, "date", ["Portfolio", "Benchmark"], "Cumulative return")
    st.caption("Geometrically linked daily returns for the selected portfolio. External contributions and withdrawals are excluded from performance.")
    display = monthly.rename(columns={"month_end": "Period", "portfolio_return_twr": "TWR",
        "portfolio_return_dietz": "Modified Dietz", "benchmark_return": "Benchmark", "active_return": "Active return"})
    columns = ["Period", "TWR", "Modified Dietz", "Benchmark", "Active return"]
    st.dataframe(display[columns].style.format({c: "{:.2%}" for c in columns[1:]}, na_rep="—"), hide_index=True, width="stretch")
    download_csv("Export performance", monthly, "portfolio_performance.csv", "performance_csv")
    with st.expander("Drawdown and observed risk"):
        wealth = 1 + ordered["Portfolio"]
        peak = wealth.cummax().clip(lower=1)
        ordered["Drawdown"] = wealth / peak - 1
        left, right = st.columns(2)
        left.metric("Maximum drawdown", pct(ordered["Drawdown"].min()))
        right.metric("Return observations", len(ordered))
        line_chart(ordered, "date", ["Drawdown"], "Drawdown")
        st.caption("The opening valuation anchors wealth at 1. Short sample windows are insufficient for a reliable annualized Sharpe estimate.")


def attribution_view(monthly: pd.DataFrame, attribution: pd.DataFrame, policy: dict) -> None:
    st.subheader("Sources of active return")
    if attribution.empty:
        st.info("No attribution is available for this portfolio.")
        return
    period = st.selectbox("Attribution period", sorted(attribution["month_end"].unique(), reverse=True))
    frame = attribution[attribution["month_end"].eq(period)].copy()
    effects = {"allocation_effect": "Allocation", "selection_effect": "Selection", "interaction_effect": "Interaction"}
    plot = frame[["sector"] + list(effects)].rename(columns=effects).melt("sector", var_name="Effect", value_name="bps")
    plot["bps"] *= 10000
    chart = alt.Chart(plot).mark_bar(cornerRadiusEnd=3).encode(
        x=alt.X("sector:N", title=None), xOffset="Effect:N", y=alt.Y("bps:Q", title="Basis points"),
        color=alt.Color("Effect:N", scale=alt.Scale(range=COLORS), legend=alt.Legend(orient="bottom", title=None)),
        tooltip=["sector:N", "Effect:N", alt.Tooltip("bps:Q", format=".2f")],
    ).properties(height=300)
    st.altair_chart(chart, width="stretch")
    if "active_return_arithmetic" not in monthly.columns:
        st.warning("Recalculate this workspace to include the arithmetic return basis required for attribution controls.")
    else:
        recon = attribution_reconciliation(monthly[monthly["month_end"].eq(period)], frame,
                    float(policy.get("attribution_reconciliation_tolerance_bps", 5)))
        if not recon.empty:
            row = recon.iloc[0]
            cols = st.columns(3)
            cols[0].metric("Attributed active return", pct(row["attribution_sum"]))
            cols[1].metric("Arithmetic active return", pct(row["active_return_reference"]))
            cols[2].metric("Difference · bps", number(row["diff_bps"], 2))
            tolerance = policy.get("attribution_reconciliation_tolerance_bps", 5)
            if bool(row["within_tolerance"]):
                st.success(f"Attribution reconciles within {tolerance} bps; weights and sector return checks pass.")
            else:
                st.warning(f"Attribution requires review. Active return and sector return must reconcile within {tolerance} bps, with portfolio and benchmark weights summing to 100%.")
    st.caption("Brinson–Fachler allocation, selection and interaction effects use an arithmetic return basis. Period TWR is reported separately in Performance.")
    shown = frame[["sector", "w_p", "w_b", "r_p", "r_b", "active_effect"]].rename(columns={
        "sector": "Sector", "w_p": "Portfolio weight", "w_b": "Benchmark weight", "r_p": "Sector return",
        "r_b": "Benchmark sector return", "active_effect": "Active effect"})
    st.dataframe(shown.style.format({c: "{:.2%}" for c in shown.columns if c != "Sector"}, na_rep="—"), hide_index=True, width="stretch")
    download_csv("Export attribution", frame, "attribution.csv", "attribution_csv")


def control_overview(signoff: pd.DataFrame, recon: pd.DataFrame, breaks: pd.DataFrame) -> None:
    st.subheader("Reporting readiness")
    st.caption("All portfolios and reconciliation books in the current run.")
    final = signoff[signoff["control_area"].eq("Final Reporting Sign-Off")]
    ready = not final.empty and bool(final.iloc[0]["ready_for_signoff"])
    open_recon = recon[~recon["workflow_status"].eq("CLOSED")] if not recon.empty else recon
    cols = st.columns(4)
    cols[0].metric("Release status", "Ready" if ready else "On hold")
    cols[1].metric("Open reconciliation breaks", len(open_recon))
    cols[2].metric("High priority", int(open_recon["severity"].eq("HIGH").sum()) if not open_recon.empty else 0)
    cols[3].metric("QA findings", len(breaks))
    if ready:
        st.success("All required checks pass. The reporting pack is ready for human review.")
    else:
        st.warning("Reporting is on hold until required controls pass. Review notes do not clear a financial break.")
    if signoff.empty:
        st.info("Run controls to generate a readiness assessment.")
    else:
        visible = signoff[~signoff["control_area"].eq("Final Reporting Sign-Off")]
        st.dataframe(visible[["control_area", "status", "open_exception_count", "action_required"]].rename(columns={
            "control_area": "Control", "status": "Result", "open_exception_count": "Open findings", "action_required": "Next action"}),
            hide_index=True, width="stretch")
    with st.expander(f"Quality checks · {len(breaks)} findings", expanded=not breaks.empty):
        if breaks.empty:
            st.success("No QA findings in this run.")
        else:
            st.dataframe(breaks.drop(columns=["break_id"], errors="ignore"), hide_index=True, width="stretch")
            download_csv("Export QA findings", breaks, "qa_findings.csv", "qa_csv")


def reconciliation_view(frame: pd.DataFrame) -> None:
    st.subheader("Exception workbench")
    st.caption("Review position and cash differences, assign responsibility and retain investigation evidence.")
    if frame.empty:
        st.info("No reconciliation feed has been loaded. Recalculate the bundled workspace to load the sample position and cash records.")
        return
    frame = frame.copy()
    frame["exception_key"] = frame.apply(lambda row: exception_key(row.to_dict()), axis=1)
    reviews = load_reviews(DB_PATH)
    if not reviews.empty:
        frame = frame.merge(reviews[["exception_key", "owner", "review_status", "note", "updated_at"]].rename(columns={"owner": "review_owner"}), on="exception_key", how="left")
    else:
        for column in ["review_owner", "review_status", "note", "updated_at"]:
            frame[column] = pd.NA
    frame["review_status"] = frame["review_status"].fillna("Unreviewed")
    controls = st.columns([2, 1, 1, 2])
    book = controls[0].selectbox("Reconciliation book", ["All books"] + sorted(frame["portfolio_id"].unique().tolist()))
    severity = controls[1].selectbox("Severity", ["All", "HIGH", "MEDIUM", "LOW"])
    review = controls[2].selectbox("Review", ["All"] + list(REVIEW_STATUSES))
    query = controls[3].text_input("Search security or account", placeholder="Ticker, security, account…")
    only_open = st.checkbox("Open breaks only", value=True)
    filtered = frame.copy()
    if book != "All books": filtered = filtered[filtered["portfolio_id"].eq(book)]
    if severity != "All": filtered = filtered[filtered["severity"].eq(severity)]
    if review != "All": filtered = filtered[filtered["review_status"].eq(review)]
    if only_open: filtered = filtered[~filtered["workflow_status"].eq("CLOSED")]
    if query:
        mask = filtered[["ticker", "security_id", "account_id", "currency"]].fillna("").astype(str).apply(
            lambda column: column.str.contains(query, case=False, regex=False)).any(axis=1)
        filtered = filtered[mask]
    filtered = filtered.assign(_priority=filtered["severity"].map({"HIGH": 0, "MEDIUM": 1, "LOW": 2})).sort_values(["_priority", "age_days"], ascending=[True, False]).drop(columns="_priority")
    labels = {"ticker": "Ticker", "currency": "Currency", "status": "Technical result", "severity": "Severity",
        "market_value_diff": "Value difference · base", "cash_diff": "Cash difference · currency", "sla_bucket": "SLA",
        "review_owner": "Review owner", "review_status": "Review status"}
    st.caption(f"{len(filtered)} of {len(frame)} records · Cash differences remain in their stated currency.")
    st.dataframe(filtered[list(labels)].rename(columns=labels).style.format(na_rep="—"), hide_index=True, width="stretch")
    download_csv("Export filtered queue", filtered, "exception_queue.csv", "queue_csv")
    candidates = filtered[~filtered["workflow_status"].eq("CLOSED")]
    if candidates.empty:
        st.info("No open breaks match these filters.")
        return
    options = candidates["exception_key"].tolist()
    indexed = candidates.set_index("exception_key")
    selected = st.selectbox("Inspect exception", options, format_func=lambda key:
        f"{indexed.loc[key, 'portfolio_id']} / {indexed.loc[key, 'account_id']} / "
        f"{indexed.loc[key, 'ticker'] if pd.notna(indexed.loc[key, 'ticker']) else indexed.loc[key, 'currency']} — {indexed.loc[key, 'status']}")
    row = indexed.loc[selected]
    left, right = st.columns([1, 1])
    with left:
        st.markdown("#### Source comparison")
        comparison = pd.DataFrame({"Measure": ["Quantity", "Price", "Market value · base", "Cash · stated currency"],
            "Internal": [row["internal_quantity"], row["internal_price"], row["internal_market_value"], row["internal_cash"]],
            "External": [row["external_quantity"], row["external_price"], row["external_market_value"], row["external_cash"]]})
        st.dataframe(comparison.style.format({"Internal": "{:,.2f}", "External": "{:,.2f}"}, na_rep="—"), hide_index=True, width="stretch")
        st.write("**Suggested investigation:**", row["action_required"])
        st.caption(f"Source date: {row['asof_date']} · Age: {row['age_days']} days as recorded by the run")
    with right:
        st.markdown("#### Record a review")
        with st.form(f"review_{selected}"):
            owner = st.text_input("Owner", value=str(row["review_owner"]) if pd.notna(row["review_owner"]) else "", max_chars=120, key=f"owner_{selected}")
            state = st.selectbox("Review status", list(REVIEW_STATUSES), index=list(REVIEW_STATUSES).index(row["review_status"]), key=f"status_{selected}")
            note = st.text_area("Investigation note", placeholder="Record the evidence reviewed and the next action (at least 10 characters).", max_chars=2000, key=f"note_{selected}")
            if st.form_submit_button("Save review", type="primary"):
                try:
                    save_review(DB_PATH, selected, owner, state, note)
                except ValueError as exc:
                    st.error(str(exc))
                else:
                    st.session_state["review_saved"] = True
                    st.rerun()
        st.caption("Each save appends a dated entry. Correct the source records and rerun controls to clear a technical break.")
    history = load_review_history(DB_PATH)
    if not history.empty:
        history = history[history["exception_key"].eq(selected)]
        with st.expander(f"Review history · {len(history)} entries", expanded=not history.empty):
            st.dataframe(history[["updated_at", "owner", "review_status", "note"]], hide_index=True, width="stretch")


def reports_view(data: dict[str, pd.DataFrame], policy: dict) -> None:
    st.subheader("Reporting pack")
    st.caption("Reports reflect the last calculation. The review journal is exported separately and does not alter sign-off.")
    latest = pd.to_datetime(data["daily"]["date"]).max()
    folder = ROOT / "outputs" / latest.strftime("%Y-%m")
    for name, label, mime in [("report.xlsx", "Download Excel report", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                               ("onepager.pdf", "Download performance tear sheet", "application/pdf")]:
        file = folder / name
        if file.exists():
            st.download_button(label, file.read_bytes(), name, mime)
    files = [p for p in folder.iterdir() if p.is_file() and p.suffix in {".csv", ".json", ".xlsx", ".pdf", ".png", ".md"}] if folder.exists() else []
    if files:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in files: archive.writestr(path.name, path.read_bytes())
        st.download_button("Download full reporting pack", buffer.getvalue(), f"pbor_{folder.name}.zip", "application/zip")
        summary_file = folder / "summary.json"
        if summary_file.exists():
            summary = json.loads(summary_file.read_text(encoding="utf-8"))
            st.caption(f"Calculated: {summary.get('generated_at_utc', 'Unknown')} · Reporting date: {summary.get('asof_date', 'Unknown')}")
    else:
        st.info("Recalculate the workspace to generate downloadable reports.")
    journal = load_review_history(DB_PATH)
    if not journal.empty: download_csv("Export complete review journal", journal, "review_journal.csv", "journal_csv")
    with st.expander("Calculation policy"):
        st.json(policy, expanded=False)
    st.markdown("**Methodology**")
    st.write("Daily time-weighted returns isolate investment performance from external cash flows. Modified Dietz provides a cash-flow-weighted comparison. Sector attribution uses Brinson–Fachler effects with a separate arithmetic return reconciliation.")
    st.write("Position and cash controls compare internal records with external files using the configured tolerances. Missing feeds and failed controls block reporting readiness. The bundled operational records include intentional exceptions.")


def main() -> None:
    st.markdown("""<style>
    .block-container {padding-top:2rem; max-width:1500px}
    [data-testid="stSidebar"] {border-right:1px solid #24334a}
    [data-testid="stMetric"] {padding:1rem; border:1px solid #28374c; border-radius:8px}
    [data-testid="stMetricLabel"] {font-size:.9rem}
    [data-testid="stMetricValue"] {font-size:1.6rem}
    h1 {letter-spacing:-.035em} h3 {letter-spacing:-.02em}
    [data-testid="stTabs"] button {font-size:1rem}
    </style>""", unsafe_allow_html=True)
    with st.sidebar:
        st.title("◈ PBOR")
        st.caption("PORTFOLIO CONTROL")
    st.title("Portfolio reconciliation & attribution")
    if not DB_PATH.exists():
        st.write("Open a working portfolio with performance, attribution, position checks and cash reconciliation.")
        st.info("The bundled workspace uses synthetic data for January 2026 and includes position and cash breaks to investigate.")
        if st.button("Open sample workspace", type="primary"):
            try: run_sample()
            except Exception as exc: st.error(f"The workspace could not be calculated: {exc}")
        return
    try:
        data = read_workspace()
    except (sqlite3.Error, pd.errors.DatabaseError) as exc:
        st.error("This workspace is incomplete or unavailable. Recalculate the bundled data to rebuild the reporting tables.")
        with st.expander("Error details"): st.code(str(exc))
        if st.button("Rebuild sample workspace", type="primary"): run_sample()
        return
    if data["monthly"].empty or data["daily"].empty:
        st.info("No calculated performance is available.")
        if st.button("Open sample workspace", type="primary"): run_sample()
        return
    policy = yaml.safe_load((ROOT / "policy.yaml").read_text(encoding="utf-8"))
    portfolios = sorted(data["monthly"]["portfolio_id"].unique().tolist())
    with st.sidebar:
        portfolio = st.selectbox("Performance portfolio", portfolios)
        st.caption("Performance and attribution follow this selection. Controls cover the complete run.")
        st.divider()
        st.markdown("**Workspace**")
        st.caption("Local data · bundled inputs are synthetic")
        if st.button("Refresh workspace", width="stretch"): st.rerun()
        with st.expander("Run calculations"):
            st.caption("Recalculate the bundled January 2026 inputs and current sample reconciliation files. Replaces calculated reports; review history is retained.")
            if st.button("Recalculate bundled data", type="primary"):
                try: run_sample()
                except Exception as exc: st.error(f"Calculation failed: {exc}")
        st.divider()
        st.caption("PBOR · Reconciliation, performance and reporting")
    monthly = scoped(data["monthly"], portfolio).sort_values("month_end")
    daily = scoped(data["daily"], portfolio).sort_values("date")
    attr = scoped(data["attribution"], portfolio)
    latest = monthly.iloc[-1]
    asof = pd.to_datetime(daily["date"]).max().strftime("%d %b %Y")
    st.caption(f"{portfolio} · {policy.get('base_currency', 'USD')} · Data through {asof}")
    if st.session_state.pop("review_saved", False): st.success("Review saved to the journal.")
    cols = st.columns(4)
    cols[0].metric("Portfolio value", number(daily.iloc[-1]["portfolio_value_base"], 2))
    cols[1].metric("Latest period · TWR", pct(latest["portfolio_return_twr"]))
    cols[2].metric("Benchmark", pct(latest["benchmark_return"]))
    cols[3].metric("Active return", pct(latest["active_return"]))
    tabs = st.tabs(["Control overview", "Performance", "Attribution", "Exception workbench", "Reports & methodology"])
    with tabs[0]: control_overview(data["signoff"], data["recon"], data["breaks"])
    with tabs[1]: performance(monthly, daily)
    with tabs[2]: attribution_view(monthly, attr, policy)
    with tabs[3]: reconciliation_view(data["recon"])
    with tabs[4]: reports_view(data, policy)


main()
