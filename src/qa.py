from __future__ import annotations

from datetime import date

import pandas as pd


def _flow_window_frame(
    daily_returns: pd.DataFrame,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
) -> pd.DataFrame:
    if daily_returns.empty or "date" not in daily_returns.columns:
        return daily_returns
    frame = daily_returns.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    if start_date is not None:
        start = pd.to_datetime(start_date).date()
        frame = frame[frame["date"] >= start]
    if end_date is not None:
        end = pd.to_datetime(end_date).date()
        frame = frame[frame["date"] <= end]
    return frame


def flow_summary_stats(
    daily_returns: pd.DataFrame,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
) -> dict[str, object]:
    windowed = _flow_window_frame(daily_returns=daily_returns, start_date=start_date, end_date=end_date)
    if windowed.empty or "external_flow_base" not in windowed.columns:
        return {
            "net_flow": 0.0,
            "net_flow_mtd": 0.0,
            "largest_flow_date": None,
            "largest_flow_amount": 0.0,
            "flows_present": False,
        }

    flow_by_date = (
        windowed.assign(date=pd.to_datetime(windowed["date"]).dt.date)
        .groupby("date", as_index=False)["external_flow_base"]
        .sum()
        .sort_values("date", kind="mergesort")
    )
    if flow_by_date.empty:
        return {
            "net_flow": 0.0,
            "net_flow_mtd": 0.0,
            "largest_flow_date": None,
            "largest_flow_amount": 0.0,
            "flows_present": False,
        }

    largest_idx = flow_by_date["external_flow_base"].abs().idxmax()
    largest_row = flow_by_date.loc[largest_idx]
    net_flow = float(flow_by_date["external_flow_base"].sum())
    return {
        "net_flow": net_flow,
        "net_flow_mtd": net_flow,
        "largest_flow_date": str(largest_row["date"]),
        "largest_flow_amount": float(largest_row["external_flow_base"]),
        "flows_present": bool(flow_by_date["external_flow_base"].abs().max() > 1e-12),
    }


def format_flow_summary_line(
    daily_returns: pd.DataFrame,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    window_label: str = "MTD",
) -> str:
    stats = flow_summary_stats(daily_returns=daily_returns, start_date=start_date, end_date=end_date)
    largest_date = str(stats["largest_flow_date"]) if stats["largest_flow_date"] else "N/A"
    flows_present = "Yes" if bool(stats["flows_present"]) else "No"
    return (
        f"Net flow ({window_label}): ${float(stats['net_flow']):,.0f} | "
        f"Largest flow: ${float(stats['largest_flow_amount']):,.0f} on {largest_date} | "
        f"Flows present: {flows_present}"
    )


def _classify_outlier_cause(
    outlier_row: pd.Series,
    transactions: pd.DataFrame,
    positions: pd.DataFrame,
) -> tuple[str, str]:
    outlier_date = pd.to_datetime(outlier_row["date"]).date()
    portfolio_id = str(outlier_row["portfolio_id"])

    tx_day = transactions[
        (pd.to_datetime(transactions["date"]).dt.date == outlier_date)
        & (transactions["portfolio_id"].astype(str) == portfolio_id)
    ]
    pos_day = positions[
        (pd.to_datetime(positions["date"]).dt.date == outlier_date)
        & (positions["portfolio_id"].astype(str) == portfolio_id)
        & (positions["quantity_eod"].abs() > 1e-12)
    ]

    missing_price_secs = pos_day[pos_day["price_local"].isna()]["security_id"].dropna().astype(str).unique().tolist()
    if missing_price_secs:
        security_list = ", ".join(sorted(missing_price_secs))
        return (
            f"Price stale/missing for security {security_list}",
            "Load complete pricing for impacted securities and rerun month-end.",
        )

    corp_action_txn = tx_day[tx_day["txn_type"].isin(["SPLIT", "DIV", "INT"])]
    if not corp_action_txn.empty:
        events = ", ".join(sorted(corp_action_txn["txn_type"].astype(str).unique().tolist()))
        return (
            f"Corporate action applied ({events})",
            "Validate corporate action booking and adjusted price/share factors.",
        )

    if abs(float(outlier_row.get("external_flow_base", 0.0))) > 1e-12 or not tx_day[
        tx_day["txn_type"].isin(["CONTRIB", "WITHDRAW"])
    ].empty:
        return (
            "Flow/valuation timing mismatch",
            "Confirm external flow timestamp versus valuation cut-off and rerun.",
        )

    return (
        "Flow/valuation timing mismatch",
        "Review same-day trades, valuations, and cut-off timestamps.",
    )


def _break_row(
    asof_date: date,
    break_type: str,
    severity: str,
    details: str,
    portfolio_id: str | None = None,
    root_cause: str | None = None,
    resolution: str | None = None,
) -> dict[str, object]:
    return {
        "asof_date": asof_date,
        "portfolio_id": portfolio_id,
        "break_type": break_type,
        "severity": severity,
        "details": details,
        "root_cause": root_cause,
        "resolution": resolution,
    }


def run_break_checks(
    asof_date: date,
    policy: dict[str, object],
    inputs: dict[str, pd.DataFrame],
    positions: pd.DataFrame,
    daily_returns: pd.DataFrame,
    monthly_returns: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    base_currency = str(policy["base_currency"])
    outlier_threshold = float(policy.get("outlier_return_threshold", 0.2))
    holdings_tolerance = float(policy.get("holdings_tolerance", 0.0001))
    nav_jump_threshold = float(policy.get("nav_jump_zero_flow_threshold", 0.1))

    prices = inputs["prices.csv"]
    transactions = inputs["transactions.csv"].copy()
    transactions["txn_type"] = transactions["txn_type"].astype(str).str.upper()
    security_master = inputs["security_master.csv"]
    holdings_reported = inputs["holdings_reported.csv"]

    duplicate_prices = prices[prices.duplicated(subset=["date", "security_id", "source"], keep=False)]
    for _, row in duplicate_prices.iterrows():
        rows.append(
            _break_row(
                asof_date,
                break_type="DUPLICATE_PRICE",
                severity="MEDIUM",
                details=f"Duplicate price for security={row['security_id']} date={row['date']}",
                root_cause="Duplicate source rows",
                resolution="Deduplicate by source priority",
            )
        )

    known_ids = set(security_master["security_id"])
    unknown_tx = transactions[
        transactions["security_id"].notna() & ~transactions["security_id"].isin(known_ids)
    ]
    for _, row in unknown_tx.iterrows():
        rows.append(
            _break_row(
                asof_date,
                break_type="UNKNOWN_SECURITY_ID",
                severity="HIGH",
                details=f"Unknown security_id in transactions: {row['security_id']}",
                portfolio_id=str(row["portfolio_id"]),
                root_cause="Security master missing mapping",
                resolution="Map security_id or fix transaction feed",
            )
        )

    missing_prices = positions[(positions["quantity_eod"].abs() > 1e-12) & (positions["price_local"].isna())]
    for _, row in missing_prices.iterrows():
        rows.append(
            _break_row(
                asof_date,
                break_type="MISSING_PRICE",
                severity="HIGH",
                details=f"Held security {row['security_id']} has missing price on {row['date']}",
                portfolio_id=str(row["portfolio_id"]),
                root_cause="Price feed gap",
                resolution="Backfill or manually price security",
            )
        )

    missing_fx = positions[
        (positions["security_currency"] != base_currency)
        & (positions["quantity_eod"].abs() > 1e-12)
        & (positions["fx_to_base"].isna())
    ]
    for _, row in missing_fx.iterrows():
        rows.append(
            _break_row(
                asof_date,
                break_type="MISSING_FX_RATE",
                severity="HIGH",
                details=f"Missing FX rate for {row['security_currency']} on {row['date']}",
                portfolio_id=str(row["portfolio_id"]),
                root_cause="FX feed gap",
                resolution="Fill nearest valid rate with audit note",
            )
        )

    outlier_returns = daily_returns[daily_returns["daily_return"].abs() > outlier_threshold]
    for _, row in outlier_returns.iterrows():
        root_cause, resolution = _classify_outlier_cause(
            outlier_row=row,
            transactions=transactions,
            positions=positions,
        )
        rows.append(
            _break_row(
                asof_date,
                break_type="RETURN_OUTLIER",
                severity="MEDIUM",
                details=f"Daily return {row['daily_return']:.4f} exceeds threshold on {row['date']}",
                portfolio_id=str(row["portfolio_id"]),
                root_cause=root_cause,
                resolution=resolution,
            )
        )

    daily_sorted = daily_returns.sort_values(["portfolio_id", "date"]).copy()
    daily_sorted["prev_value"] = daily_sorted.groupby("portfolio_id")["portfolio_value_base"].shift(1)
    daily_sorted["delta_nav"] = daily_sorted["portfolio_value_base"] - daily_sorted["prev_value"]
    nav_jump = daily_sorted[
        (daily_sorted["prev_value"].abs() > 1e-12)
        & (daily_sorted["external_flow_base"].abs() <= 1e-12)
        & ((daily_sorted["delta_nav"].abs() / daily_sorted["prev_value"].abs()) > nav_jump_threshold)
    ]
    for _, row in nav_jump.iterrows():
        implied_missing_flow = row["portfolio_value_base"] - (
            row["prev_value"] * (1.0 + float(row.get("benchmark_return", 0.0)))
        )
        rows.append(
            _break_row(
                asof_date,
                break_type="NAV_JUMP_ZERO_FLOW",
                severity="HIGH",
                details=(
                    f"NAV jump with zero flow on {row['date']}; "
                    f"implied_missing_flow_base={float(implied_missing_flow):.2f}"
                ),
                portfolio_id=str(row["portfolio_id"]),
                root_cause="Potential missing flow or valuation break",
                resolution="Reconcile transactions, valuations, and corporate actions for the date.",
            )
        )

    if bool(policy.get("long_only", True)):
        negative_mv = positions[(positions["quantity_eod"] > 0) & (positions["market_value_base"] < -1e-9)]
        for _, row in negative_mv.iterrows():
            rows.append(
                _break_row(
                    asof_date,
                    break_type="NEGATIVE_MARKET_VALUE",
                    severity="HIGH",
                    details=f"Negative MV for long position {row['security_id']} on {row['date']}",
                    portfolio_id=str(row["portfolio_id"]),
                    root_cause="Sign inversion or bad price",
                    resolution="Validate quantity and price direction",
                )
            )

    near_zero_denom = monthly_returns[monthly_returns["dietz_denominator"].abs() < 1e-6]
    for _, row in near_zero_denom.iterrows():
        rows.append(
            _break_row(
                asof_date,
                break_type="DIETZ_DENOMINATOR_NEAR_ZERO",
                severity="MEDIUM",
                details=f"Dietz denominator near zero for month {row['month_end']}",
                portfolio_id=str(row["portfolio_id"]),
                root_cause="External flow dominates period",
                resolution="Use subperiod valuation or review flow timing",
            )
        )

    asof_reported = holdings_reported[pd.to_datetime(holdings_reported["date"]).dt.date == asof_date]
    asof_rebuilt = positions[pd.to_datetime(positions["date"]).dt.date == asof_date]
    rebuilt_qty = asof_rebuilt.groupby(["portfolio_id", "security_id"], as_index=False)["quantity_eod"].sum()
    compare = asof_reported.merge(
        rebuilt_qty,
        on=["portfolio_id", "security_id"],
        how="outer",
    ).fillna({"quantity": 0.0, "quantity_eod": 0.0})
    compare["diff"] = (compare["quantity"] - compare["quantity_eod"]).abs()
    mismatches = compare[compare["diff"] > holdings_tolerance]
    for _, row in mismatches.iterrows():
        rows.append(
            _break_row(
                asof_date,
                break_type="HOLDINGS_MISMATCH",
                severity="HIGH",
                details=f"Reported vs rebuilt mismatch for {row['security_id']}: {row['quantity']} vs {row['quantity_eod']}",
                portfolio_id=str(row["portfolio_id"]),
                root_cause="Missing transaction or corporate action",
                resolution="Reconcile transaction history and custodian snapshot",
            )
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "asof_date",
                "portfolio_id",
                "break_type",
                "severity",
                "details",
                "root_cause",
                "resolution",
            ]
        )
    return pd.DataFrame(rows)
