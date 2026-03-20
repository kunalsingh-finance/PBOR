from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from .dietz import compute_monthly_dietz


def _infer_cash_amount(row: pd.Series) -> float:
    if pd.notna(row.get("cash_amount")):
        return float(row["cash_amount"])

    txn_type = str(row.get("txn_type", "")).upper()
    quantity = float(row.get("quantity") or 0.0)
    price = float(row.get("price") or 0.0)
    fees = float(row.get("fees") or 0.0)
    notional = abs(quantity * price)

    if txn_type == "BUY":
        return -(notional + fees)
    if txn_type == "SELL":
        return notional - fees
    if txn_type in {"DIV", "INT"}:
        return notional if notional else abs(quantity)
    if txn_type == "CONTRIB":
        return notional if notional else abs(quantity) if quantity else abs(price)
    if txn_type == "WITHDRAW":
        base = notional if notional else abs(quantity) if quantity else abs(price)
        return -base
    return 0.0


def _build_positions(
    transactions: pd.DataFrame,
    prices: pd.DataFrame,
    fx_rates: pd.DataFrame,
    security_master: pd.DataFrame,
    base_currency: str,
    asof_date: pd.Timestamp,
) -> pd.DataFrame:
    tx = transactions.copy()
    tx["date"] = pd.to_datetime(tx["date"]).dt.date
    prices = prices.copy()
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
    fx_rates = fx_rates.copy()
    fx_rates["date"] = pd.to_datetime(fx_rates["date"]).dt.date

    buy_sell = tx[tx["txn_type"].isin(["BUY", "SELL"])].copy()
    if buy_sell.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "portfolio_id",
                "security_id",
                "quantity_eod",
                "price_local",
                "security_currency",
                "fx_to_base",
                "market_value_base",
            ]
        )

    buy_sell["signed_qty"] = np.where(
        buy_sell["txn_type"] == "BUY",
        buy_sell["quantity"].fillna(0.0),
        -buy_sell["quantity"].fillna(0.0),
    )
    qty_daily = buy_sell.groupby(["date", "portfolio_id", "security_id"], as_index=False)["signed_qty"].sum()

    min_date = min(qty_daily["date"].min(), prices["date"].min())
    # Keep holdings on a full calendar grid so positions carry cleanly across non-trading days.
    all_dates = pd.date_range(min_date, asof_date, freq="D").date

    pivot = qty_daily.pivot_table(
        index="date",
        columns=["portfolio_id", "security_id"],
        values="signed_qty",
        aggfunc="sum",
    ).reindex(all_dates, fill_value=0.0).fillna(0.0)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The previous implementation of stack is deprecated",
            category=FutureWarning,
        )
        cumulative = pivot.cumsum().stack(["portfolio_id", "security_id"]).reset_index(name="quantity_eod")
    cumulative.rename(columns={"level_0": "date"}, inplace=True)

    price_daily = prices.sort_values("source").drop_duplicates(subset=["date", "security_id"], keep="first")
    cumulative = cumulative.merge(
        price_daily[["date", "security_id", "price"]],
        on=["date", "security_id"],
        how="left",
    )
    cumulative.rename(columns={"price": "price_local"}, inplace=True)

    cumulative = cumulative.merge(
        security_master[["security_id", "currency"]],
        on="security_id",
        how="left",
    )
    cumulative.rename(columns={"currency": "security_currency"}, inplace=True)

    cumulative["ccy_pair"] = np.where(
        cumulative["security_currency"] == base_currency,
        None,
        cumulative["security_currency"].fillna("") + base_currency,
    )
    fx_daily = fx_rates.sort_values("source").drop_duplicates(subset=["date", "ccy_pair"], keep="first")
    cumulative = cumulative.merge(
        fx_daily[["date", "ccy_pair", "rate"]],
        on=["date", "ccy_pair"],
        how="left",
    )
    cumulative["fx_to_base"] = np.where(cumulative["security_currency"] == base_currency, 1.0, cumulative["rate"])
    cumulative["market_value_base"] = cumulative["quantity_eod"] * cumulative["price_local"] * cumulative["fx_to_base"]
    return cumulative.drop(columns=["rate"])


def _build_daily_cash(transactions: pd.DataFrame, start_date: pd.Timestamp, asof_date: pd.Timestamp) -> pd.DataFrame:
    tx = transactions.copy()
    tx["date"] = pd.to_datetime(tx["date"]).dt.date
    tx["cash_impact_base"] = tx.apply(_infer_cash_amount, axis=1)
    daily_flow = tx.groupby(["date", "portfolio_id"], as_index=False)["cash_impact_base"].sum()

    all_dates = pd.date_range(start_date, asof_date, freq="D").date
    portfolios = daily_flow["portfolio_id"].dropna().drop_duplicates().tolist()
    date_grid = (
        pd.MultiIndex.from_product([all_dates, portfolios], names=["date", "portfolio_id"])
        .to_frame(index=False)
        .sort_values(["portfolio_id", "date"])
    )
    merged = date_grid.merge(daily_flow, on=["date", "portfolio_id"], how="left").fillna({"cash_impact_base": 0.0})
    merged["cash_balance_base"] = merged.groupby("portfolio_id")["cash_impact_base"].cumsum()
    return merged


def _build_benchmark_daily(benchmark_weights: pd.DataFrame, benchmark_returns: pd.DataFrame) -> pd.DataFrame:
    weights = benchmark_weights.copy()
    returns = benchmark_returns.copy()
    weights["date"] = pd.to_datetime(weights["date"]).dt.date
    returns["date"] = pd.to_datetime(returns["date"]).dt.date

    joined = weights.merge(returns, on=["date", "benchmark_id", "sector"], how="inner")
    joined["weighted_return"] = joined["weight"] * joined["return"]
    bench = joined.groupby(["date", "benchmark_id"], as_index=False)["weighted_return"].sum()
    bench.rename(columns={"weighted_return": "benchmark_return"}, inplace=True)
    return bench


def _portfolio_benchmark_id(policy: dict[str, object], portfolio_id: str) -> str:
    benchmark_id_default = str(policy.get("benchmark_id_default", "BM1"))
    raw_map = policy.get("portfolio_benchmark_map", {})
    if not isinstance(raw_map, dict):
        return benchmark_id_default
    mapped = raw_map.get(str(portfolio_id))
    return str(mapped).strip().upper() if mapped else benchmark_id_default


def _link_returns(series: pd.Series) -> float:
    clean = series.dropna()
    if clean.empty:
        return np.nan
    return float((1.0 + clean).prod() - 1.0)


def compute_returns(
    inputs: dict[str, pd.DataFrame],
    policy: dict[str, object],
    asof_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    asof_day = pd.to_datetime(asof_date).date()
    base_currency = str(policy["base_currency"])
    prices = inputs["prices.csv"]
    transactions = inputs["transactions.csv"]
    security_master = inputs["security_master.csv"]
    fx_rates = inputs["fx_rates.csv"]
    benchmark_weights = inputs["benchmark_weights.csv"]
    benchmark_returns = inputs["benchmark_returns.csv"]

    positions = _build_positions(
        transactions=transactions,
        prices=prices,
        fx_rates=fx_rates,
        security_master=security_master,
        base_currency=base_currency,
        asof_date=asof_date,
    )
    start_date = min(pd.to_datetime(prices["date"]).min(), pd.to_datetime(transactions["date"]).min())
    daily_cash = _build_daily_cash(transactions, start_date=start_date, asof_date=asof_date)

    holdings_mv = (
        positions.groupby(["date", "portfolio_id"], as_index=False)["market_value_base"]
        .sum()
        .rename(columns={"market_value_base": "holdings_mv_base"})
    )
    daily_values = holdings_mv.merge(daily_cash[["date", "portfolio_id", "cash_balance_base"]], on=["date", "portfolio_id"], how="left")
    daily_values["cash_balance_base"] = daily_values["cash_balance_base"].fillna(0.0)
    daily_values["portfolio_value_base"] = daily_values["holdings_mv_base"] + daily_values["cash_balance_base"]

    tx = transactions.copy()
    tx["date"] = pd.to_datetime(tx["date"]).dt.date
    tx["cash_impact_base"] = tx.apply(_infer_cash_amount, axis=1)
    ext_flow = (
        tx[tx["txn_type"].isin(["CONTRIB", "WITHDRAW"])]
        .groupby(["date", "portfolio_id"], as_index=False)["cash_impact_base"]
        .sum()
        .rename(columns={"cash_impact_base": "external_flow_base"})
    )
    daily_returns = daily_values.merge(ext_flow, on=["date", "portfolio_id"], how="left")
    daily_returns["external_flow_base"] = daily_returns["external_flow_base"].fillna(0.0)
    daily_returns = daily_returns.sort_values(["portfolio_id", "date"])
    daily_returns["prev_value"] = daily_returns.groupby("portfolio_id")["portfolio_value_base"].shift(1)
    daily_returns["daily_return"] = np.where(
        daily_returns["prev_value"].abs() > 1e-12,
        (daily_returns["portfolio_value_base"] - daily_returns["prev_value"] - daily_returns["external_flow_base"])
        / daily_returns["prev_value"],
        np.nan,
    )

    benchmark_daily = _build_benchmark_daily(benchmark_weights, benchmark_returns)
    benchmark_daily = benchmark_daily[benchmark_daily["date"] <= asof_day].copy()
    daily_returns["benchmark_id"] = daily_returns["portfolio_id"].map(
        lambda portfolio_id: _portfolio_benchmark_id(policy=policy, portfolio_id=str(portfolio_id))
    )
    benchmark_daily_lookup = benchmark_daily.rename(columns={"benchmark_id": "mapped_benchmark_id"})
    daily_returns = daily_returns.merge(
        benchmark_daily_lookup,
        left_on=["date", "benchmark_id"],
        right_on=["date", "mapped_benchmark_id"],
        how="left",
    ).drop(columns=["mapped_benchmark_id"])
    daily_returns = daily_returns[
        [
            "date",
            "portfolio_id",
            "portfolio_value_base",
            "external_flow_base",
            "daily_return",
            "benchmark_return",
        ]
    ]

    daily_with_month = daily_returns.copy()
    daily_with_month["month_bucket"] = pd.to_datetime(daily_with_month["date"]).dt.to_period("M")
    monthly_twr = (
        daily_with_month.groupby(["portfolio_id", "month_bucket"], as_index=False)
        .agg(
            month_start=("date", "min"),
            month_end=("date", "max"),
            portfolio_return_twr=("daily_return", _link_returns),
        )
        .drop(columns=["month_bucket"])
    )
    monthly_arithmetic = (
        daily_with_month.groupby(["portfolio_id", "month_bucket"], as_index=False)
        .agg(
            month_start=("date", "min"),
            month_end=("date", "max"),
            begin_value=("portfolio_value_base", "first"),
            end_value=("portfolio_value_base", "last"),
            total_external_flow=("external_flow_base", "sum"),
        )
        .drop(columns=["month_bucket"])
    )
    start_flow = daily_with_month.assign(
        month_start=lambda x: x.groupby(["portfolio_id", "month_bucket"])["date"].transform("min")
    )
    start_flow = (
        start_flow[start_flow["date"] == start_flow["month_start"]][
            ["portfolio_id", "month_start", "external_flow_base"]
        ]
        .rename(columns={"external_flow_base": "start_day_external_flow"})
        .drop_duplicates(subset=["portfolio_id", "month_start"])
    )
    monthly_arithmetic = monthly_arithmetic.merge(
        start_flow,
        on=["portfolio_id", "month_start"],
        how="left",
    )
    monthly_arithmetic["start_day_external_flow"] = monthly_arithmetic["start_day_external_flow"].fillna(0.0)
    monthly_arithmetic["flow_excluding_start"] = (
        monthly_arithmetic["total_external_flow"] - monthly_arithmetic["start_day_external_flow"]
    )
    monthly_arithmetic["portfolio_return_arithmetic"] = np.where(
        monthly_arithmetic["begin_value"].abs() > 1e-12,
        (
            monthly_arithmetic["end_value"]
            - monthly_arithmetic["begin_value"]
            - monthly_arithmetic["flow_excluding_start"]
        )
        / monthly_arithmetic["begin_value"],
        np.nan,
    )
    monthly_arithmetic = monthly_arithmetic[
        [
            "portfolio_id",
            "month_start",
            "month_end",
            "portfolio_return_arithmetic",
        ]
    ]

    benchmark_monthly = (
        benchmark_daily.assign(month_bucket=lambda x: pd.to_datetime(x["date"]).dt.to_period("M"))
        .groupby(["benchmark_id", "month_bucket"], as_index=False)
        .agg(
            month_start=("date", "min"),
            month_end=("date", "max"),
            benchmark_return=("benchmark_return", _link_returns),
        )
        .drop(columns=["month_bucket"])
    )
    benchmark_monthly["benchmark_return_arithmetic"] = benchmark_monthly["benchmark_return"]

    monthly_dietz = compute_monthly_dietz(
        daily_values=daily_with_month[["date", "portfolio_id", "portfolio_value_base"]],
        external_flows=daily_with_month[["date", "portfolio_id", "external_flow_base"]],
        end_of_day=str(policy.get("cash_flow_timing", "end_of_day")).lower() == "end_of_day",
    )

    monthly_returns = monthly_twr.merge(monthly_dietz, on=["portfolio_id", "month_end"], how="left")
    monthly_returns = monthly_returns.merge(monthly_arithmetic, on=["portfolio_id", "month_start", "month_end"], how="left")
    monthly_returns["benchmark_id"] = monthly_returns["portfolio_id"].map(
        lambda portfolio_id: _portfolio_benchmark_id(policy=policy, portfolio_id=str(portfolio_id))
    )
    monthly_returns = monthly_returns.merge(
        benchmark_monthly,
        on=["benchmark_id", "month_start", "month_end"],
        how="left",
    )
    monthly_returns["active_return"] = monthly_returns["portfolio_return_twr"] - monthly_returns["benchmark_return"]
    monthly_returns["active_return_arithmetic"] = (
        monthly_returns["portfolio_return_arithmetic"] - monthly_returns["benchmark_return_arithmetic"]
    )
    monthly_returns = monthly_returns[
        [
            "month_start",
            "month_end",
            "portfolio_id",
            "portfolio_return_twr",
            "portfolio_return_arithmetic",
            "portfolio_return_dietz",
            "dietz_denominator",
            "benchmark_return",
            "benchmark_return_arithmetic",
            "active_return",
            "active_return_arithmetic",
        ]
    ]

    positions = positions.merge(
        daily_cash[["date", "portfolio_id", "cash_balance_base"]],
        on=["date", "portfolio_id"],
        how="left",
    )
    positions["cash_balance_base"] = positions["cash_balance_base"].fillna(0.0)
    positions = positions[
        [
            "date",
            "portfolio_id",
            "security_id",
            "quantity_eod",
            "price_local",
            "security_currency",
            "fx_to_base",
            "market_value_base",
            "cash_balance_base",
        ]
    ]
    return positions, daily_returns, monthly_returns, benchmark_daily
