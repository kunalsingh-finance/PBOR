from __future__ import annotations

import numpy as np
import pandas as pd


def _policy_cash_return(
    cash_return_source: str,
    cash_return_annual_rates: dict[str, float] | None,
    period_days: int,
) -> float:
    source = str(cash_return_source).strip().upper()
    if source in {"0%", "0", "ZERO", "NONE"}:
        annual_rate = 0.0
    else:
        rates = {str(k).strip().upper(): float(v) for k, v in (cash_return_annual_rates or {}).items()}
        annual_rate = float(rates.get(source, 0.0))
    days = max(int(period_days), 0)
    return (1.0 + annual_rate) ** (days / 365.0) - 1.0


def brinson_fachler_effects(sector_frame: pd.DataFrame) -> pd.DataFrame:
    frame = sector_frame.copy()
    frame["benchmark_total_return"] = (frame["w_b"] * frame["r_b"]).groupby(
        [frame["month_end"], frame["portfolio_id"], frame["benchmark_id"]]
    ).transform("sum")
    frame["allocation_effect"] = (frame["w_p"] - frame["w_b"]) * (frame["r_b"] - frame["benchmark_total_return"])
    frame["selection_effect"] = frame["w_b"] * (frame["r_p"] - frame["r_b"])
    frame["interaction_effect"] = (frame["w_p"] - frame["w_b"]) * (frame["r_p"] - frame["r_b"])
    frame["active_effect"] = frame["allocation_effect"] + frame["selection_effect"] + frame["interaction_effect"]
    return frame


def _trade_flow_by_sector(
    transactions: pd.DataFrame | None,
    security_master: pd.DataFrame,
) -> pd.DataFrame:
    if transactions is None or transactions.empty:
        return pd.DataFrame(columns=["portfolio_id", "date", "month_bucket", "sector", "net_internal_flow"])

    tx = transactions.copy()
    tx["txn_type"] = tx["txn_type"].astype(str).str.upper()
    tx["date"] = pd.to_datetime(tx["date"]).dt.date
    tx = tx[tx["txn_type"].isin(["BUY", "SELL"]) & tx["security_id"].notna()].copy()
    if tx.empty:
        return pd.DataFrame(columns=["portfolio_id", "date", "month_bucket", "sector", "net_internal_flow"])

    tx = tx.merge(security_master[["security_id", "sector"]], on="security_id", how="left")
    tx["sector"] = tx["sector"].fillna("UNCLASSIFIED")
    tx["month_bucket"] = pd.to_datetime(tx["date"]).dt.to_period("M")
    tx["notional"] = tx["quantity"].fillna(0.0).abs() * tx["price"].fillna(0.0).abs()
    tx["net_internal_flow"] = np.where(tx["txn_type"] == "BUY", tx["notional"], -tx["notional"])
    return tx.groupby(["portfolio_id", "date", "month_bucket", "sector"], as_index=False)["net_internal_flow"].sum()


def _income_by_sector(
    transactions: pd.DataFrame | None,
    security_master: pd.DataFrame,
) -> pd.DataFrame:
    if transactions is None or transactions.empty:
        return pd.DataFrame(columns=["portfolio_id", "month_bucket", "sector", "sector_income"])

    tx = transactions.copy()
    tx["txn_type"] = tx["txn_type"].astype(str).str.upper()
    tx["date"] = pd.to_datetime(tx["date"]).dt.date
    tx = tx[tx["txn_type"].isin(["DIV", "INT"]) & tx["security_id"].notna()].copy()
    if tx.empty:
        return pd.DataFrame(columns=["portfolio_id", "month_bucket", "sector", "sector_income"])

    tx = tx.merge(security_master[["security_id", "sector"]], on="security_id", how="left")
    tx["sector"] = tx["sector"].fillna("UNCLASSIFIED")
    tx["month_bucket"] = pd.to_datetime(tx["date"]).dt.to_period("M")
    tx["sector_income"] = np.where(
        tx["cash_amount"].notna(),
        tx["cash_amount"].astype(float),
        tx["quantity"].fillna(0.0).abs() * tx["price"].fillna(0.0).abs(),
    )
    return tx.groupby(["portfolio_id", "month_bucket", "sector"], as_index=False)["sector_income"].sum()


def _portfolio_sector_period(
    positions: pd.DataFrame,
    security_master: pd.DataFrame,
    transactions: pd.DataFrame | None,
    cash_return_source: str,
    cash_return_annual_rates: dict[str, float] | None,
) -> pd.DataFrame:
    pos = positions.copy()
    pos["date"] = pd.to_datetime(pos["date"]).dt.date

    sec_mv = pos.merge(security_master[["security_id", "sector"]], on="security_id", how="left")
    sec_mv["sector"] = sec_mv["sector"].fillna("UNCLASSIFIED")
    sec_daily = sec_mv.groupby(["portfolio_id", "date", "sector"], as_index=False)["market_value_base"].sum()

    cash_daily = (
        pos.groupby(["portfolio_id", "date"], as_index=False)["cash_balance_base"]
        .max()
        .rename(columns={"cash_balance_base": "market_value_base"})
    )
    cash_daily["sector"] = "Cash"

    sector_daily = pd.concat([sec_daily, cash_daily], ignore_index=True)
    sector_daily["month_bucket"] = pd.to_datetime(sector_daily["date"]).dt.to_period("M")

    bounds = sector_daily.groupby(["portfolio_id", "month_bucket"], as_index=False)["date"].agg(
        first_date="min", month_end="max"
    )
    bounds["period_days"] = (pd.to_datetime(bounds["month_end"]) - pd.to_datetime(bounds["first_date"])).dt.days + 1
    sector_daily = sector_daily.merge(bounds, on=["portfolio_id", "month_bucket"], how="left")

    bop = sector_daily[sector_daily["date"] == sector_daily["first_date"]][
        ["portfolio_id", "month_bucket", "month_end", "sector", "market_value_base"]
    ].rename(columns={"market_value_base": "bop_mv"})
    eop = sector_daily[sector_daily["date"] == sector_daily["month_end"]][
        ["portfolio_id", "month_bucket", "sector", "market_value_base"]
    ].rename(columns={"market_value_base": "eop_mv"})

    flows = _trade_flow_by_sector(transactions=transactions, security_master=security_master)
    if not flows.empty:
        flows = flows.merge(bounds, on=["portfolio_id", "month_bucket"], how="left")
        flows = flows[flows["date"] > flows["first_date"]]
        flows = flows.groupby(["portfolio_id", "month_bucket", "sector"], as_index=False)["net_internal_flow"].sum()
    income = _income_by_sector(transactions=transactions, security_master=security_master)
    period = bop.merge(eop, on=["portfolio_id", "month_bucket", "sector"], how="outer")
    for col in ["bop_mv", "eop_mv"]:
        period[col] = pd.to_numeric(period[col], errors="coerce").fillna(0.0)
    period = period.merge(flows, on=["portfolio_id", "month_bucket", "sector"], how="left")
    period["net_internal_flow"] = pd.to_numeric(period["net_internal_flow"], errors="coerce").fillna(0.0)
    period = period.merge(income, on=["portfolio_id", "month_bucket", "sector"], how="left")
    period["sector_income"] = pd.to_numeric(period["sector_income"], errors="coerce").fillna(0.0)
    period = period.merge(
        bounds[["portfolio_id", "month_bucket", "period_days"]],
        on=["portfolio_id", "month_bucket"],
        how="left",
    )
    period["period_days"] = period["period_days"].fillna(0).astype(int)
    period["total_bop_mv"] = period.groupby(["portfolio_id", "month_bucket"])["bop_mv"].transform("sum")
    period["w_p"] = np.where(period["total_bop_mv"].abs() > 1e-12, period["bop_mv"] / period["total_bop_mv"], 0.0)

    # Remove buy/sell transfer effects from sector return; cash return follows configured policy source.
    period["cash_policy_return"] = period["period_days"].apply(
        lambda x: _policy_cash_return(
            cash_return_source=cash_return_source,
            cash_return_annual_rates=cash_return_annual_rates,
            period_days=int(x),
        )
    )
    period["r_p"] = np.where(
        period["sector"].astype(str).str.lower() == "cash",
        period["cash_policy_return"],
        np.where(
            period["bop_mv"].abs() > 1e-12,
            (
                period["eop_mv"]
                - period["bop_mv"]
                - period["net_internal_flow"]
                + period["sector_income"]
            )
            / period["bop_mv"],
            0.0,
        ),
    )
    return period


def _benchmark_sector_period(
    benchmark_weights: pd.DataFrame,
    benchmark_returns: pd.DataFrame,
) -> pd.DataFrame:
    bw = benchmark_weights.copy()
    br = benchmark_returns.copy()
    bw["date"] = pd.to_datetime(bw["date"]).dt.date
    br["date"] = pd.to_datetime(br["date"]).dt.date
    bw["month_bucket"] = pd.to_datetime(bw["date"]).dt.to_period("M")
    br["month_bucket"] = pd.to_datetime(br["date"]).dt.to_period("M")

    bw_month = (
        bw.sort_values("date")
        .groupby(["benchmark_id", "month_bucket", "sector"], as_index=False)
        .first()[["benchmark_id", "month_bucket", "sector", "weight"]]
        .rename(columns={"weight": "w_b"})
    )
    br_month = (
        br.groupby(["benchmark_id", "month_bucket", "sector"], as_index=False)["return"]
        .apply(lambda s: (1.0 + s).prod() - 1.0)
        .rename(columns={"return": "r_b"})
    )
    month_end = br.groupby(["benchmark_id", "month_bucket"], as_index=False)["date"].max().rename(
        columns={"date": "month_end"}
    )
    bench = bw_month.merge(br_month, on=["benchmark_id", "month_bucket", "sector"], how="outer")
    bench["w_b"] = pd.to_numeric(bench["w_b"], errors="coerce").fillna(0.0)
    bench["r_b"] = pd.to_numeric(bench["r_b"], errors="coerce").fillna(0.0)
    bench = bench.merge(month_end, on=["benchmark_id", "month_bucket"], how="left")
    return bench


def compute_monthly_attribution(
    positions: pd.DataFrame,
    security_master: pd.DataFrame,
    benchmark_weights: pd.DataFrame,
    benchmark_returns: pd.DataFrame,
    benchmark_id_default: str,
    portfolio_benchmark_map: dict[str, str] | None = None,
    transactions: pd.DataFrame | None = None,
    cash_return_source: str = "0%",
    cash_return_annual_rates: dict[str, float] | None = None,
) -> pd.DataFrame:
    if positions.empty:
        return pd.DataFrame(
            columns=[
                "month_end",
                "portfolio_id",
                "benchmark_id",
                "sector",
                "w_p",
                "w_b",
                "r_p",
                "r_b",
                "allocation_effect",
                "selection_effect",
                "interaction_effect",
                "active_effect",
            ]
        )

    asof_day = pd.to_datetime(positions["date"]).max().date()
    benchmark_weights = benchmark_weights.copy()
    benchmark_returns = benchmark_returns.copy()
    benchmark_weights["date"] = pd.to_datetime(benchmark_weights["date"]).dt.date
    benchmark_returns["date"] = pd.to_datetime(benchmark_returns["date"]).dt.date
    benchmark_weights = benchmark_weights[benchmark_weights["date"] <= asof_day].copy()
    benchmark_returns = benchmark_returns[benchmark_returns["date"] <= asof_day].copy()

    port = _portfolio_sector_period(
        positions=positions,
        security_master=security_master,
        transactions=transactions,
        cash_return_source=cash_return_source,
        cash_return_annual_rates=cash_return_annual_rates,
    )
    bench = _benchmark_sector_period(
        benchmark_weights=benchmark_weights,
        benchmark_returns=benchmark_returns,
    )

    rows: list[dict[str, object]] = []
    benchmark_map = {
        str(portfolio_id): str(benchmark_id).strip().upper()
        for portfolio_id, benchmark_id in (portfolio_benchmark_map or {}).items()
    }
    for (portfolio_id, month_bucket), port_chunk in port.groupby(["portfolio_id", "month_bucket"]):
        month_end = port_chunk["month_end"].max()
        benchmark_id = benchmark_map.get(str(portfolio_id), benchmark_id_default)
        bench_chunk = bench[
            (bench["benchmark_id"] == benchmark_id)
            & (bench["month_bucket"] == month_bucket)
        ]

        port_map = {
            str(r["sector"]): {"w_p": float(r["w_p"]), "r_p": float(r["r_p"])}
            for _, r in port_chunk.iterrows()
        }
        bench_map = {
            str(r["sector"]): {"w_b": float(r["w_b"]), "r_b": float(r["r_b"])}
            for _, r in bench_chunk.iterrows()
        }
        sectors = sorted(set(port_map.keys()) | set(bench_map.keys()) | {"Cash"})

        for sector in sectors:
            p = port_map.get(sector, {"w_p": 0.0, "r_p": 0.0})
            b = bench_map.get(sector, {"w_b": 0.0, "r_b": 0.0})
            rows.append(
                {
                    "month_end": month_end,
                    "portfolio_id": portfolio_id,
                    "benchmark_id": benchmark_id,
                    "sector": sector,
                    "w_p": p["w_p"],
                    "w_b": b["w_b"],
                    "r_p": p["r_p"],
                    "r_b": b["r_b"],
                }
            )

    merged = pd.DataFrame(rows)
    effects = brinson_fachler_effects(merged)
    return effects[
        [
            "month_end",
            "portfolio_id",
            "benchmark_id",
            "sector",
            "w_p",
            "w_b",
            "r_p",
            "r_b",
            "allocation_effect",
            "selection_effect",
            "interaction_effect",
            "active_effect",
        ]
    ]
