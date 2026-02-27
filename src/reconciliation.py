from __future__ import annotations

import pandas as pd


def _active_return_column(monthly_returns: pd.DataFrame) -> str:
    return "active_return_arithmetic" if "active_return_arithmetic" in monthly_returns.columns else "active_return"


def _portfolio_return_column(monthly_returns: pd.DataFrame) -> str:
    return "portfolio_return_arithmetic" if "portfolio_return_arithmetic" in monthly_returns.columns else "portfolio_return_twr"


def attribution_reconciliation(
    monthly_returns: pd.DataFrame,
    attribution: pd.DataFrame,
    tolerance_bps: float,
) -> pd.DataFrame:
    if monthly_returns.empty:
        return pd.DataFrame(
            columns=[
                "month_end",
                "portfolio_id",
                "active_return_reference",
                "attribution_sum",
                "diff",
                "diff_bps",
                "w_p_sum",
                "w_b_sum",
                "weights_ok",
                "portfolio_return_reference",
                "portfolio_return_from_sectors",
                "portfolio_return_diff_bps",
                "portfolio_return_ok",
                "within_tolerance",
            ]
        )

    active_col = _active_return_column(monthly_returns)
    port_col = _portfolio_return_column(monthly_returns)
    monthly = monthly_returns[["month_end", "portfolio_id", active_col, port_col]].copy()
    monthly = monthly.rename(
        columns={
            active_col: "active_return_reference",
            port_col: "portfolio_return_reference",
        }
    )

    if attribution.empty:
        monthly["attribution_sum"] = 0.0
        monthly["w_p_sum"] = 0.0
        monthly["w_b_sum"] = 0.0
        monthly["portfolio_return_from_sectors"] = 0.0
    else:
        attr_sum = (
            attribution.groupby(["month_end", "portfolio_id"], as_index=False)["active_effect"]
            .sum()
            .rename(columns={"active_effect": "attribution_sum"})
        )
        weight_sums = (
            attribution.groupby(["month_end", "portfolio_id"], as_index=False)
            .agg(w_p_sum=("w_p", "sum"), w_b_sum=("w_b", "sum"))
        )
        sector_port_return = (
            attribution.assign(weighted_sector_return=lambda x: x["w_p"] * x["r_p"])
            .groupby(["month_end", "portfolio_id"], as_index=False)["weighted_sector_return"]
            .sum()
            .rename(columns={"weighted_sector_return": "portfolio_return_from_sectors"})
        )
        monthly = monthly.merge(attr_sum, on=["month_end", "portfolio_id"], how="left")
        monthly = monthly.merge(weight_sums, on=["month_end", "portfolio_id"], how="left")
        monthly = monthly.merge(sector_port_return, on=["month_end", "portfolio_id"], how="left")
        monthly[["attribution_sum", "w_p_sum", "w_b_sum", "portfolio_return_from_sectors"]] = monthly[
            ["attribution_sum", "w_p_sum", "w_b_sum", "portfolio_return_from_sectors"]
        ].fillna(0.0)

    monthly["diff"] = monthly["attribution_sum"] - monthly["active_return_reference"]
    monthly["diff_bps"] = monthly["diff"].abs() * 10000.0
    monthly["weights_ok"] = (monthly["w_p_sum"] - 1.0).abs() < 1e-6
    monthly["weights_ok"] = monthly["weights_ok"] & ((monthly["w_b_sum"] - 1.0).abs() < 1e-6)
    monthly["portfolio_return_diff_bps"] = (
        (monthly["portfolio_return_from_sectors"] - monthly["portfolio_return_reference"]).abs() * 10000.0
    )
    monthly["portfolio_return_ok"] = monthly["portfolio_return_diff_bps"] < float(tolerance_bps)
    monthly["within_tolerance"] = (
        (monthly["diff_bps"] < float(tolerance_bps)) & monthly["weights_ok"] & monthly["portfolio_return_ok"]
    )
    return monthly


def latest_reconciliation(
    monthly_returns: pd.DataFrame,
    attribution: pd.DataFrame,
    tolerance_bps: float,
) -> dict[str, object]:
    recon = attribution_reconciliation(monthly_returns, attribution, tolerance_bps=tolerance_bps)
    if recon.empty:
        return {
            "available": False,
            "within_tolerance": False,
            "attribution_sum": 0.0,
            "active_return": 0.0,
            "diff_bps": float("inf"),
            "w_p_sum": 0.0,
            "w_b_sum": 0.0,
            "weights_ok": False,
            "portfolio_return_reference": 0.0,
            "portfolio_return_from_sectors": 0.0,
            "portfolio_return_diff_bps": float("inf"),
            "portfolio_return_ok": False,
            "month_end": None,
            "portfolio_id": None,
        }

    latest = recon.sort_values("month_end").iloc[-1]
    return {
        "available": True,
        "within_tolerance": bool(latest["within_tolerance"]),
        "attribution_sum": float(latest["attribution_sum"]),
        "active_return": float(latest["active_return_reference"]),
        "diff_bps": float(latest["diff_bps"]),
        "w_p_sum": float(latest["w_p_sum"]),
        "w_b_sum": float(latest["w_b_sum"]),
        "weights_ok": bool(latest["weights_ok"]),
        "portfolio_return_reference": float(latest["portfolio_return_reference"]),
        "portfolio_return_from_sectors": float(latest["portfolio_return_from_sectors"]),
        "portfolio_return_diff_bps": float(latest["portfolio_return_diff_bps"]),
        "portfolio_return_ok": bool(latest["portfolio_return_ok"]),
        "month_end": latest["month_end"],
        "portfolio_id": latest["portfolio_id"],
    }
