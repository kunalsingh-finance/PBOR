from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass
class DietzResult:
    period_return: float
    denominator: float


def modified_dietz(
    period_start: date,
    period_end: date,
    begin_value: float,
    end_value: float,
    flows: pd.DataFrame,
    end_of_day: bool = True,
) -> DietzResult:
    period_days = max((period_end - period_start).days, 1)
    flows = flows.copy()
    if flows.empty:
        denominator = begin_value
        numerator = end_value - begin_value
        period_return = numerator / denominator if denominator else 0.0
        return DietzResult(period_return=period_return, denominator=denominator)

    flows["days_remaining"] = (pd.to_datetime(period_end) - pd.to_datetime(flows["flow_date"])).dt.days
    if end_of_day:
        flows["days_remaining"] = (flows["days_remaining"] - 1).clip(lower=0)
    flows["weight"] = flows["days_remaining"] / period_days

    flow_total = flows["amount"].sum()
    weighted_flow = (flows["weight"] * flows["amount"]).sum()
    denominator = begin_value + weighted_flow
    numerator = end_value - begin_value - flow_total
    period_return = numerator / denominator if denominator else 0.0
    return DietzResult(period_return=period_return, denominator=denominator)


def compute_monthly_dietz(
    daily_values: pd.DataFrame,
    external_flows: pd.DataFrame,
    end_of_day: bool = True,
) -> pd.DataFrame:
    if daily_values.empty:
        return pd.DataFrame(
            columns=[
                "month_end",
                "portfolio_id",
                "portfolio_return_dietz",
                "dietz_denominator",
            ]
        )

    values = daily_values.copy()
    values["date"] = pd.to_datetime(values["date"]).dt.date
    flows = external_flows.copy()
    flows["date"] = pd.to_datetime(flows["date"]).dt.date

    values["month_bucket"] = pd.to_datetime(values["date"]).dt.to_period("M")
    rows: list[dict[str, object]] = []

    grouped = values.sort_values("date").groupby(["portfolio_id", "month_bucket"], as_index=False)
    for (portfolio_id, _month_bucket), chunk in grouped:
        chunk = chunk.sort_values("date")
        month_start = chunk["date"].iloc[0]
        month_end = chunk["date"].iloc[-1]
        begin_value = float(chunk["portfolio_value_base"].iloc[0])
        end_value = float(chunk["portfolio_value_base"].iloc[-1])

        month_flows = flows[
            (flows["portfolio_id"] == portfolio_id)
            & (flows["date"] >= month_start)
            & (flows["date"] <= month_end)
        ][["date", "external_flow_base"]].rename(columns={"date": "flow_date", "external_flow_base": "amount"})

        start_day_flow = month_flows.loc[month_flows["flow_date"] == month_start, "amount"].sum()
        begin_value = begin_value - float(start_day_flow)

        result = modified_dietz(
            period_start=month_start,
            period_end=month_end,
            begin_value=begin_value,
            end_value=end_value,
            flows=month_flows,
            end_of_day=end_of_day,
        )
        rows.append(
            {
                "month_end": month_end,
                "portfolio_id": portfolio_id,
                "portfolio_return_dietz": result.period_return,
                "dietz_denominator": result.denominator,
            }
        )

    return pd.DataFrame(rows)
