from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .date_source import (
    derive_date_context,
    get_now_et,
    get_trading_sessions as _get_trading_sessions,
    last_trading_session_on_or_before as _last_trading_session_on_or_before,
)


def get_trading_sessions(
    start_date: date | datetime | str,
    end_date: date | datetime | str,
    exchange: str = "XNYS",
    cache_dir: Path | None = None,
) -> pd.DatetimeIndex:
    return _get_trading_sessions(
        start_date=start_date,
        end_date=end_date,
        exchange=exchange,
        cache_dir=cache_dir,
    )


def last_trading_session_on_or_before(
    value: date | datetime | str,
    exchange: str = "XNYS",
    cache_dir: Path | None = None,
) -> date:
    return _last_trading_session_on_or_before(
        value=value,
        exchange=exchange,
        cache_dir=cache_dir,
    )


def derive_reporting_windows(
    daily_returns: pd.DataFrame,
    exchange: str = "XNYS",
    now_utc: datetime | None = None,
    cache_dir: Path | None = None,
) -> dict[str, object]:
    ctx = derive_date_context(
        daily_returns=daily_returns,
        exchange=exchange,
        now_utc=now_utc,
        cache_dir=cache_dir,
        clamp_to_market=True,
    )
    market_date = ctx.get("market_last_closed_session")
    return {
        "asof_date": str(ctx["asof_date"]),
        "analysis_window": dict(ctx["analysis_window"]),
        "mtd_window": dict(ctx["mtd_window"]),
        "asof_data_date": str(ctx["data_asof_date"]),
        "asof_market_date": str(market_date) if market_date is not None else "N/A",
    }
