from __future__ import annotations

from datetime import date, datetime, time, timezone
from pathlib import Path
import json
import re
import urllib.error
import urllib.request
import warnings
from zoneinfo import ZoneInfo

import pandas as pd

_NYSE_CALENDAR_URL = "https://www.nyse.com/markets/hours-calendars"
_SCRAPE_WARNING_EMITTED = False
_WEEKDAY_WARNING_EMITTED = False
_FAILED_HOLIDAY_YEARS: set[int] = set()


def get_now_et(now_utc: datetime | None = None) -> datetime:
    current_utc = now_utc or datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=timezone.utc)
    return current_utc.astimezone(ZoneInfo("America/New_York"))


def _normalize_sessions(values: object) -> pd.DatetimeIndex:
    idx = pd.DatetimeIndex(pd.to_datetime(values))
    if idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    return pd.DatetimeIndex(idx.normalize().sort_values().unique())


def _sessions_from_exchange_calendars(
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    exchange: str,
) -> pd.DatetimeIndex | None:
    try:
        import exchange_calendars as xcals  # type: ignore

        cal = xcals.get_calendar(exchange)
        sessions = cal.sessions_in_range(start_ts, end_ts)
        return _normalize_sessions(sessions)
    except Exception:
        return None


def _sessions_from_pandas_market_calendars(
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    exchange: str,
) -> pd.DatetimeIndex | None:
    try:
        import pandas_market_calendars as mcal  # type: ignore

        market_name = "NYSE" if exchange.upper() == "XNYS" else exchange
        cal = mcal.get_calendar(market_name)
        schedule = cal.schedule(start_date=start_ts.date(), end_date=end_ts.date())
        return _normalize_sessions(schedule.index)
    except Exception:
        return None


def _holiday_cache_path(cache_dir: Path | None, exchange: str, year: int) -> Path | None:
    if cache_dir is None:
        return None
    safe_exchange = exchange.lower()
    return cache_dir / f"{safe_exchange}_{year}.json"


def _load_cached_holidays(cache_path: Path | None, year: int) -> set[date]:
    if cache_path is None or not cache_path.exists():
        return set()
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        if int(payload.get("year", -1)) != year:
            return set()
        out: set[date] = set()
        for value in payload.get("closed_dates", []):
            out.add(pd.Timestamp(value).date())
        return out
    except Exception:
        return set()


def _save_cached_holidays(cache_path: Path | None, year: int, closed_dates: set[date]) -> None:
    if cache_path is None:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": "NYSE_WEB_FALLBACK",
        "year": int(year),
        "closed_dates": sorted(str(d) for d in closed_dates),
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _extract_dates_from_text(blob: str, year: int) -> set[date]:
    month_pattern = (
        r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+\d{1,2},\s+\d{4}"
    )
    short_month_pattern = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}"
    full_matches = re.findall(month_pattern, blob, flags=re.IGNORECASE)
    full_matches += re.findall(short_month_pattern, blob, flags=re.IGNORECASE)

    out: set[date] = set()
    for raw in full_matches:
        text = str(raw).strip()
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                parsed = datetime.strptime(text, fmt).date()
                if parsed.year == year and parsed.weekday() < 5:
                    out.add(parsed)
                break
            except ValueError:
                continue
    return out


def _fetch_nyse_closed_dates(year: int, cache_dir: Path | None = None) -> set[date]:
    if year in _FAILED_HOLIDAY_YEARS:
        return set()
    cache_path = _holiday_cache_path(cache_dir=cache_dir, exchange="nyse", year=year)
    cached = _load_cached_holidays(cache_path=cache_path, year=year)
    if cached:
        return cached

    req = urllib.request.Request(
        _NYSE_CALENDAR_URL,
        headers={"User-Agent": "PBOR-Lite/1.0 calendar-fallback"},
    )
    try:
        with urllib.request.urlopen(req, timeout=4) as resp:  # nosec B310
            html = resp.read().decode("utf-8", errors="ignore")
    except Exception:
        _FAILED_HOLIDAY_YEARS.add(year)
        raise
    closed_dates = _extract_dates_from_text(html, year=year)
    if closed_dates:
        _save_cached_holidays(cache_path=cache_path, year=year, closed_dates=closed_dates)
    return closed_dates


def _sessions_from_scraped_nyse(
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    exchange: str,
    cache_dir: Path | None,
) -> pd.DatetimeIndex | None:
    if exchange.upper() != "XNYS":
        return None
    holidays: set[date] = set()
    fetched_any = False
    for year in range(start_ts.year, end_ts.year + 1):
        try:
            holidays.update(_fetch_nyse_closed_dates(year=year, cache_dir=cache_dir))
            fetched_any = True
        except (urllib.error.URLError, TimeoutError, ValueError):
            continue
        except Exception:
            continue
    if not fetched_any:
        return None
    global _SCRAPE_WARNING_EMITTED
    if not _SCRAPE_WARNING_EMITTED:
        warnings.warn("USING SCRAPED CALENDAR FALLBACK", RuntimeWarning, stacklevel=2)
        _SCRAPE_WARNING_EMITTED = True
    business_days = pd.date_range(start_ts, end_ts, freq="B")
    sessions = [d for d in business_days if d.date() not in holidays]
    return _normalize_sessions(sessions)


def get_trading_sessions(
    start_date: date | datetime | str,
    end_date: date | datetime | str,
    exchange: str = "XNYS",
    cache_dir: Path | None = None,
) -> pd.DatetimeIndex:
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    if end_ts < start_ts:
        return pd.DatetimeIndex([])

    sessions = _sessions_from_exchange_calendars(start_ts=start_ts, end_ts=end_ts, exchange=exchange)
    if sessions is not None:
        return sessions
    sessions = _sessions_from_pandas_market_calendars(start_ts=start_ts, end_ts=end_ts, exchange=exchange)
    if sessions is not None:
        return sessions
    sessions = _sessions_from_scraped_nyse(start_ts=start_ts, end_ts=end_ts, exchange=exchange, cache_dir=cache_dir)
    if sessions is not None and len(sessions) > 0:
        return sessions

    global _WEEKDAY_WARNING_EMITTED
    if not _WEEKDAY_WARNING_EMITTED:
        warnings.warn(
            "Falling back to weekday-only trading calendar (Mon-Fri); exchange calendars unavailable.",
            RuntimeWarning,
            stacklevel=2,
        )
        _WEEKDAY_WARNING_EMITTED = True
    return pd.DatetimeIndex(pd.date_range(start_ts, end_ts, freq="B"))


def last_trading_session_on_or_before(
    value: date | datetime | str,
    exchange: str = "XNYS",
    cache_dir: Path | None = None,
) -> date:
    target = pd.Timestamp(value).normalize()
    lookback_days = 14
    for _ in range(8):
        sessions = get_trading_sessions(
            start_date=target - pd.Timedelta(days=lookback_days),
            end_date=target,
            exchange=exchange,
            cache_dir=cache_dir,
        )
        eligible = sessions[sessions <= target]
        if len(eligible) > 0:
            return eligible[-1].date()
        lookback_days *= 2
    return target.date()


def market_last_closed_session(
    now_utc: datetime | None = None,
    exchange: str = "XNYS",
    cache_dir: Path | None = None,
    close_cutoff_et: time = time(16, 10),
) -> date | None:
    now_et = get_now_et(now_utc=now_utc)
    today = now_et.date()
    lookback_start = pd.Timestamp(today) - pd.Timedelta(days=30)
    sessions = get_trading_sessions(
        start_date=lookback_start,
        end_date=today,
        exchange=exchange,
        cache_dir=cache_dir,
    )
    if len(sessions) == 0:
        return None
    session_dates = [d.date() for d in sessions]
    if today in session_dates:
        if now_et.time() < close_cutoff_et:
            prior = [d for d in session_dates if d < today]
            return prior[-1] if prior else None
        return today
    eligible = [d for d in session_dates if d <= today]
    return eligible[-1] if eligible else None


def derive_date_context(
    daily_returns: pd.DataFrame,
    exchange: str = "XNYS",
    now_utc: datetime | None = None,
    cache_dir: Path | None = None,
    clamp_to_market: bool = True,
) -> dict[str, object]:
    current_utc = now_utc or datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=timezone.utc)
    current_et = get_now_et(now_utc=current_utc)

    if daily_returns.empty or "date" not in daily_returns.columns:
        return {
            "data_asof_date": "N/A",
            "asof_date": "N/A",
            "generated_at_utc": current_utc.isoformat(),
            "generated_at_et": current_et.isoformat(),
            "market_last_closed_session": None,
            "analysis_window": {"start": "N/A", "end": "N/A", "obs_rows": 0, "trading_days": 0},
            "mtd_window": {"start": "N/A", "end": "N/A", "obs_rows": 0, "trading_days": 0},
        }

    dates = pd.to_datetime(daily_returns["date"], errors="coerce").dt.date.dropna()
    if dates.empty:
        return {
            "data_asof_date": "N/A",
            "asof_date": "N/A",
            "generated_at_utc": current_utc.isoformat(),
            "generated_at_et": current_et.isoformat(),
            "market_last_closed_session": None,
            "analysis_window": {"start": "N/A", "end": "N/A", "obs_rows": 0, "trading_days": 0},
            "mtd_window": {"start": "N/A", "end": "N/A", "obs_rows": 0, "trading_days": 0},
        }

    analysis_start = min(dates)
    data_asof = max(dates)
    market_last = market_last_closed_session(
        now_utc=current_utc,
        exchange=exchange,
        cache_dir=cache_dir,
    )
    if clamp_to_market and market_last is not None:
        asof = min(data_asof, market_last)
    else:
        asof = data_asof

    analysis_sessions = get_trading_sessions(
        start_date=analysis_start,
        end_date=asof,
        exchange=exchange,
        cache_dir=cache_dir,
    )
    analysis_obs_rows = int(((dates >= analysis_start) & (dates <= asof)).sum())

    month_first = date(asof.year, asof.month, 1)
    mtd_sessions = get_trading_sessions(
        start_date=month_first,
        end_date=asof,
        exchange=exchange,
        cache_dir=cache_dir,
    )
    mtd_start = mtd_sessions[0].date() if len(mtd_sessions) > 0 else asof
    mtd_obs_rows = int(((dates >= mtd_start) & (dates <= asof)).sum())

    return {
        "data_asof_date": str(data_asof),
        "asof_date": str(asof),
        "generated_at_utc": current_utc.isoformat(),
        "generated_at_et": current_et.isoformat(),
        "market_last_closed_session": str(market_last) if market_last is not None else None,
        "analysis_window": {
            "start": str(analysis_start),
            "end": str(asof),
            "obs_rows": analysis_obs_rows,
            "trading_days": int(len(analysis_sessions)),
        },
        "mtd_window": {
            "start": str(mtd_start),
            "end": str(asof),
            "obs_rows": mtd_obs_rows,
            "trading_days": int(len(mtd_sessions)),
        },
    }
