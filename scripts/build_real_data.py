"""Build PBOR input CSVs from live market data."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml
import yfinance as yf

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "policy.yaml"
DEFAULT_OUT_DIR = PROJECT_ROOT / "data_real" / "market_real"
FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
BENCHMARK_SECTOR = "Equity"
PRICE_SOURCE = "yfinance"
FX_CONST_SOURCE = "CONST"
SOFR_SERIES_ID = "SOFR"

DEFAULT_PORTFOLIOS = [
    {
        "portfolio_id": "PF_REAL",
        "tickers": ["SPY", "QQQ", "IWM", "SCHD"],
        "benchmark_ticker": "SPY",
        "benchmark_id": "BM1",
    }
]


@dataclass
class PortfolioConfig:
    portfolio_id: str
    tickers: list[str]
    benchmark_ticker: str
    benchmark_id: str
    valid_tickers: list[str] = field(default_factory=list)
    initial_trade_date: pd.Timestamp | None = None


def _info(message: str) -> None:
    print(f"[INFO] {message}")


def _warn(message: str) -> None:
    print(f"[WARN] {message}")


def _previous_month_end(reference: pd.Timestamp | None = None) -> pd.Timestamp:
    today = (reference or pd.Timestamp.today()).normalize()
    month_start = today.replace(day=1)
    return (month_start - pd.Timedelta(days=1)).normalize()


def _normalize_tickers(raw_tickers: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in raw_tickers:
        ticker = str(raw).strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        ordered.append(ticker)
    return ordered


def _to_timestamp(value: str) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def _to_iso_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def _normalize_download_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_localize(None).dt.normalize()


def _extract_download_series(downloaded: pd.DataFrame, fields: tuple[str, ...]) -> pd.Series:
    if downloaded.empty:
        return pd.Series(dtype=float)

    for field in fields:
        if isinstance(downloaded.columns, pd.MultiIndex):
            if field not in downloaded.columns.get_level_values(0):
                continue
            extracted = downloaded[field]
            if isinstance(extracted, pd.DataFrame):
                if extracted.empty:
                    continue
                return extracted.iloc[:, 0]
            return extracted
        if field in downloaded.columns:
            extracted = downloaded[field]
            if isinstance(extracted, pd.DataFrame):
                if extracted.empty:
                    continue
                return extracted.iloc[:, 0]
            return extracted

    return pd.Series(dtype=float)


def _download_adjusted_close(ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    try:
        downloaded = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=(end + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
        )
    except Exception as exc:
        _warn(f"{ticker}: yfinance price download failed ({exc}).")
        return pd.DataFrame(columns=["date", "price"])

    series = _extract_download_series(downloaded, fields=("Adj Close", "Close"))
    if series.empty:
        _warn(f"{ticker}: no adjusted-close rows returned by yfinance.")
        return pd.DataFrame(columns=["date", "price"])

    frame = series.rename("price").reset_index()
    frame.columns = ["date", "price"]
    frame["date"] = _normalize_download_dates(frame["date"])
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame = frame.dropna(subset=["date", "price"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return frame.reset_index(drop=True)


def _download_dividends(ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    try:
        dividends = yf.Ticker(ticker).dividends
    except Exception as exc:
        _warn(f"{ticker}: dividend history lookup failed ({exc}).")
        return pd.DataFrame(columns=["date", "dividend_per_share"])

    if dividends is None or len(dividends) == 0:
        return pd.DataFrame(columns=["date", "dividend_per_share"])

    frame = dividends.rename("dividend_per_share").reset_index()
    frame.columns = ["date", "dividend_per_share"]
    frame["date"] = _normalize_download_dates(frame["date"])
    frame["dividend_per_share"] = pd.to_numeric(frame["dividend_per_share"], errors="coerce")
    frame = frame.dropna(subset=["date", "dividend_per_share"])
    frame = frame[(frame["date"] >= start) & (frame["date"] <= end)]
    return frame.sort_values("date").reset_index(drop=True)


def _load_ticker_info(ticker: str) -> dict[str, Any]:
    try:
        info = yf.Ticker(ticker).info
        if isinstance(info, dict):
            return info
    except Exception as exc:
        _warn(f"{ticker}: metadata lookup failed ({exc}); using fallbacks.")
    return {}


def _portfolio_configs_from_args(args: argparse.Namespace) -> list[PortfolioConfig]:
    if args.portfolios and args.tickers:
        raise ValueError("Use either --tickers or --portfolios, not both.")

    if args.portfolios:
        try:
            raw_configs = json.loads(args.portfolios)
        except json.JSONDecodeError as exc:
            raise ValueError(f"--portfolios must be valid JSON ({exc}).") from exc
    elif args.tickers:
        raw_configs = [
            {
                "portfolio_id": args.portfolio_id,
                "tickers": args.tickers,
                "benchmark_ticker": args.benchmark_ticker,
                "benchmark_id": args.benchmark_id,
            }
        ]
    else:
        raw_configs = DEFAULT_PORTFOLIOS

    if not isinstance(raw_configs, list) or not raw_configs:
        raise ValueError("At least one portfolio configuration is required.")

    configs: list[PortfolioConfig] = []
    seen_portfolios: set[str] = set()
    seen_benchmarks: dict[str, str] = {}

    for raw in raw_configs:
        if not isinstance(raw, dict):
            raise ValueError("Each portfolio configuration must be a JSON object.")
        portfolio_id = str(raw.get("portfolio_id", "")).strip()
        tickers = _normalize_tickers(list(raw.get("tickers") or []))
        benchmark_ticker = str(raw.get("benchmark_ticker", "")).strip().upper()
        benchmark_id = str(raw.get("benchmark_id", "")).strip().upper()

        if not portfolio_id:
            raise ValueError("Each portfolio configuration requires portfolio_id.")
        if portfolio_id in seen_portfolios:
            raise ValueError(f"Duplicate portfolio_id supplied: {portfolio_id}")
        if not tickers:
            raise ValueError(f"{portfolio_id}: at least one valid ticker is required.")
        if not benchmark_ticker:
            raise ValueError(f"{portfolio_id}: benchmark_ticker is required.")
        if not benchmark_id:
            raise ValueError(f"{portfolio_id}: benchmark_id is required.")

        prior_ticker = seen_benchmarks.get(benchmark_id)
        if prior_ticker and prior_ticker != benchmark_ticker:
            raise ValueError(
                f"Benchmark id {benchmark_id} was assigned to both {prior_ticker} and {benchmark_ticker}."
            )
        seen_benchmarks[benchmark_id] = benchmark_ticker
        seen_portfolios.add(portfolio_id)
        configs.append(
            PortfolioConfig(
                portfolio_id=portfolio_id,
                tickers=tickers,
                benchmark_ticker=benchmark_ticker,
                benchmark_id=benchmark_id,
            )
        )

    return configs


def _download_price_history(
    tickers: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[dict[str, pd.DataFrame], int]:
    raw_by_ticker: dict[str, pd.DataFrame] = {}
    missing_price_warnings = 0

    for ticker in tickers:
        frame = _download_adjusted_close(ticker=ticker, start=start, end=end)
        if frame.empty:
            _warn(f"{ticker}: skipped because no price data was available in the requested range.")
            missing_price_warnings += 1
            continue
        raw_by_ticker[ticker] = frame

    return raw_by_ticker, missing_price_warnings


def _first_common_trading_date(tickers: list[str], raw_by_ticker: dict[str, pd.DataFrame]) -> pd.Timestamp | None:
    common_dates: set[pd.Timestamp] | None = None
    for ticker in tickers:
        frame = raw_by_ticker.get(ticker)
        if frame is None or frame.empty:
            return None
        ticker_dates = set(pd.to_datetime(frame["date"]).tolist())
        common_dates = ticker_dates if common_dates is None else common_dates & ticker_dates
    if not common_dates:
        return None
    return min(common_dates)


def _build_prices(
    tickers: list[str],
    raw_by_ticker: dict[str, pd.DataFrame],
    start_date: pd.Timestamp,
    build_end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], int]:
    full_calendar = pd.date_range(start_date, build_end, freq="D")
    missing_price_warnings = 0
    price_rows: list[pd.DataFrame] = []
    filled_by_ticker: dict[str, pd.DataFrame] = {}

    for ticker in tickers:
        raw = raw_by_ticker[ticker]
        trimmed = raw[(raw["date"] >= start_date) & (raw["date"] <= build_end)].copy()
        series = trimmed.set_index("date")["price"].reindex(full_calendar).ffill(limit=3)
        remaining_missing = int(series.isna().sum())
        if remaining_missing > 0:
            _warn(
                f"{ticker}: {remaining_missing} price dates remained missing after 3-day forward fill and were dropped."
            )
            missing_price_warnings += 1
        cleaned = series.dropna().rename("price").reset_index()
        cleaned.columns = ["date", "price"]
        cleaned["security_id"] = ticker
        cleaned["price_currency"] = "USD"
        cleaned["source"] = PRICE_SOURCE
        filled_by_ticker[ticker] = cleaned[["date", "price"]].copy()
        price_rows.append(cleaned[["date", "security_id", "price", "price_currency", "source"]])

    prices = pd.concat(price_rows, ignore_index=True).sort_values(["date", "security_id"]).reset_index(drop=True)
    return prices, filled_by_ticker, missing_price_warnings


def _build_security_master(valid_tickers: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ticker in valid_tickers:
        info = _load_ticker_info(ticker)
        sector = str(info.get("sector") or BENCHMARK_SECTOR).strip() or BENCHMARK_SECTOR
        currency = str(info.get("currency") or "USD").strip().upper() or "USD"
        name = str(info.get("longName") or ticker).strip() or ticker
        rows.append(
            {
                "security_id": ticker,
                "ticker": ticker,
                "name": name,
                "asset_class": "Equity",
                "sector": sector,
                "currency": currency,
            }
        )

    security_master = pd.DataFrame(rows)
    return security_master[["security_id", "ticker", "name", "asset_class", "sector", "currency"]]


def _build_benchmark_returns(
    portfolios: list[PortfolioConfig],
    benchmark_price_cache: dict[str, pd.DataFrame],
    start_date: pd.Timestamp,
    build_end: pd.Timestamp,
) -> pd.DataFrame:
    full_calendar = pd.date_range(start_date, build_end, freq="D")
    rows: list[pd.DataFrame] = []

    for portfolio in portfolios:
        benchmark_raw = benchmark_price_cache[portfolio.benchmark_ticker]
        benchmark_prices = (
            benchmark_raw.set_index("date")["price"].reindex(full_calendar).ffill(limit=3).dropna().reset_index()
        )
        benchmark_prices.columns = ["date", "price"]
        benchmark_prices["return"] = benchmark_prices["price"].pct_change().fillna(0.0)
        benchmark_prices["benchmark_id"] = portfolio.benchmark_id
        benchmark_prices["sector"] = BENCHMARK_SECTOR
        rows.append(benchmark_prices[["date", "benchmark_id", "sector", "return"]])

    benchmark_returns = pd.concat(rows, ignore_index=True)
    benchmark_returns = benchmark_returns.drop_duplicates(subset=["date", "benchmark_id", "sector"], keep="last")
    return benchmark_returns.sort_values(["benchmark_id", "date", "sector"]).reset_index(drop=True)


def _build_benchmark_weights(
    benchmark_ids: list[str],
    price_dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for benchmark_id in benchmark_ids:
        rows.append(
            pd.DataFrame(
                {
                    "date": price_dates,
                    "benchmark_id": benchmark_id,
                    "sector": BENCHMARK_SECTOR,
                    "weight": 1.0,
                }
            )
        )

    weights = pd.concat(rows, ignore_index=True)
    checks = weights.groupby(["date", "benchmark_id"], as_index=False)["weight"].sum()
    if not (checks["weight"].round(12) == 1.0).all():
        raise ValueError("Benchmark weights do not sum to 1.0 for each date.")
    return weights.sort_values(["benchmark_id", "date", "sector"]).reset_index(drop=True)


def _portfolio_trading_dates(
    tickers: list[str],
    raw_by_ticker: dict[str, pd.DataFrame],
    initial_trade_date: pd.Timestamp,
    build_end: pd.Timestamp,
) -> pd.DatetimeIndex:
    trading_dates = sorted(
        {
            pd.Timestamp(date)
            for ticker in tickers
            for date in raw_by_ticker[ticker]["date"].tolist()
            if initial_trade_date <= pd.Timestamp(date) <= build_end
        }
    )
    return pd.DatetimeIndex(trading_dates)


def _build_transactions(
    portfolios: list[PortfolioConfig],
    price_by_ticker: dict[str, pd.DataFrame],
    raw_by_ticker: dict[str, pd.DataFrame],
    build_end: pd.Timestamp,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dividend_cache: dict[str, pd.DataFrame] = {}

    for portfolio in portfolios:
        assert portfolio.initial_trade_date is not None
        initial_purchase_total = 0.0

        for ticker in portfolio.valid_tickers:
            frame = price_by_ticker[ticker]
            matched = frame[pd.to_datetime(frame["date"]) == portfolio.initial_trade_date]
            if matched.empty:
                _warn(
                    f"{portfolio.portfolio_id}/{ticker}: skipped initial buy because no price existed on "
                    f"{portfolio.initial_trade_date.strftime('%Y-%m-%d')}."
                )
                continue
            price = float(matched.iloc[0]["price"])
            initial_purchase_total += 100.0 * price
            rows.append(
                {
                    "date": portfolio.initial_trade_date,
                    "portfolio_id": portfolio.portfolio_id,
                    "security_id": ticker,
                    "txn_type": "BUY",
                    "quantity": 100.0,
                    "price": price,
                    "cash_amount": -(100.0 * price),
                    "fees": 0.0,
                }
            )

        if initial_purchase_total > 0.0:
            rows.append(
                {
                    "date": portfolio.initial_trade_date,
                    "portfolio_id": portfolio.portfolio_id,
                    "security_id": None,
                    "txn_type": "CONTRIB",
                    "quantity": 0.0,
                    "price": 0.0,
                    "cash_amount": initial_purchase_total,
                    "fees": 0.0,
                }
            )

        trading_dates = _portfolio_trading_dates(
            tickers=portfolio.valid_tickers,
            raw_by_ticker=raw_by_ticker,
            initial_trade_date=portfolio.initial_trade_date,
            build_end=build_end,
        )
        trading_frame = pd.DataFrame({"date": trading_dates})
        trading_frame["month"] = trading_frame["date"].dt.to_period("M")
        initial_month = portfolio.initial_trade_date.to_period("M")
        first_trading_days = (
            trading_frame[trading_frame["month"] > initial_month]
            .groupby("month", as_index=False)["date"]
            .min()
            .sort_values("date")
        )
        for _, row in first_trading_days.iterrows():
            rows.append(
                {
                    "date": pd.Timestamp(row["date"]),
                    "portfolio_id": portfolio.portfolio_id,
                    "security_id": None,
                    "txn_type": "CONTRIB",
                    "quantity": 0.0,
                    "price": 0.0,
                    "cash_amount": 10000.0,
                    "fees": 0.0,
                }
            )

        for ticker in portfolio.valid_tickers:
            if ticker not in dividend_cache:
                dividend_cache[ticker] = _download_dividends(
                    ticker=ticker,
                    start=portfolio.initial_trade_date,
                    end=build_end,
                )
            dividends = dividend_cache[ticker]
            if dividends.empty:
                continue
            dividends = dividends[dividends["date"] >= portfolio.initial_trade_date].copy()
            for _, row in dividends.iterrows():
                amount_per_share = float(row["dividend_per_share"])
                rows.append(
                    {
                        "date": pd.Timestamp(row["date"]),
                        "portfolio_id": portfolio.portfolio_id,
                        "security_id": ticker,
                        "txn_type": "DIV",
                        "quantity": 100.0,
                        "price": amount_per_share,
                        "cash_amount": 100.0 * amount_per_share,
                        "fees": 0.0,
                    }
                )

    transactions = pd.DataFrame(rows)
    if transactions.empty:
        raise ValueError("No transactions were generated from the live data build.")

    transactions = transactions.sort_values(["date", "portfolio_id", "txn_type", "security_id"], na_position="last")
    return transactions.reset_index(drop=True)[
        ["date", "portfolio_id", "security_id", "txn_type", "quantity", "price", "cash_amount", "fees"]
    ]


def _build_holdings_reported(
    portfolios: list[PortfolioConfig],
    price_by_ticker: dict[str, pd.DataFrame],
    asof_date: pd.Timestamp,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for portfolio in portfolios:
        for ticker in portfolio.valid_tickers:
            frame = price_by_ticker[ticker]
            matched = frame[pd.to_datetime(frame["date"]) == asof_date]
            if matched.empty:
                _warn(
                    f"{portfolio.portfolio_id}/{ticker}: omitted from holdings_reported because no as-of price "
                    f"existed on {asof_date.strftime('%Y-%m-%d')}."
                )
                continue
            price = float(matched.iloc[0]["price"])
            rows.append(
                {
                    "date": asof_date,
                    "portfolio_id": portfolio.portfolio_id,
                    "security_id": ticker,
                    "quantity": 100.0,
                    "market_value_base": 100.0 * price,
                    "cash_balance_base": 0.0,
                }
            )

    holdings = pd.DataFrame(rows)
    if holdings.empty:
        raise ValueError("No holdings_reported rows could be generated for the as-of date.")
    return holdings[["date", "portfolio_id", "security_id", "quantity", "market_value_base", "cash_balance_base"]]


def _download_fx_pair_history(pair_ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    history = _download_adjusted_close(ticker=pair_ticker, start=start, end=end)
    if history.empty:
        raise ValueError(f"FX pair {pair_ticker} returned no price data.")
    history = history.rename(columns={"price": "rate"})
    return history[["date", "rate"]]


def _build_fx_rates(
    security_master: pd.DataFrame,
    price_dates: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
    base_currency: str,
) -> pd.DataFrame:
    unique_dates = pd.to_datetime(price_dates).drop_duplicates().sort_values()
    rows: list[pd.DataFrame] = [
        pd.DataFrame(
            {
                "date": unique_dates,
                "ccy_pair": f"{base_currency}{base_currency}",
                "rate": 1.0,
                "source": FX_CONST_SOURCE,
            }
        )
    ]

    non_base = sorted(
        {
            str(currency).upper()
            for currency in security_master["currency"].dropna().tolist()
            if str(currency).upper() != base_currency.upper()
        }
    )
    for currency in non_base:
        pair = f"{currency}{base_currency.upper()}"
        pair_ticker = f"{pair}=X"
        try:
            fx_history = _download_fx_pair_history(pair_ticker=pair_ticker, start=start, end=end)
        except Exception as exc:
            _warn(f"{pair_ticker}: FX download failed ({exc}).")
            continue

        aligned = fx_history.set_index("date")["rate"].reindex(unique_dates).ffill(limit=3).dropna().reset_index()
        aligned.columns = ["date", "rate"]
        aligned["ccy_pair"] = pair
        aligned["source"] = PRICE_SOURCE
        rows.append(aligned[["date", "ccy_pair", "rate", "source"]])

    fx_rates = pd.concat(rows, ignore_index=True)
    return fx_rates.sort_values(["date", "ccy_pair"]).reset_index(drop=True)[["date", "ccy_pair", "rate", "source"]]


def _get_policy_sofr_rate(policy: dict[str, Any]) -> float:
    rates = policy.get("cash_return_annual_rates", {})
    if not isinstance(rates, dict):
        return 0.0
    try:
        return float(rates.get("SOFR", 0.0))
    except (TypeError, ValueError):
        return 0.0


def _fetch_latest_sofr(api_key: str) -> tuple[pd.Timestamp, float]:
    response = requests.get(
        FRED_OBSERVATIONS_URL,
        params={
            "series_id": SOFR_SERIES_ID,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 5,
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    observations = payload.get("observations", [])
    for row in observations:
        value = row.get("value")
        date_value = row.get("date")
        if value in {None, ".", ""} or not date_value:
            continue
        return pd.Timestamp(date_value).normalize(), float(value) / 100.0
    raise ValueError("FRED returned no non-null SOFR observations in the latest sample.")


def _fetch_sofr_history(api_key: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    response = requests.get(
        FRED_OBSERVATIONS_URL,
        params={
            "series_id": SOFR_SERIES_ID,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "asc",
            "observation_start": start.strftime("%Y-%m-%d"),
            "observation_end": end.strftime("%Y-%m-%d"),
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    observations = payload.get("observations", [])
    rows: list[dict[str, object]] = []
    for row in observations:
        value = row.get("value")
        date_value = row.get("date")
        if value in {None, ".", ""} or not date_value:
            continue
        rows.append({"date": pd.Timestamp(date_value).normalize(), "rate_annual": float(value) / 100.0})
    return pd.DataFrame(rows, columns=["date", "rate_annual"])


def _build_sofr_outputs(
    policy_path: Path,
    price_dates: pd.Series,
    portfolios: list[PortfolioConfig],
    fred_api_key: str | None,
    dry_run: bool,
) -> tuple[pd.DataFrame, float]:
    policy = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    current_rate = _get_policy_sofr_rate(policy)
    used_rate = current_rate
    latest_date: pd.Timestamp | None = None
    latest_rate: float | None = None
    sofr_history = pd.DataFrame(columns=["date", "rate_annual"])

    if fred_api_key:
        try:
            latest_date, latest_rate = _fetch_latest_sofr(api_key=fred_api_key)
            sofr_history = _fetch_sofr_history(
                api_key=fred_api_key,
                start=pd.to_datetime(price_dates).min().normalize(),
                end=pd.to_datetime(price_dates).max().normalize(),
            )
            used_rate = latest_rate
        except Exception as exc:
            _warn(f"FRED SOFR refresh failed ({exc}); leaving SOFR rate unchanged.")
    else:
        _warn("No --fred-api-key supplied; leaving SOFR rate unchanged.")

    if sofr_history.empty:
        fallback_date = latest_date if latest_date is not None else pd.to_datetime(price_dates).max().normalize()
        sofr_history = pd.DataFrame([{"date": fallback_date, "rate_annual": used_rate}])

    if "cash_return_annual_rates" not in policy or not isinstance(policy["cash_return_annual_rates"], dict):
        policy["cash_return_annual_rates"] = {}

    desired_map = {portfolio.portfolio_id: portfolio.benchmark_id for portfolio in portfolios}
    updated_policy = False

    if latest_rate is not None:
        policy["cash_return_annual_rates"]["SOFR"] = float(latest_rate)
        updated_policy = True

    if portfolios:
        default_benchmark = portfolios[0].benchmark_id
        if str(policy.get("benchmark_id_default", "")).strip().upper() != default_benchmark:
            policy["benchmark_id_default"] = default_benchmark
            updated_policy = True

    existing_map = policy.get("portfolio_benchmark_map", {})
    if not isinstance(existing_map, dict) or existing_map != desired_map:
        policy["portfolio_benchmark_map"] = desired_map
        updated_policy = True

    if updated_policy:
        if dry_run:
            _info("Dry run: would update policy.yaml.")
        else:
            policy_path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
            _info("Updated policy.yaml.")

    return sofr_history, float(used_rate)


def _format_output_dates(frame: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
    formatted = frame.copy()
    for column in date_columns:
        formatted[column] = _to_iso_date(formatted[column])
    return formatted


def _write_csv(frame: pd.DataFrame, path: Path, dry_run: bool) -> None:
    if dry_run:
        print(f"  - would write {path} ({len(frame):,} rows)")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _print_summary_table(
    row_counts: dict[str, int],
    date_start: str,
    date_end: str,
    sofr_rate_used: float,
    missing_price_warnings: int,
    dry_run: bool,
) -> None:
    title = "Real Data Build"
    if dry_run:
        title += " (Dry Run)"

    col1 = 22
    col2 = 17
    width = col1 + col2 + 3

    def separator(char: str = "-") -> str:
        return f"+{char * col1}+{char * col2}+"

    def row(label: str, value: str) -> str:
        return f"|{label:<{col1}}|{value:<{col2}}|"

    print()
    print("+" + "-" * width + "+")
    print(f"|{title:^{width}}|")
    print(separator("="))
    print(row("File", "Rows Written"))
    print(separator("="))
    for file_name, count in row_counts.items():
        print(row(file_name, f"{count:,}"))
    print(separator("="))
    print(row("Date range", f"{date_start} to"))
    print(row("", date_end))
    print(row("SOFR rate used", f"{sofr_rate_used:.4f}"))
    print(row("Missing price warns", str(missing_price_warnings)))
    print(separator("-"))


def _default_build_command(args: argparse.Namespace) -> str:
    if args.portfolios:
        portfolio_json = json.dumps(json.loads(args.portfolios))
        return (
            f"python scripts/build_real_data.py --portfolios '{portfolio_json}' "
            f"--start {args.start} --end {args.end} --out-dir {args.out_dir}"
            + (f" --fred-api-key {args.fred_api_key}" if args.fred_api_key else "")
            + (" --dry-run" if args.dry_run else "")
        )

    if args.tickers:
        tickers = " ".join(_normalize_tickers(args.tickers))
        return (
            f"python scripts/build_real_data.py --tickers {tickers} --start {args.start} --end {args.end} "
            f"--portfolio-id {args.portfolio_id} --benchmark-id {args.benchmark_id} "
            f"--benchmark-ticker {str(args.benchmark_ticker).strip().upper()} --out-dir {args.out_dir}"
            + (f" --fred-api-key {args.fred_api_key}" if args.fred_api_key else "")
            + (" --dry-run" if args.dry_run else "")
        )

    return (
        f"python scripts/build_real_data.py --start {args.start} --end {args.end} --out-dir {args.out_dir}"
        + (f" --fred-api-key {args.fred_api_key}" if args.fred_api_key else "")
        + (" --dry-run" if args.dry_run else "")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build real-market PBOR inputs.")
    parser.add_argument("--tickers", nargs="+", default=None, help="Portfolio tickers to download from yfinance.")
    parser.add_argument("--start", default="2024-01-01", help="Start date in YYYY-MM-DD format.")
    parser.add_argument(
        "--end",
        default=_previous_month_end().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format. Defaults to the previous month-end.",
    )
    parser.add_argument("--portfolio-id", default="PF_REAL", help="Portfolio identifier for single-portfolio mode.")
    parser.add_argument("--benchmark-id", default="BM1", help="Benchmark identifier for single-portfolio mode.")
    parser.add_argument("--benchmark-ticker", default="SPY", help="Benchmark market ticker for single-portfolio mode.")
    parser.add_argument(
        "--portfolios",
        default=None,
        help="Optional JSON array of portfolio configs for multi-portfolio builds.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Output directory for PBOR input CSVs.",
    )
    parser.add_argument("--fred-api-key", default=None, help="Optional FRED API key for live SOFR refresh.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be written without writing files.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start = _to_timestamp(args.start)
    requested_end = _to_timestamp(args.end)
    if requested_end < start:
        raise ValueError("--end must be on or after --start.")

    portfolios = _portfolio_configs_from_args(args)
    out_dir = Path(args.out_dir)
    _info(f"Building real PBOR data for {len(portfolios)} portfolio configuration(s) into {out_dir}.")

    all_tickers = sorted({ticker for portfolio in portfolios for ticker in portfolio.tickers})
    benchmark_tickers = sorted({portfolio.benchmark_ticker for portfolio in portfolios})
    raw_prices, missing_price_warnings = _download_price_history(tickers=all_tickers, start=start, end=requested_end)
    benchmark_price_cache, benchmark_missing_warnings = _download_price_history(
        tickers=benchmark_tickers,
        start=start,
        end=requested_end,
    )
    missing_price_warnings += benchmark_missing_warnings

    active_portfolios: list[PortfolioConfig] = []
    for portfolio in portfolios:
        portfolio.valid_tickers = [ticker for ticker in portfolio.tickers if ticker in raw_prices]
        if not portfolio.valid_tickers:
            _warn(f"{portfolio.portfolio_id}: skipped because no portfolio tickers produced price data.")
            continue
        if portfolio.benchmark_ticker not in benchmark_price_cache:
            _warn(
                f"{portfolio.portfolio_id}: skipped because benchmark ticker {portfolio.benchmark_ticker} "
                "returned no price data."
            )
            continue

        initial_trade_date = _first_common_trading_date(portfolio.valid_tickers, raw_prices)
        if initial_trade_date is None:
            _warn(f"{portfolio.portfolio_id}: skipped because no common trading date was found across its tickers.")
            continue
        portfolio.initial_trade_date = initial_trade_date
        active_portfolios.append(portfolio)

    if not active_portfolios:
        raise ValueError("No valid portfolios remained after market-data validation; nothing to build.")

    earliest_trade_date = min(
        portfolio.initial_trade_date for portfolio in active_portfolios if portfolio.initial_trade_date is not None
    )
    used_tickers = sorted({ticker for portfolio in active_portfolios for ticker in portfolio.valid_tickers})
    used_benchmark_tickers = sorted({portfolio.benchmark_ticker for portfolio in active_portfolios})
    latest_available_dates = [
        pd.to_datetime(raw_prices[ticker]["date"]).max().normalize() for ticker in used_tickers
    ] + [
        pd.to_datetime(benchmark_price_cache[ticker]["date"]).max().normalize() for ticker in used_benchmark_tickers
    ]
    build_end = min(latest_available_dates)

    for portfolio in active_portfolios:
        assert portfolio.initial_trade_date is not None
        if portfolio.initial_trade_date > start:
            _info(
                f"{portfolio.portfolio_id}: initial buy date shifted from {start.strftime('%Y-%m-%d')} to "
                f"{portfolio.initial_trade_date.strftime('%Y-%m-%d')}."
            )

    if build_end < requested_end:
        _warn(
            f"Requested end date {requested_end.strftime('%Y-%m-%d')} exceeds the latest common market data date "
            f"{build_end.strftime('%Y-%m-%d')}; clamping the build to {build_end.strftime('%Y-%m-%d')}."
        )

    prices, price_by_ticker, ffill_warnings = _build_prices(
        tickers=used_tickers,
        raw_by_ticker=raw_prices,
        start_date=earliest_trade_date,
        build_end=build_end,
    )
    missing_price_warnings += ffill_warnings

    security_master = _build_security_master(valid_tickers=used_tickers)
    price_currency_map = security_master.set_index("security_id")["currency"].to_dict()
    prices["price_currency"] = prices["security_id"].map(price_currency_map).fillna("USD")

    benchmark_returns = _build_benchmark_returns(
        portfolios=active_portfolios,
        benchmark_price_cache=benchmark_price_cache,
        start_date=earliest_trade_date,
        build_end=build_end,
    )
    full_calendar = pd.date_range(earliest_trade_date, build_end, freq="D")
    benchmark_weights = _build_benchmark_weights(
        benchmark_ids=[portfolio.benchmark_id for portfolio in active_portfolios],
        price_dates=full_calendar,
    )
    transactions = _build_transactions(
        portfolios=active_portfolios,
        price_by_ticker=price_by_ticker,
        raw_by_ticker=raw_prices,
        build_end=build_end,
    )
    holdings_reported = _build_holdings_reported(
        portfolios=active_portfolios,
        price_by_ticker=price_by_ticker,
        asof_date=build_end,
    )
    fx_rates = _build_fx_rates(
        security_master=security_master,
        price_dates=prices["date"],
        start=earliest_trade_date,
        end=build_end,
        base_currency="USD",
    )
    sofr_rates, sofr_rate_used = _build_sofr_outputs(
        policy_path=POLICY_PATH,
        price_dates=prices["date"],
        portfolios=active_portfolios,
        fred_api_key=args.fred_api_key,
        dry_run=args.dry_run,
    )

    prices_out = _format_output_dates(prices, ["date"])
    benchmark_returns_out = _format_output_dates(benchmark_returns, ["date"])
    benchmark_weights_out = _format_output_dates(benchmark_weights, ["date"])
    transactions_out = _format_output_dates(transactions, ["date"])
    holdings_reported_out = _format_output_dates(holdings_reported, ["date"])
    fx_rates_out = _format_output_dates(fx_rates, ["date"])
    sofr_rates_out = _format_output_dates(sofr_rates, ["date"])

    row_counts = {
        "prices.csv": len(prices_out),
        "benchmark_returns.csv": len(benchmark_returns_out),
        "benchmark_weights.csv": len(benchmark_weights_out),
        "security_master.csv": len(security_master),
        "transactions.csv": len(transactions_out),
        "holdings_reported.csv": len(holdings_reported_out),
        "fx_rates.csv": len(fx_rates_out),
        "sofr_rates.csv": len(sofr_rates_out),
    }

    if args.dry_run:
        print()
        _info("Dry run enabled.")
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        _info(f"Writing PBOR input files to {out_dir}.")

    _write_csv(prices_out, out_dir / "prices.csv", dry_run=args.dry_run)
    _write_csv(benchmark_returns_out, out_dir / "benchmark_returns.csv", dry_run=args.dry_run)
    _write_csv(benchmark_weights_out, out_dir / "benchmark_weights.csv", dry_run=args.dry_run)
    _write_csv(security_master, out_dir / "security_master.csv", dry_run=args.dry_run)
    _write_csv(transactions_out, out_dir / "transactions.csv", dry_run=args.dry_run)
    _write_csv(holdings_reported_out, out_dir / "holdings_reported.csv", dry_run=args.dry_run)
    _write_csv(fx_rates_out, out_dir / "fx_rates.csv", dry_run=args.dry_run)
    _write_csv(sofr_rates_out, out_dir / "sofr_rates.csv", dry_run=args.dry_run)

    _print_summary_table(
        row_counts=row_counts,
        date_start=prices_out["date"].min(),
        date_end=prices_out["date"].max(),
        sofr_rate_used=sofr_rate_used,
        missing_price_warnings=missing_price_warnings,
        dry_run=args.dry_run,
    )

    print()
    print("Run month-end with:")
    print(f"  {_default_build_command(args=args)}")
    print(f"  python -m src.run_month_end --asof {build_end.strftime('%Y-%m-%d')} --data-dir {args.out_dir}")
    print()
    print("Use the same folder for --out-dir and --data-dir.")


if __name__ == "__main__":
    main()
