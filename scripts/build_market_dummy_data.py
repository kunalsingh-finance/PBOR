from __future__ import annotations

import argparse
import io
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import urllib.request

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SecuritySpec:
    security_id: str
    ticker: str
    name: str
    sector: str
    target_weight: float


PORTFOLIO_UNIVERSE: list[SecuritySpec] = [
    SecuritySpec("SEC_AAPL", "AAPL.US", "Apple Inc", "Tech", 0.12),
    SecuritySpec("SEC_MSFT", "MSFT.US", "Microsoft Corp", "Tech", 0.11),
    SecuritySpec("SEC_NVDA", "NVDA.US", "NVIDIA Corp", "Tech", 0.10),
    SecuritySpec("SEC_XOM", "XOM.US", "Exxon Mobil", "Energy", 0.09),
    SecuritySpec("SEC_CVX", "CVX.US", "Chevron Corp", "Energy", 0.08),
    SecuritySpec("SEC_JPM", "JPM.US", "JPMorgan Chase", "Financials", 0.08),
    SecuritySpec("SEC_BAC", "BAC.US", "Bank of America", "Financials", 0.07),
    SecuritySpec("SEC_UNH", "UNH.US", "UnitedHealth", "HealthCare", 0.08),
    SecuritySpec("SEC_JNJ", "JNJ.US", "Johnson & Johnson", "HealthCare", 0.07),
    SecuritySpec("SEC_CAT", "CAT.US", "Caterpillar", "Industrials", 0.10),
    SecuritySpec("SEC_HON", "HON.US", "Honeywell", "Industrials", 0.10),
]

SECTOR_BENCHMARK_ETF: dict[str, str] = {
    "Tech": "XLK.US",
    "Energy": "XLE.US",
    "Financials": "XLF.US",
    "HealthCare": "XLV.US",
    "Industrials": "XLI.US",
}

STOOQ_BASE_URL = "https://stooq.com/q/d/l/?s={symbol}&i=d"


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if "." not in raw:
        return f"{raw}.US"
    return raw


def _fetch_stooq_close(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    normalized = _normalize_symbol(symbol)
    url = STOOQ_BASE_URL.format(symbol=normalized.lower())
    req = urllib.request.Request(url, headers={"User-Agent": "PBOR-Lite/1.0 market-dummy-builder"})
    with urllib.request.urlopen(req, timeout=10) as response:  # nosec B310
        payload = response.read().decode("utf-8", errors="ignore")
    frame = pd.read_csv(io.StringIO(payload))
    if frame.empty or "Date" not in frame.columns or "Close" not in frame.columns:
        raise ValueError(f"No usable rows for {normalized}")
    frame = frame.rename(columns={"Date": "date", "Close": "close"})[["date", "close"]]
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["date", "close"]).sort_values("date")
    frame = frame[(frame["date"] >= start) & (frame["date"] <= end)]
    if frame.empty:
        raise ValueError(f"No rows in requested range for {normalized}")
    return frame.reset_index(drop=True)


def _build_daily_price_series(
    raw: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    calendar = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})
    merged = calendar.merge(raw, on="date", how="left")
    merged["close"] = merged["close"].ffill().bfill()
    return merged


def _synthetic_price_series(symbol: str, start: pd.Timestamp, end: pd.Timestamp, base_price: float) -> pd.DataFrame:
    dates = pd.date_range(start, end, freq="D")
    rng = np.random.default_rng(abs(hash(symbol)) % (2**32))
    shock = rng.normal(loc=0.0004, scale=0.012, size=len(dates))
    close = base_price * np.cumprod(1.0 + shock)
    return pd.DataFrame({"date": dates, "close": close})


def _sector_weights_from_universe(universe: list[SecuritySpec]) -> dict[str, float]:
    sector_sum: dict[str, float] = {}
    for spec in universe:
        sector_sum[spec.sector] = sector_sum.get(spec.sector, 0.0) + float(spec.target_weight)
    total = sum(sector_sum.values())
    if total <= 0:
        return {}
    return {k: v / total for k, v in sector_sum.items()}


def build_market_dummy_dataset(
    out_dir: Path,
    start_date: str,
    end_date: str,
    portfolio_id: str,
    benchmark_id: str,
    initial_capital: float,
    topup_amount: float,
) -> dict[str, object]:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")
    universe = PORTFOLIO_UNIVERSE.copy()
    sector_weights = _sector_weights_from_universe(universe)

    security_master_rows: list[dict[str, object]] = []
    price_rows: list[dict[str, object]] = []
    fetch_log: dict[str, str] = {}

    for spec in universe:
        security_master_rows.append(
            {
                "security_id": spec.security_id,
                "ticker": spec.ticker,
                "name": spec.name,
                "asset_class": "Equity",
                "sector": spec.sector,
                "currency": "USD",
            }
        )
        try:
            fetched = _fetch_stooq_close(spec.ticker, start=start, end=end)
            daily = _build_daily_price_series(fetched, start=start, end=end)
            fetch_log[spec.ticker] = "stooq"
        except Exception:
            synthetic_base = float(50 + (abs(hash(spec.ticker)) % 250))
            daily = _synthetic_price_series(spec.ticker, start=start, end=end, base_price=synthetic_base)
            fetch_log[spec.ticker] = "synthetic_fallback"

        for _, row in daily.iterrows():
            price_rows.append(
                {
                    "date": row["date"].date().isoformat(),
                    "security_id": spec.security_id,
                    "price": float(row["close"]),
                    "price_currency": "USD",
                    "source": fetch_log[spec.ticker],
                }
            )

    security_master = pd.DataFrame(security_master_rows)
    prices = pd.DataFrame(price_rows).sort_values(["date", "security_id"]).reset_index(drop=True)

    all_dates = pd.date_range(start, end, freq="D")
    fx_rates = pd.DataFrame(
        {
            "date": [d.date().isoformat() for d in all_dates],
            "ccy_pair": ["USDUSD"] * len(all_dates),
            "rate": [1.0] * len(all_dates),
            "source": ["CONST"] * len(all_dates),
        }
    )

    first_date = all_dates[0].date().isoformat()
    mid_date = all_dates[len(all_dates) // 2].date().isoformat()
    last_date = all_dates[-1].date().isoformat()

    first_prices = prices[prices["date"] == first_date].set_index("security_id")["price"].to_dict()
    mid_prices = prices[prices["date"] == mid_date].set_index("security_id")["price"].to_dict()
    last_prices = prices[prices["date"] == last_date].set_index("security_id")["price"].to_dict()

    transactions_rows: list[dict[str, object]] = [
        {
            "date": first_date,
            "portfolio_id": portfolio_id,
            "security_id": None,
            "quantity": 0.0,
            "price": 0.0,
            "fees": 0.0,
            "txn_type": "CONTRIB",
            "cash_amount": float(initial_capital),
        }
    ]
    running_qty: dict[str, float] = {s.security_id: 0.0 for s in universe}

    for spec in universe:
        px = float(first_prices[spec.security_id])
        target_cash = float(initial_capital) * float(spec.target_weight)
        qty = max(int(target_cash / max(px, 1e-8)), 1)
        fees = round(max(5.0, 0.0005 * target_cash), 2)
        transactions_rows.append(
            {
                "date": first_date,
                "portfolio_id": portfolio_id,
                "security_id": spec.security_id,
                "quantity": float(qty),
                "price": px,
                "fees": fees,
                "txn_type": "BUY",
                "cash_amount": -(px * qty + fees),
            }
        )
        running_qty[spec.security_id] += float(qty)

    if float(topup_amount) > 0:
        transactions_rows.append(
            {
                "date": mid_date,
                "portfolio_id": portfolio_id,
                "security_id": None,
                "quantity": 0.0,
                "price": 0.0,
                "fees": 0.0,
                "txn_type": "CONTRIB",
                "cash_amount": float(topup_amount),
            }
        )
        for spec in universe:
            px = float(mid_prices[spec.security_id])
            target_cash = float(topup_amount) * float(spec.target_weight)
            qty = max(int(target_cash / max(px, 1e-8)), 0)
            if qty <= 0:
                continue
            fees = round(max(2.0, 0.0005 * target_cash), 2)
            transactions_rows.append(
                {
                    "date": mid_date,
                    "portfolio_id": portfolio_id,
                    "security_id": spec.security_id,
                    "quantity": float(qty),
                    "price": px,
                    "fees": fees,
                    "txn_type": "BUY",
                    "cash_amount": -(px * qty + fees),
                }
            )
            running_qty[spec.security_id] += float(qty)

    transactions = pd.DataFrame(transactions_rows)

    holdings_rows: list[dict[str, object]] = []
    for spec in universe:
        qty = float(running_qty[spec.security_id])
        holdings_rows.append(
            {
                "date": last_date,
                "portfolio_id": portfolio_id,
                "security_id": spec.security_id,
                "quantity": qty,
                "market_value_base": qty * float(last_prices[spec.security_id]),
                "cash_balance_base": 0.0,
            }
        )
    holdings_reported = pd.DataFrame(holdings_rows)

    benchmark_price: dict[str, pd.DataFrame] = {}
    benchmark_fetch_log: dict[str, str] = {}
    for sector, etf_symbol in SECTOR_BENCHMARK_ETF.items():
        try:
            fetched = _fetch_stooq_close(etf_symbol, start=start, end=end)
            benchmark_price[sector] = _build_daily_price_series(fetched, start=start, end=end)
            benchmark_fetch_log[sector] = f"stooq:{etf_symbol}"
        except Exception:
            synthetic_base = float(80 + (abs(hash(etf_symbol)) % 120))
            benchmark_price[sector] = _synthetic_price_series(
                etf_symbol,
                start=start,
                end=end,
                base_price=synthetic_base,
            )
            benchmark_fetch_log[sector] = "synthetic_fallback"

    benchmark_weights_rows: list[dict[str, object]] = []
    benchmark_returns_rows: list[dict[str, object]] = []
    sector_return_by_date: dict[str, dict[str, float]] = {}
    for sector, px in benchmark_price.items():
        tmp = px.copy()
        tmp["return"] = tmp["close"].pct_change().fillna(0.0)
        sector_return_by_date[sector] = {
            row["date"].date().isoformat(): float(row["return"])
            for _, row in tmp.iterrows()
        }
    for d in all_dates:
        d_str = d.date().isoformat()
        for sector, weight in sorted(sector_weights.items()):
            benchmark_weights_rows.append(
                {
                    "date": d_str,
                    "benchmark_id": benchmark_id,
                    "sector": sector,
                    "weight": float(weight),
                }
            )
            r = float(sector_return_by_date[sector][d_str])
            benchmark_returns_rows.append(
                {
                    "date": d_str,
                    "benchmark_id": benchmark_id,
                    "sector": sector,
                    "return": r,
                }
            )

    benchmark_weights = pd.DataFrame(benchmark_weights_rows)
    benchmark_returns = pd.DataFrame(benchmark_returns_rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    security_master.to_csv(out_dir / "security_master.csv", index=False)
    prices.to_csv(out_dir / "prices.csv", index=False)
    fx_rates.to_csv(out_dir / "fx_rates.csv", index=False)
    transactions.to_csv(out_dir / "transactions.csv", index=False)
    holdings_reported.to_csv(out_dir / "holdings_reported.csv", index=False)
    benchmark_weights.to_csv(out_dir / "benchmark_weights.csv", index=False)
    benchmark_returns.to_csv(out_dir / "benchmark_returns.csv", index=False)

    summary = {
        "output_dir": str(out_dir),
        "portfolio_id": portfolio_id,
        "benchmark_id": benchmark_id,
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "rows": {
            "security_master": int(len(security_master)),
            "prices": int(len(prices)),
            "fx_rates": int(len(fx_rates)),
            "transactions": int(len(transactions)),
            "holdings_reported": int(len(holdings_reported)),
            "benchmark_weights": int(len(benchmark_weights)),
            "benchmark_returns": int(len(benchmark_returns)),
        },
        "price_source": fetch_log,
        "benchmark_source": benchmark_fetch_log,
    }
    (out_dir / "dataset_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build PBOR-Lite market-based dummy dataset.")
    parser.add_argument(
        "--out-dir",
        default=str(Path(__file__).resolve().parents[1] / "data_real" / "market_dummy"),
        help="Output directory for PBOR input CSVs.",
    )
    parser.add_argument("--start", default="2025-08-22", help="Start date YYYY-MM-DD.")
    parser.add_argument("--end", default=date.today().isoformat(), help="End date YYYY-MM-DD.")
    parser.add_argument("--portfolio-id", default="PF_MKT", help="Portfolio ID.")
    parser.add_argument("--benchmark-id", default="BM1", help="Benchmark ID.")
    parser.add_argument("--initial-capital", type=float, default=1_000_000.0, help="Initial contribution amount.")
    parser.add_argument("--topup-amount", type=float, default=200_000.0, help="Mid-period top-up contribution.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_market_dummy_dataset(
        out_dir=Path(args.out_dir),
        start_date=args.start,
        end_date=args.end,
        portfolio_id=args.portfolio_id,
        benchmark_id=args.benchmark_id,
        initial_capital=float(args.initial_capital),
        topup_amount=float(args.topup_amount),
    )
    print("Market dummy dataset build completed.")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
