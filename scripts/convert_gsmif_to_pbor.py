from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.error import URLError
from urllib.parse import quote_plus

import pandas as pd


def _parse_yyyymmdd(value: str) -> str | None:
    raw = str(value).strip()
    if len(raw) != 8 or not raw.isdigit():
        return None
    return datetime.strptime(raw, "%Y%m%d").date().isoformat()


def _to_float(value: str) -> float | None:
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw.replace(",", ""))
    except ValueError:
        return None


def _parse_sections(path: Path) -> dict[str, list[list[str]]]:
    sections: dict[str, list[list[str]]] = defaultdict(list)
    current: str | None = None
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row:
                continue
            if row[0] == "BOS":
                current = row[1].strip() if len(row) > 1 else None
                continue
            if row[0] == "EOS":
                current = None
                continue
            if current:
                sections[current].append(row)
    return sections


def _build_equt_nav(sections: dict[str, list[list[str]]], base_currency: str) -> tuple[pd.DataFrame, str]:
    rows: list[dict[str, object]] = []
    account_id = "GSMIF_ACCOUNT"
    for row in sections.get("EQUT", []):
        if len(row) < 21:
            continue
        account_id = str(row[0]).strip() or account_id
        date = _parse_yyyymmdd(row[4] if len(row) > 4 else "")
        nav = _to_float(row[20] if len(row) > 20 else "")
        if nav is None and len(row) > 21:
            nav = _to_float(row[21])
        ccy = str(row[3]).strip() if len(row) > 3 else base_currency
        if date and nav is not None and nav > 0:
            rows.append({"date": date, "nav_base": nav, "currency": ccy or base_currency})
    equt = pd.DataFrame(rows)
    if equt.empty:
        raise ValueError("No valid EQUT NAV rows found in GSMIF file.")
    equt = equt.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return equt, account_id


def _build_fx_rates(
    sections: dict[str, list[list[str]]],
    equt_dates: pd.Series,
    base_currency: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in sections.get("RATE", []):
        if len(row) < 4:
            continue
        date = _parse_yyyymmdd(row[0])
        ccy_from = str(row[1]).strip().upper()
        ccy_to = str(row[2]).strip().upper()
        rate = _to_float(row[3])
        if not date or not ccy_from or not ccy_to or rate is None or rate <= 0:
            continue
        rows.append(
            {
                "date": date,
                "ccy_pair": f"{ccy_from}{ccy_to}",
                "rate": rate,
                "source": "GSMIF_RATE",
            }
        )
    fx = pd.DataFrame(rows)
    # Ensure base/base exists for all valuation dates.
    usd_rows = pd.DataFrame(
        {
            "date": equt_dates.astype(str),
            "ccy_pair": [f"{base_currency}{base_currency}"] * len(equt_dates),
            "rate": [1.0] * len(equt_dates),
            "source": ["SYNTH_BASE"] * len(equt_dates),
        }
    )
    fx = pd.concat([fx, usd_rows], ignore_index=True)
    fx = fx.sort_values(["date", "ccy_pair", "source"]).drop_duplicates(["date", "ccy_pair"], keep="first")
    return fx.reset_index(drop=True)


def _normalize_stooq_symbol(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if not raw:
        return "SPY.US"
    if "." not in raw:
        return f"{raw}.US"
    return raw


def _build_benchmark_from_stooq(
    symbol: str,
    start_date: str,
    end_date: str,
    benchmark_id: str,
    benchmark_sector: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized = _normalize_stooq_symbol(symbol)
    url = f"https://stooq.com/q/d/l/?s={quote_plus(normalized.lower())}&i=d"
    raw = pd.read_csv(url)
    if raw.empty or "Date" not in raw.columns or "Close" not in raw.columns:
        raise ValueError(f"No usable Stooq data for symbol {normalized}.")

    px = raw[["Date", "Close"]].copy()
    px = px.rename(columns={"Date": "date", "Close": "close"})
    px["date"] = pd.to_datetime(px["date"], errors="coerce")
    px["close"] = pd.to_numeric(px["close"], errors="coerce")
    px = px.dropna(subset=["date", "close"]).sort_values("date")
    if px.empty:
        raise ValueError(f"All Stooq rows invalid for symbol {normalized}.")

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    px = px[(px["date"] >= start) & (px["date"] <= end)]
    if px.empty:
        raise ValueError(f"Stooq returned no rows inside {start_date}..{end_date} for {normalized}.")

    calendar = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})
    px = calendar.merge(px, on="date", how="left")
    px["close"] = px["close"].ffill().bfill()
    px["return"] = px["close"].pct_change().fillna(0.0)
    px["date"] = px["date"].dt.date.astype(str)

    benchmark_weights = pd.DataFrame(
        {
            "date": px["date"],
            "benchmark_id": benchmark_id,
            "sector": benchmark_sector,
            "weight": 1.0,
        }
    )
    benchmark_returns = pd.DataFrame(
        {
            "date": px["date"],
            "benchmark_id": benchmark_id,
            "sector": benchmark_sector,
            "return": px["return"].astype(float),
        }
    )
    return benchmark_weights, benchmark_returns


def convert_gsmif_to_pbor(
    gsmif_path: Path,
    output_dir: Path,
    portfolio_id: str,
    benchmark_id: str,
    base_currency: str,
    benchmark_source: str,
    benchmark_symbol: str,
    benchmark_sector: str,
) -> dict[str, object]:
    sections = _parse_sections(gsmif_path)
    equt, account_id = _build_equt_nav(sections=sections, base_currency=base_currency)
    equt = equt.copy()
    equt["date"] = pd.to_datetime(equt["date"])
    calendar = pd.DataFrame({"date": pd.date_range(equt["date"].min(), equt["date"].max(), freq="D")})
    equt = calendar.merge(equt[["date", "nav_base"]], on="date", how="left")
    equt["nav_base"] = equt["nav_base"].ffill().bfill()
    equt["currency"] = base_currency
    equt["date"] = equt["date"].dt.date.astype(str)

    first_date = str(equt["date"].iloc[0])
    last_date = str(equt["date"].iloc[-1])
    nav_first = float(equt["nav_base"].iloc[0])
    nav_last = float(equt["nav_base"].iloc[-1])
    nav_security_id = f"NAV_{account_id}"
    cash_security_id = f"CASH_{base_currency}"

    security_master = pd.DataFrame(
        [
            {
                "security_id": nav_security_id,
                "ticker": nav_security_id,
                "name": f"{account_id} NAV Composite",
                "asset_class": "CompositeNAV",
                "sector": "UNCLASSIFIED",
                "currency": base_currency,
            },
            {
                "security_id": cash_security_id,
                "ticker": cash_security_id,
                "name": f"{base_currency} Cash",
                "asset_class": "Cash",
                "sector": "Cash",
                "currency": base_currency,
            },
        ]
    )

    prices = (
        equt.rename(columns={"nav_base": "price"})[["date", "price"]]
        .assign(security_id=nav_security_id, price_currency=base_currency, source="GSMIF_EQUT")
        .loc[:, ["date", "security_id", "price", "price_currency", "source"]]
    )

    fx_rates = _build_fx_rates(sections=sections, equt_dates=equt["date"], base_currency=base_currency)

    transactions = pd.DataFrame(
        [
            {
                "date": first_date,
                "portfolio_id": portfolio_id,
                "security_id": cash_security_id,
                "quantity": nav_first,
                "price": 1.0,
                "fees": 0.0,
                "txn_type": "CONTRIB",
                "cash_amount": nav_first,
            },
            {
                "date": first_date,
                "portfolio_id": portfolio_id,
                "security_id": nav_security_id,
                "quantity": 1.0,
                "price": nav_first,
                "fees": 0.0,
                "txn_type": "BUY",
                "cash_amount": -nav_first,
            },
        ]
    )

    holdings_reported = pd.DataFrame(
        [
            {
                "date": last_date,
                "portfolio_id": portfolio_id,
                "security_id": nav_security_id,
                "quantity": 1.0,
                "market_value_base": nav_last,
                "cash_balance_base": 0.0,
            }
        ]
    )

    equt = equt.sort_values("date").reset_index(drop=True)
    benchmark_mode = str(benchmark_source).strip().lower()
    benchmark_fetch_error: str | None = None
    benchmark_source_used = "internal_nav"

    if benchmark_mode == "stooq":
        try:
            benchmark_weights, benchmark_returns = _build_benchmark_from_stooq(
                symbol=benchmark_symbol,
                start_date=first_date,
                end_date=last_date,
                benchmark_id=benchmark_id,
                benchmark_sector=benchmark_sector,
            )
            benchmark_source_used = f"stooq:{_normalize_stooq_symbol(benchmark_symbol)}"
        except (ValueError, URLError, OSError, TimeoutError) as exc:
            benchmark_fetch_error = str(exc)
            benchmark_mode = "internal"

    if benchmark_mode != "stooq":
        benchmark_daily_return = equt["nav_base"].pct_change().fillna(0.0).astype(float)
        benchmark_weights = pd.DataFrame(
            {
                "date": equt["date"],
                "benchmark_id": benchmark_id,
                "sector": benchmark_sector,
                "weight": 1.0,
            }
        )
        benchmark_returns = pd.DataFrame(
            {
                "date": equt["date"],
                "benchmark_id": benchmark_id,
                "sector": benchmark_sector,
                "return": benchmark_daily_return,
            }
        )
        benchmark_source_used = "internal_nav"

    output_dir.mkdir(parents=True, exist_ok=True)
    security_master.to_csv(output_dir / "security_master.csv", index=False)
    prices.to_csv(output_dir / "prices.csv", index=False)
    fx_rates.to_csv(output_dir / "fx_rates.csv", index=False)
    transactions.to_csv(output_dir / "transactions.csv", index=False)
    holdings_reported.to_csv(output_dir / "holdings_reported.csv", index=False)
    benchmark_weights.to_csv(output_dir / "benchmark_weights.csv", index=False)
    benchmark_returns.to_csv(output_dir / "benchmark_returns.csv", index=False)

    return {
        "output_dir": str(output_dir),
        "portfolio_id": portfolio_id,
        "benchmark_id": benchmark_id,
        "source_account_id": account_id,
        "benchmark_source": benchmark_source_used,
        "benchmark_fetch_error": benchmark_fetch_error,
        "rows": {
            "equt_nav_rows": int(len(equt)),
            "prices_rows": int(len(prices)),
            "fx_rows": int(len(fx_rates)),
            "transactions_rows": int(len(transactions)),
            "benchmark_rows": int(len(benchmark_returns)),
        },
        "date_range": {"start": first_date, "end": last_date},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert GSMIF export CSV to PBOR-Lite input files.")
    parser.add_argument("--input", required=True, help="Path to GSMIF CSV file.")
    parser.add_argument("--out-dir", required=True, help="Output folder for PBOR input CSVs.")
    parser.add_argument("--portfolio-id", default="PF_GSMIF", help="Portfolio ID for generated records.")
    parser.add_argument("--benchmark-id", default="BM1", help="Benchmark ID for generated records.")
    parser.add_argument("--base-currency", default="USD", help="Base/reporting currency.")
    parser.add_argument(
        "--benchmark-source",
        default="stooq",
        choices=["stooq", "internal"],
        help="Benchmark return source: stooq market symbol or internal NAV fallback.",
    )
    parser.add_argument(
        "--benchmark-symbol",
        default="SPY.US",
        help="Market symbol for --benchmark-source stooq (e.g., SPY.US).",
    )
    parser.add_argument(
        "--benchmark-sector",
        default="UNCLASSIFIED",
        help="Sector label used for benchmark rows (default UNCLASSIFIED).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = convert_gsmif_to_pbor(
        gsmif_path=Path(args.input),
        output_dir=Path(args.out_dir),
        portfolio_id=str(args.portfolio_id),
        benchmark_id=str(args.benchmark_id),
        base_currency=str(args.base_currency).upper(),
        benchmark_source=str(args.benchmark_source),
        benchmark_symbol=str(args.benchmark_symbol),
        benchmark_sector=str(args.benchmark_sector).upper(),
    )
    print("GSMIF conversion completed.")
    for k, v in summary.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
