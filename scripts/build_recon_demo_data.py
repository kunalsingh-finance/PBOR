from __future__ import annotations

from pathlib import Path

import pandas as pd

ASOF_DATE = "2026-01-31"
PORTFOLIO_ID = "PF_DEMO"
ACCOUNT_ID = "ACC_MAIN"


def _position(
    security_id: str,
    ticker: str,
    quantity: float,
    price: float,
) -> dict[str, object]:
    return {
        "asof_date": ASOF_DATE,
        "portfolio_id": PORTFOLIO_ID,
        "account_id": ACCOUNT_ID,
        "security_id": security_id,
        "ticker": ticker,
        "quantity": quantity,
        "price": price,
        "market_value_base": round(quantity * price, 2),
    }


def _cash(currency: str, cash_balance: float) -> dict[str, object]:
    return {
        "asof_date": ASOF_DATE,
        "portfolio_id": PORTFOLIO_ID,
        "account_id": ACCOUNT_ID,
        "currency": currency,
        "cash_balance": cash_balance,
    }


def build_recon_demo_data(output_dir: Path | str = Path("data/recon_demo")) -> dict[str, object]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    internal_positions = pd.DataFrame(
        [
            _position("SEC_SPY", "SPY", 16500, 480.25),
            _position("SEC_AAPL", "AAPL", 8000, 185.00),
            _position("SEC_MSFT", "MSFT", 5500, 410.10),
            _position("SEC_JPM", "JPM", 6000, 155.40),
            _position("SEC_QQQ", "QQQ", 7600, 421.35),
        ]
    )
    custodian_positions = pd.DataFrame(
        [
            _position("SEC_SPY", "SPY", 16625, 480.25),
            _position("SEC_AAPL", "AAPL", 8000, 184.70),
            _position("SEC_GOOGL", "GOOGL", 1200, 140.55),
            _position("SEC_JPM", "JPM", 6000, 155.40),
            _position("SEC_QQQ", "QQQ", 7600, 421.35),
        ]
    )
    internal_cash = pd.DataFrame([_cash("USD", 25000.00)])
    bank_cash = pd.DataFrame([_cash("USD", 24200.00)])

    files = {
        "internal_positions.csv": internal_positions,
        "custodian_positions.csv": custodian_positions,
        "internal_cash.csv": internal_cash,
        "bank_cash.csv": bank_cash,
    }
    for file_name, frame in files.items():
        frame.to_csv(output_dir / file_name, index=False)

    return {
        "output_dir": output_dir,
        "files": list(files.keys()),
        "rows": {file_name: int(len(frame)) for file_name, frame in files.items()},
        "intentional_breaks": [
            "SPY quantity break",
            "AAPL price / market value break",
            "MSFT missing in custodian",
            "GOOGL missing in internal PBOR",
            "USD cash break",
        ],
    }


def main() -> None:
    summary = build_recon_demo_data()
    output_dir = Path(summary["output_dir"])
    print("Synthetic PBOR-style reconciliation demo data created.")
    print(f"Output folder: {output_dir}")
    print("Files created:")
    for file_name in summary["files"]:
        print(f"  - {output_dir / file_name}")
    print("Rows created:")
    for file_name, row_count in summary["rows"].items():
        print(f"  - {file_name}: {row_count}")
    print("Intentional breaks injected:")
    for break_name in summary["intentional_breaks"]:
        print(f"  - {break_name}")
    print("Next command:")
    print("python -m src.run_month_end --asof 2026-01-31 --recon-data-dir data/recon_demo")


if __name__ == "__main__":
    main()
