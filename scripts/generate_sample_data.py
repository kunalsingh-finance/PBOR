from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    write_csv(
        data_dir / "security_master.csv",
        [
            {"security_id": "SEC1", "ticker": "AAPL", "name": "Apple Inc", "asset_class": "Equity", "sector": "Tech", "currency": "USD"},
            {"security_id": "SEC2", "ticker": "XOM", "name": "Exxon Mobil", "asset_class": "Equity", "sector": "Energy", "currency": "USD"},
            {"security_id": "SEC3", "ticker": "SAP", "name": "SAP SE", "asset_class": "Equity", "sector": "Tech", "currency": "EUR"},
        ],
    )
    print(f"Sample data generated in {data_dir}")


if __name__ == "__main__":
    main()
