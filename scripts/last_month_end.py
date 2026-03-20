"""Print the last calendar day of the previous month as YYYY-MM-DD."""

from __future__ import annotations

import pandas as pd


def last_month_end(reference: pd.Timestamp | None = None) -> pd.Timestamp:
    today = (reference or pd.Timestamp.today()).normalize()
    return today.replace(day=1) - pd.Timedelta(days=1)


def main() -> None:
    print(last_month_end().strftime("%Y-%m-%d"))


if __name__ == "__main__":
    main()
