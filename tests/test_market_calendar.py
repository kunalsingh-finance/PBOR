from __future__ import annotations

from datetime import datetime, timezone
import unittest

import pandas as pd

from pbor.market_calendar import derive_reporting_windows


class MarketCalendarTests(unittest.TestCase):
    def test_mtd_start_uses_first_trading_session(self) -> None:
        daily = pd.DataFrame(
            [
                {"date": "2026-02-02", "daily_return": 0.01},
                {"date": "2026-02-03", "daily_return": 0.00},
                {"date": "2026-02-04", "daily_return": 0.01},
            ]
        )
        ctx = derive_reporting_windows(
            daily_returns=daily,
            exchange="XNYS",
            now_utc=datetime(2026, 2, 10, 15, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(ctx["mtd_window"]["start"], "2026-02-02")
        self.assertEqual(ctx["mtd_window"]["end"], "2026-02-04")

    def test_trading_days_are_calendar_sessions_not_row_count(self) -> None:
        daily = pd.DataFrame(
            [
                {"date": "2026-02-02", "daily_return": 0.01},
                {"date": "2026-02-04", "daily_return": 0.01},
            ]
        )
        ctx = derive_reporting_windows(
            daily_returns=daily,
            exchange="XNYS",
            now_utc=datetime(2026, 2, 10, 15, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(int(ctx["mtd_window"]["obs_rows"]), 2)
        self.assertEqual(int(ctx["mtd_window"]["trading_days"]), 3)

    def test_asof_date_is_min_of_data_and_market_dates(self) -> None:
        daily = pd.DataFrame(
            [
                {"date": "2026-02-02", "daily_return": 0.01},
                {"date": "2026-02-03", "daily_return": 0.00},
                {"date": "2026-02-04", "daily_return": 0.01},
            ]
        )
        ctx = derive_reporting_windows(
            daily_returns=daily,
            exchange="XNYS",
            now_utc=datetime(2026, 2, 3, 15, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(ctx["asof_data_date"], "2026-02-04")
        self.assertEqual(ctx["asof_market_date"], "2026-02-02")
        self.assertEqual(ctx["asof_date"], "2026-02-02")


if __name__ == "__main__":
    unittest.main()
