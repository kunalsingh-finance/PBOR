from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import unittest

import pandas as pd

from pbor.date_source import derive_date_context, get_trading_sessions


def _has_calendar_libs() -> bool:
    return bool(
        importlib.util.find_spec("exchange_calendars")
        or importlib.util.find_spec("pandas_market_calendars")
    )


class DateSourceTests(unittest.TestCase):
    def test_data_asof_and_generated_timestamps(self) -> None:
        daily = pd.DataFrame(
            [
                {"date": "2026-02-02", "daily_return": 0.01},
                {"date": "2026-02-03", "daily_return": 0.00},
                {"date": "2026-02-04", "daily_return": 0.01},
            ]
        )
        now_utc = datetime(2026, 2, 5, 14, 30, tzinfo=timezone.utc)
        ctx = derive_date_context(
            daily_returns=daily,
            exchange="XNYS",
            now_utc=now_utc,
            clamp_to_market=True,
        )
        self.assertEqual(ctx["data_asof_date"], "2026-02-04")
        self.assertEqual(datetime.fromisoformat(str(ctx["generated_at_utc"])), now_utc)
        et = datetime.fromisoformat(str(ctx["generated_at_et"]))
        self.assertIsNotNone(et.tzinfo)

    def test_asof_is_clamped_to_data_and_market_when_available(self) -> None:
        daily = pd.DataFrame(
            [
                {"date": "2026-02-02", "daily_return": 0.01},
                {"date": "2026-02-03", "daily_return": 0.00},
                {"date": "2026-02-04", "daily_return": 0.01},
            ]
        )
        ctx = derive_date_context(
            daily_returns=daily,
            exchange="XNYS",
            now_utc=datetime(2026, 2, 3, 15, 0, tzinfo=timezone.utc),
            clamp_to_market=True,
        )
        asof = pd.Timestamp(ctx["asof_date"]).date()
        data_asof = pd.Timestamp(ctx["data_asof_date"]).date()
        self.assertLessEqual(asof, data_asof)
        if ctx.get("market_last_closed_session"):
            market_asof = pd.Timestamp(ctx["market_last_closed_session"]).date()
            self.assertLessEqual(asof, market_asof)

    @unittest.skipUnless(_has_calendar_libs(), "calendar library not installed")
    def test_market_sessions_use_exchange_calendar_when_libs_installed(self) -> None:
        sessions = get_trading_sessions("2026-01-01", "2026-01-02", exchange="XNYS")
        session_dates = {s.date() for s in sessions}
        self.assertNotIn(pd.Timestamp("2026-01-01").date(), session_dates)
        self.assertIn(pd.Timestamp("2026-01-02").date(), session_dates)


if __name__ == "__main__":
    unittest.main()
