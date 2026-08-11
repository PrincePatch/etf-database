"""Tests for the performance statistics engine.

The assertions here are deliberately arithmetic rather than structural. A stats
engine that returns a float of the right dtype for every field and gets the
compounding wrong is worse than one that crashes, so wherever an expected value
can be derived independently -- analytically, or by a slow and obviously-correct
Python loop -- it is, and the engine is checked against that rather than against
itself.
"""

from __future__ import annotations

import math
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from pipeline import schema
from pipeline.config import (
    MIN_HISTORY_DAYS,
    RISK_FREE_RATE,
    TRADING_DAYS_PER_YEAR,
)
from pipeline.stats import (
    MIN_BETA_OBSERVATIONS,
    compute_all,
    compute_monthly_returns,
    compute_performance,
    compute_yearly_returns,
)


# --------------------------------------------------------------------------- #
# Fixtures and independent reference implementations
# --------------------------------------------------------------------------- #


def frame(dates, adj, isin="TEST0000001", close=None, currency="EUR") -> pd.DataFrame:
    """A PRICES-shaped frame for one fund."""
    adj = np.asarray(adj, dtype=float)
    return pd.DataFrame(
        {
            "isin": isin,
            "date": pd.to_datetime(pd.Series(list(dates))),
            "close": adj if close is None else np.asarray(close, dtype=float),
            "adj_close": adj,
            "volume": 1_000.0,
            "currency": currency,
        }
    )


def business_days(start: str, end: str) -> list[date]:
    return [d.date() for d in pd.bdate_range(start, end)]


def anchor_bar(dates: list[date], target: date) -> int | None:
    """Index of the last bar on or before `target` -- the slow, obvious version."""
    found = None
    for i, d in enumerate(dates):
        if d <= target:
            found = i
        else:
            break
    return found


def minus_months(day: date, months: int) -> date:
    """Calendar month subtraction, clamped to the end of the target month."""
    total = (day.year * 12 + day.month - 1) - months
    year, month = divmod(total, 12)
    month += 1
    last = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
    return date(year, month, min(day.day, last))


def slow_max_drawdown(prices) -> float:
    """Worst peak-to-trough drop, by the definition, one bar at a time."""
    peak = -math.inf
    worst = 0.0
    for p in prices:
        peak = max(peak, p)
        worst = min(worst, p / peak - 1.0)
    return worst


def geometric_series(dates, daily_log_return: float, start: float = 100.0):
    return start * np.exp(daily_log_return * np.arange(len(dates)))


# --------------------------------------------------------------------------- #
# Analytically known series
# --------------------------------------------------------------------------- #


def test_constant_daily_return_has_analytic_cagr_and_zero_volatility():
    """A series that grows by the same log amount every bar pins every number."""
    r = 0.0004  # daily log return
    dates = business_days("2014-01-01", "2024-12-31")
    prices = geometric_series(dates, r)
    row = compute_performance(frame(dates, prices))

    # Cumulative return over a calendar window is exp(r * bars in the window).
    for window, months in (("1y", 12), ("3y", 36), ("5y", 60), ("10y", 120)):
        start = anchor_bar(dates, minus_months(dates[-1], months))
        bars = (len(dates) - 1) - start
        assert row[f"ret_{window}"] == pytest.approx(math.expm1(r * bars), rel=1e-12)

    for window, years in (("3y", 3), ("5y", 5), ("10y", 10)):
        expected = (1.0 + row[f"ret_{window}"]) ** (1.0 / years) - 1.0
        assert row[f"cagr_{window}"] == pytest.approx(expected, rel=1e-12)

    # No dispersion at all, so volatility is zero and the ratios are undefined
    # rather than an excess return divided by float noise.
    for window in ("1y", "3y", "5y"):
        assert row[f"vol_{window}"] == pytest.approx(0.0, abs=1e-9)
        assert row[f"sharpe_{window}"] is None
    assert row["sortino_3y"] is None

    # A monotonically rising series never draws down and is always at its high.
    assert row["max_drawdown_max"] == pytest.approx(0.0, abs=1e-12)
    assert row["current_drawdown"] == pytest.approx(0.0, abs=1e-12)
    assert row["distance_from_ath"] == pytest.approx(0.0, abs=1e-12)
    assert row["positive_months_pct"] == pytest.approx(1.0)
    assert row["ath_date"] == dates[-1]


def test_two_state_series_has_analytic_volatility_and_sharpe():
    """Log returns alternating +a / -a give a stdev that is known in closed form."""
    a = 0.01
    dates = business_days("2022-06-01", "2024-06-03")
    prices = 100.0 * np.exp(a * (np.arange(len(dates)) % 2))
    row = compute_performance(frame(dates, prices))

    start = anchor_bar(dates, minus_months(dates[-1], 12))
    n = (len(dates) - 1) - start  # daily returns inside the 1-year window

    # An even count of alternating returns has mean zero and a ddof=1 stdev of
    # a*sqrt(n/(n-1)); an odd count is left with a mean of +-a/n, which works
    # out to a*sqrt((n+1)/n).
    closed_form = a * math.sqrt((n + 1) / n if n % 2 else n / (n - 1))
    expected_vol = closed_form * math.sqrt(TRADING_DAYS_PER_YEAR)
    assert row["vol_1y"] == pytest.approx(expected_vol, rel=1e-9)

    # Sharpe is the annualised window return less the risk-free rate, over that
    # volatility -- checked against the engine's own published inputs.
    assert row["sharpe_1y"] == pytest.approx(
        (row["ret_1y"] - RISK_FREE_RATE) / row["vol_1y"], rel=1e-9
    )


def test_volatility_uses_log_returns_not_simple_returns():
    rng = np.random.default_rng(11)
    dates = business_days("2020-01-01", "2024-12-31")
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0002, 0.02, len(dates))))
    row = compute_performance(frame(dates, prices))

    start = anchor_bar(dates, minus_months(dates[-1], 12))
    log_vol = np.std(np.diff(np.log(prices[start:])), ddof=1) * math.sqrt(
        TRADING_DAYS_PER_YEAR
    )
    simple_vol = np.std(np.diff(prices[start:]) / prices[start:-1], ddof=1) * math.sqrt(
        TRADING_DAYS_PER_YEAR
    )
    assert row["vol_1y"] == pytest.approx(log_vol, rel=1e-9)
    # At 2% daily the two differ enough that the test can tell them apart.
    assert abs(log_vol - simple_vol) > 1e-4


def test_cumulative_and_annualised_are_different_numbers():
    rng = np.random.default_rng(3)
    dates = business_days("2012-01-01", "2024-12-31")
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0004, 0.01, len(dates))))
    row = compute_performance(frame(dates, prices))

    for window, years in (("3y", 3), ("5y", 5), ("10y", 10)):
        cumulative, annual = row[f"ret_{window}"], row[f"cagr_{window}"]
        assert (1.0 + annual) ** years == pytest.approx(1.0 + cumulative, rel=1e-9)
        assert abs(cumulative - annual) > 0.01  # they are genuinely not the same field


# --------------------------------------------------------------------------- #
# Calendar anchoring
# --------------------------------------------------------------------------- #


def test_leap_day_anniversary_clamps_to_28_february():
    """29 Feb minus one year is 28 Feb, and the bar on that day is the anchor."""
    dates = [date(2023, 1, 2) + timedelta(days=i) for i in range(425)]
    assert date(2024, 2, 29) in dates and date(2023, 2, 28) in dates
    dates = dates[: dates.index(date(2024, 2, 29)) + 1]

    prices = geometric_series(dates, 0.0004)
    row = compute_performance(frame(dates, prices))

    base = prices[dates.index(date(2023, 2, 28))]
    assert row["price_date"] == date(2024, 2, 29)
    assert row["ret_1y"] == pytest.approx(prices[-1] / base - 1.0, rel=1e-12)
    # The 1 March bar is the wrong anchor and must not be the one chosen.
    wrong = prices[dates.index(date(2023, 3, 1))]
    assert row["ret_1y"] != pytest.approx(prices[-1] / wrong - 1.0, rel=1e-9)


def test_anniversary_on_a_holiday_falls_back_to_the_closest_prior_bar():
    """No bar on the anniversary means the last bar before it, not the next one."""
    dates = [d for d in business_days("2021-01-01", "2024-03-08")]
    # Punch a hole around the anniversary of the final bar.
    last = dates[-1]
    anniversary = minus_months(last, 12)
    hole = {d for d in dates if anniversary - timedelta(days=4) <= d <= anniversary}
    dates = [d for d in dates if d not in hole]
    prices = geometric_series(dates, 0.0003)
    row = compute_performance(frame(dates, prices))

    expected_index = anchor_bar(dates, anniversary)
    assert dates[expected_index] < anniversary - timedelta(days=3)  # the hole worked
    assert row["ret_1y"] == pytest.approx(
        prices[-1] / prices[expected_index] - 1.0, rel=1e-12
    )


def test_windows_are_calendar_anchored_not_bar_counted():
    """A year with a long trading halt has far fewer than 252 bars in it."""
    dates = business_days("2019-01-01", "2024-06-28")
    halt = {d for d in dates if date(2023, 9, 1) <= d <= date(2024, 1, 31)}
    dates = [d for d in dates if d not in halt]
    rng = np.random.default_rng(5)
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.01, len(dates))))
    row = compute_performance(frame(dates, prices))

    calendar_index = anchor_bar(dates, minus_months(dates[-1], 12))
    bars_in_year = len(dates) - 1 - calendar_index
    assert bars_in_year < 200  # the halt really did shorten the bar count

    assert row["ret_1y"] == pytest.approx(
        prices[-1] / prices[calendar_index] - 1.0, rel=1e-12
    )
    # The "last 252 bars" answer is a different number, and the wrong one.
    assert row["ret_1y"] != pytest.approx(prices[-1] / prices[-253] - 1.0, rel=1e-6)


def test_ytd_is_measured_from_the_previous_year_end_close():
    dates = business_days("2021-01-01", "2024-05-31")
    rng = np.random.default_rng(9)
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0002, 0.009, len(dates))))
    row = compute_performance(frame(dates, prices))

    base = anchor_bar(dates, date(2023, 12, 31))
    assert dates[base].year == 2023
    assert row["ret_ytd"] == pytest.approx(prices[-1] / prices[base] - 1.0, rel=1e-12)


def test_windows_are_anchored_on_the_funds_own_last_bar_when_delisted():
    """A fund that stopped in 2018 reports its final year, not a fabricated 0%."""
    dates = business_days("2008-01-01", "2018-06-29")
    prices = geometric_series(dates, 0.0002)
    delisted = frame(dates, prices, isin="DEAD00000001")

    row = compute_performance(delisted, as_of=date(2026, 1, 1))
    assert row["price_date"] == date(2018, 6, 29)

    base = anchor_bar(dates, minus_months(dates[-1], 12))
    assert row["ret_1y"] == pytest.approx(prices[-1] / prices[base] - 1.0, rel=1e-12)
    assert row["ret_1y"] != pytest.approx(0.0, abs=1e-6)


def test_as_of_discards_later_bars():
    dates = business_days("2018-01-01", "2024-12-31")
    prices = geometric_series(dates, 0.0003)
    cut = date(2023, 6, 30)
    row = compute_performance(frame(dates, prices), as_of=cut)

    kept = [d for d in dates if d <= cut]
    assert row["price_date"] == kept[-1]
    assert row["history_days"] == len(kept)
    assert row["computed_at"] == cut


# --------------------------------------------------------------------------- #
# Windows longer than the available history
# --------------------------------------------------------------------------- #


def test_window_longer_than_history_is_none_not_zero():
    """Two years of data cannot produce a 3, 5 or 10 year figure."""
    dates = business_days("2022-07-01", "2024-06-28")
    prices = geometric_series(dates, 0.0005)
    row = compute_performance(frame(dates, prices))

    assert row["ret_1y"] is not None
    for field in (
        "ret_3y", "ret_5y", "ret_10y",
        "cagr_3y", "cagr_5y", "cagr_10y",
        "vol_3y", "vol_5y", "sharpe_3y", "sharpe_5y", "sortino_3y",
        "max_drawdown_3y", "max_drawdown_5y",
    ):
        assert row[field] is None, f"{field} should be null, got {row[field]}"

    # ret_max still covers what does exist, and is not a 3-year figure.
    assert row["ret_max"] == pytest.approx(prices[-1] / prices[0] - 1.0, rel=1e-12)


def test_history_shorter_than_min_history_days_emits_nulls():
    dates = business_days("2024-01-01", "2024-02-05")[: MIN_HISTORY_DAYS - 1]
    prices = geometric_series(dates, 0.001)
    row = compute_performance(frame(dates, prices))

    assert row["history_days"] == len(dates) < MIN_HISTORY_DAYS
    # Provenance survives so the UI can say "not enough history" with context.
    assert row["isin"] == "TEST0000001"
    assert row["history_start"] == dates[0]
    assert row["price_date"] == dates[-1]
    assert row["price_last"] == pytest.approx(prices[-1])
    # Everything the numbers would imply is withheld.
    for field in ("ret_1d", "ret_1w", "ret_1m", "ret_max", "vol_1y",
                  "max_drawdown_max", "current_drawdown", "ath", "ath_date",
                  "best_month", "cagr_inception"):
        assert row[field] is None, f"{field} should be null on a thin history"


def test_cagr_inception_needs_a_full_year():
    dates = business_days("2024-01-01", "2024-07-01")
    prices = geometric_series(dates, 0.002)
    row = compute_performance(frame(dates, prices))
    assert row["ret_max"] is not None
    assert row["cagr_inception"] is None


# --------------------------------------------------------------------------- #
# Drawdown
# --------------------------------------------------------------------------- #


def test_max_drawdown_on_a_hand_built_v_shape():
    """100 -> 200 -> 50 -> 150: the worst drop is 200 to 50, i.e. -75%."""
    shape = [100.0, 120.0, 200.0, 160.0, 80.0, 50.0, 90.0, 120.0, 150.0]
    assert slow_max_drawdown(shape) == pytest.approx(-0.75)

    # Padded with flat bars so the fund clears the MIN_HISTORY_DAYS gate; the
    # flat prefix cannot itself draw down, so the answer is unchanged.
    dates = business_days("2023-11-01", "2024-01-31")
    prices = [100.0] * (len(dates) - len(shape)) + shape
    row = compute_performance(frame(dates, prices))

    assert row["max_drawdown_max"] == pytest.approx(-0.75, rel=1e-12)
    assert row["max_drawdown_max"] == pytest.approx(slow_max_drawdown(prices), rel=1e-12)
    assert row["ath"] == pytest.approx(200.0)
    assert row["ath_date"] == dates[prices.index(200.0)]
    # Ends at 150 against a 200 peak.
    assert row["current_drawdown"] == pytest.approx(150.0 / 200.0 - 1.0, rel=1e-12)
    assert row["distance_from_ath"] == pytest.approx(-0.25, rel=1e-12)


def test_drawdown_is_never_positive_and_matches_the_slow_definition():
    rng = np.random.default_rng(23)
    dates = business_days("2016-01-01", "2024-12-31")
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0001, 0.013, len(dates))))
    row = compute_performance(frame(dates, prices))

    assert row["max_drawdown_max"] == pytest.approx(slow_max_drawdown(prices), rel=1e-10)
    for window, months in (("1y", 12), ("3y", 36), ("5y", 60)):
        start = anchor_bar(dates, minus_months(dates[-1], months))
        expected = slow_max_drawdown(prices[start:])
        assert row[f"max_drawdown_{window}"] == pytest.approx(expected, rel=1e-10)
        assert row[f"max_drawdown_{window}"] <= 0.0

    # A shorter window can never be worse than a longer one that contains it.
    assert row["max_drawdown_1y"] >= row["max_drawdown_3y"] >= row["max_drawdown_max"]
    assert row["distance_from_ath"] <= 0.0


def test_distance_from_ath_is_zero_at_a_new_high():
    dates = business_days("2020-01-01", "2024-12-31")
    prices = geometric_series(dates, 0.0005)
    row = compute_performance(frame(dates, prices))
    assert row["distance_from_ath"] == pytest.approx(0.0, abs=1e-12)
    assert row["ath"] == pytest.approx(prices[-1], rel=1e-9)


# --------------------------------------------------------------------------- #
# Total return, not price return
# --------------------------------------------------------------------------- #


def test_returns_come_from_adj_close_and_ignore_close():
    """A distributing fund whose price went nowhere still has a total return."""
    dates = business_days("2020-01-01", "2024-12-31")
    close = np.full(len(dates), 100.0)  # flat quote, all the return paid out
    adj = geometric_series(dates, 0.0004)
    row = compute_performance(frame(dates, adj, close=close))

    base = anchor_bar(dates, minus_months(dates[-1], 12))
    assert row["ret_1y"] == pytest.approx(adj[-1] / adj[base] - 1.0, rel=1e-12)
    assert row["ret_1y"] > 0.09  # the price return would have been exactly 0
    # price_last is the quote the holder sees, which is the raw close.
    assert row["price_last"] == pytest.approx(100.0)


def test_missing_adj_close_is_refused_rather_than_silently_substituted():
    dates = business_days("2024-01-01", "2024-03-01")
    bad = pd.DataFrame({"isin": "X", "date": pd.to_datetime(dates), "close": 100.0})
    with pytest.raises(ValueError, match="adj_close"):
        compute_performance(bad)


# --------------------------------------------------------------------------- #
# Calendar-period returns
# --------------------------------------------------------------------------- #


def test_monthly_returns_are_measured_from_the_previous_month_end():
    dates = business_days("2023-01-01", "2023-06-30")
    rng = np.random.default_rng(31)
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.01, len(dates))))
    monthly = compute_monthly_returns(frame(dates, prices))

    assert list(monthly.columns) == ["isin", "year", "month", "ret", "partial"]
    assert len(monthly) == 6

    by_month: dict[tuple[int, int], int] = {}
    for i, d in enumerate(dates):
        by_month[(d.year, d.month)] = i  # last bar of each month

    ends = sorted(by_month.items())
    for position, ((year, month), last_index) in enumerate(ends):
        row = monthly[(monthly.year == year) & (monthly.month == month)].iloc[0]
        base_index = 0 if position == 0 else ends[position - 1][1]
        expected = prices[last_index] / prices[base_index] - 1.0
        assert row["ret"] == pytest.approx(expected, rel=1e-6)

    # Only the stub first month and the still-running last month are partial.
    assert monthly["partial"].tolist() == [True, False, False, False, False, True]


def test_yearly_returns_and_partial_flagging():
    dates = business_days("2019-06-03", "2023-03-31")
    prices = geometric_series(dates, 0.0003)
    yearly = compute_yearly_returns(frame(dates, prices))

    assert yearly["year"].tolist() == [2019, 2020, 2021, 2022, 2023]
    # 2019 starts in June and 2023 stops in March: both are clipped years.
    assert yearly["partial"].tolist() == [True, False, False, False, True]

    last_of_year = {}
    for i, d in enumerate(dates):
        last_of_year[d.year] = i
    for position, year in enumerate(sorted(last_of_year)):
        base = 0 if position == 0 else last_of_year[year - 1]
        expected = prices[last_of_year[year]] / prices[base] - 1.0
        row = yearly[yearly.year == year].iloc[0]
        assert row["ret"] == pytest.approx(expected, rel=1e-6)


def test_a_full_calendar_year_is_not_flagged_partial():
    dates = business_days("2019-01-01", "2024-12-31")
    prices = geometric_series(dates, 0.0002)
    yearly = compute_yearly_returns(frame(dates, prices))
    middle = yearly[(yearly.year > 2019) & (yearly.year < 2024)]
    assert not middle["partial"].any()
    assert len(middle) == 4


def test_best_and_worst_month_ignore_partial_months():
    """The stub launch month must not be able to win or lose the ranking."""
    dates = business_days("2021-01-20", "2024-03-05")
    prices = np.full(len(dates), 100.0)
    # A violent move confined to the partial first month.
    stub = [i for i, d in enumerate(dates) if d.year == 2021 and d.month == 1]
    prices[stub[-1] :] *= 3.0
    # A milder, genuine move inside a whole month.
    march = [i for i, d in enumerate(dates) if d.year == 2021 and d.month == 3]
    prices[march[-1] :] *= 1.10
    row = compute_performance(frame(dates, prices))

    assert row["best_month"] == pytest.approx(0.10, rel=1e-6)
    assert row["worst_month"] == pytest.approx(0.0, abs=1e-9)
    assert 0.0 < row["positive_months_pct"] < 0.1


# --------------------------------------------------------------------------- #
# Benchmark comparison
# --------------------------------------------------------------------------- #


def test_beta_and_correlation_against_itself_are_exactly_one():
    rng = np.random.default_rng(41)
    dates = business_days("2019-01-01", "2024-12-31")
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.011, len(dates))))
    benchmark = pd.Series(prices, index=pd.to_datetime(dates))
    row = compute_performance(frame(dates, prices), benchmark=benchmark)

    assert row["beta_vs_world"] == pytest.approx(1.0, abs=1e-12)
    assert row["correlation_vs_world"] == pytest.approx(1.0, abs=1e-12)


def test_beta_is_computed_on_the_intersection_of_trading_calendars():
    """The fund trades a subset of the benchmark's sessions at identical prices.

    Aligned properly, that is the same asset: beta is exactly 1 and correlation
    exactly 1. Differencing each series on its own calendar first and
    intersecting the returns afterwards lines a four-day fund move up against a
    one-day benchmark move, and the damage shows up as a correlation of 0.92
    between a series and itself -- which is what this asserts against.
    """
    rng = np.random.default_rng(43)
    dates = business_days("2019-01-01", "2024-12-31")
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0002, 0.012, len(dates))))
    benchmark = pd.Series(prices, index=pd.to_datetime(dates))

    # The fund closes for three consecutive sessions every three weeks: a
    # different holiday calendar, not merely a different number of holidays.
    keep = [i for i in range(len(dates)) if i % 15 not in (3, 4, 5)]
    fund_dates = [dates[i] for i in keep]
    fund_prices = prices[keep]

    row = compute_performance(frame(fund_dates, fund_prices), benchmark=benchmark)
    assert row["beta_vs_world"] == pytest.approx(1.0, abs=1e-9)
    assert row["correlation_vs_world"] == pytest.approx(1.0, abs=1e-9)

    # Show that the naive route really does give a different, wrong answer.
    fund_returns = pd.Series(np.diff(np.log(fund_prices)), index=fund_dates[1:])
    bench_returns = pd.Series(np.diff(np.log(prices)), index=dates[1:])
    joined = pd.concat([fund_returns, bench_returns], axis=1, join="inner").dropna()
    naive_beta = joined.cov().iloc[0, 1] / joined.iloc[:, 1].var()
    naive_correlation = joined.corr().iloc[0, 1]
    assert abs(naive_beta - 1.0) > 0.005
    assert naive_correlation < 0.95


def test_beta_scales_with_a_levered_tracker():
    rng = np.random.default_rng(47)
    dates = business_days("2019-01-01", "2024-12-31")
    steps = rng.normal(0.0002, 0.01, len(dates))
    benchmark = pd.Series(100.0 * np.exp(np.cumsum(steps)), index=pd.to_datetime(dates))
    levered = 100.0 * np.exp(np.cumsum(2.0 * steps))

    row = compute_performance(frame(dates, levered), benchmark=benchmark)
    assert row["beta_vs_world"] == pytest.approx(2.0, abs=1e-9)
    assert row["correlation_vs_world"] == pytest.approx(1.0, abs=1e-9)


def test_beta_is_none_when_the_calendars_barely_overlap():
    rng = np.random.default_rng(53)
    dates = business_days("2019-01-01", "2024-12-31")
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0002, 0.01, len(dates))))

    # The yardstick only trades on a handful of the fund's sessions.
    sparse = dates[::12]
    benchmark = pd.Series(prices[::12], index=pd.to_datetime(sparse))
    assert len([d for d in sparse if d >= minus_months(dates[-1], 36)]) < MIN_BETA_OBSERVATIONS

    row = compute_performance(frame(dates, prices), benchmark=benchmark)
    assert row["beta_vs_world"] is None
    assert row["correlation_vs_world"] is None


def test_beta_is_none_without_a_benchmark():
    dates = business_days("2019-01-01", "2024-12-31")
    row = compute_performance(frame(dates, geometric_series(dates, 0.0003)))
    assert row["beta_vs_world"] is None
    assert row["correlation_vs_world"] is None


# --------------------------------------------------------------------------- #
# Degenerate inputs
# --------------------------------------------------------------------------- #


def degenerate_frames() -> dict[str, pd.DataFrame]:
    long_dates = business_days("2020-01-01", "2024-12-31")
    gapped = [d for d in long_dates if not (date(2022, 3, 1) <= d <= date(2022, 8, 31))]
    return {
        "empty": frame([], []),
        "single_row": frame([date(2024, 1, 2)], [100.0]),
        "two_rows": frame([date(2024, 1, 2), date(2024, 1, 3)], [100.0, 101.0]),
        "duplicate_dates": frame(
            [date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 3)], [100.0, 101.0, 102.0]
        ),
        "unsorted": frame(
            [date(2024, 1, 4), date(2024, 1, 2), date(2024, 1, 3)], [102.0, 100.0, 101.0]
        ),
        "nan_and_zero_prices": frame(
            business_days("2023-01-01", "2024-12-31"),
            np.where(
                np.arange(len(business_days("2023-01-01", "2024-12-31"))) % 37 == 0,
                np.nan,
                100.0,
            ),
        ),
        "all_prices_unusable": frame(
            business_days("2024-01-01", "2024-03-01"),
            np.zeros(len(business_days("2024-01-01", "2024-03-01"))),
        ),
        "negative_prices": frame(
            business_days("2024-01-01", "2024-06-01"),
            -np.ones(len(business_days("2024-01-01", "2024-06-01"))),
        ),
        "multi_week_gaps": frame(gapped, geometric_series(gapped, 0.0003)),
        "delisted": frame(
            business_days("2009-01-01", "2015-03-31"),
            geometric_series(business_days("2009-01-01", "2015-03-31"), 0.0002),
        ),
        "null_dates": pd.DataFrame(
            {
                "isin": "X",
                "date": pd.to_datetime([date(2024, 1, 2), None, date(2024, 1, 3)]),
                "close": [100.0, 101.0, 102.0],
                "adj_close": [100.0, 101.0, 102.0],
                "currency": "EUR",
            }
        ),
    }


@pytest.mark.parametrize("name", sorted(degenerate_frames()))
def test_degenerate_inputs_never_raise(name):
    prices = degenerate_frames()[name]
    row = compute_performance(prices)
    assert set(row) == set(schema.PERFORMANCE.names)
    compute_yearly_returns(prices)
    compute_monthly_returns(prices)
    performance, yearly, monthly = compute_all({"X0000000001": prices})
    assert performance.num_rows == 1
    assert performance.schema.equals(schema.PERFORMANCE)
    assert yearly.schema.equals(schema.RETURNS_YEARLY)
    assert monthly.schema.equals(schema.RETURNS_MONTHLY)


def test_duplicate_dates_keep_the_last_arrival():
    dates = business_days("2024-01-01", "2024-06-28")
    prices = list(geometric_series(dates, 0.0004))
    # A corrected bar re-sent for the final session.
    dates = dates + [dates[-1]]
    prices = prices + [999.0]
    row = compute_performance(frame(dates, prices))
    assert row["history_days"] == len(set(dates))
    assert row["price_last"] == pytest.approx(999.0)


def test_unsorted_input_matches_sorted_input():
    dates = business_days("2020-01-01", "2024-12-31")
    rng = np.random.default_rng(61)
    prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.01, len(dates))))
    ordered = compute_performance(frame(dates, prices))

    shuffle = rng.permutation(len(dates))
    scrambled = compute_performance(
        frame([dates[i] for i in shuffle], prices[shuffle])
    )
    for field in schema.PERFORMANCE.names:
        assert ordered[field] == scrambled[field] or (
            isinstance(ordered[field], float)
            and ordered[field] == pytest.approx(scrambled[field], rel=1e-12)
        ), field


def test_zero_and_nan_prices_are_dropped_not_treated_as_a_crash():
    dates = business_days("2023-01-01", "2024-12-31")
    prices = geometric_series(dates, 0.0004)
    poisoned = prices.copy()
    poisoned[100] = 0.0
    poisoned[150] = np.nan
    clean_dates = [d for i, d in enumerate(dates) if i not in (100, 150)]
    clean_prices = np.delete(prices, [100, 150])

    row = compute_performance(frame(dates, poisoned))
    reference = compute_performance(frame(clean_dates, clean_prices))
    assert row["history_days"] == len(dates) - 2
    assert row["ret_max"] == pytest.approx(reference["ret_max"], rel=1e-12)
    assert row["max_drawdown_max"] == pytest.approx(0.0, abs=1e-12)  # never a -100% day


def test_compute_performance_refuses_a_multi_fund_frame():
    dates = business_days("2024-01-01", "2024-06-28")
    a = frame(dates, geometric_series(dates, 0.0002), isin="AAA00000001")
    b = frame(dates, geometric_series(dates, 0.0003), isin="BBB00000001")
    with pytest.raises(ValueError, match="one fund"):
        compute_performance(pd.concat([a, b]))


# --------------------------------------------------------------------------- #
# The universe driver
# --------------------------------------------------------------------------- #


def small_universe(n: int = 25, seed: int = 71) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    dates = business_days("2012-01-01", "2024-12-31")
    universe = {}
    for i in range(n):
        start = rng.integers(0, len(dates) - 400)
        own = dates[int(start) :]
        prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.011, len(own))))
        universe[f"IE{i:010d}"] = frame(own, prices, isin=f"IE{i:010d}")
    return universe


def test_compute_all_conforms_to_the_declared_schemas():
    universe = small_universe()
    performance, yearly, monthly = compute_all(universe)

    assert performance.schema.equals(schema.PERFORMANCE)
    assert yearly.schema.equals(schema.RETURNS_YEARLY)
    assert monthly.schema.equals(schema.RETURNS_MONTHLY)
    assert performance.num_rows == len(universe)
    assert performance.column("isin").to_pylist() == list(universe)


def test_compute_all_matches_compute_performance_fund_by_fund():
    universe = small_universe(n=12, seed=73)
    benchmark = pd.Series(
        next(iter(universe.values()))["adj_close"].to_numpy(),
        index=pd.to_datetime(next(iter(universe.values()))["date"]),
    )
    performance, yearly, monthly = compute_all(universe, benchmark)
    table = performance.to_pandas()

    for position, (isin, prices) in enumerate(universe.items()):
        row = compute_performance(prices, benchmark=benchmark)
        for field in schema.PERFORMANCE.names:
            batched = table.iloc[position][field]
            single = row[field]
            if single is None:
                assert batched is None or pd.isna(batched), f"{isin}.{field}"
            elif isinstance(single, float):
                # float32 storage is the only difference between the two paths.
                assert batched == pytest.approx(single, rel=1e-6), f"{isin}.{field}"
            else:
                assert batched == single, f"{isin}.{field}"

    assert len(yearly.to_pandas()) == sum(
        len(compute_yearly_returns(p)) for p in universe.values()
    )
    assert len(monthly.to_pandas()) == sum(
        len(compute_monthly_returns(p)) for p in universe.values()
    )


def test_compute_all_keeps_a_row_for_a_fund_with_no_usable_bars():
    universe = small_universe(n=3, seed=79)
    universe["EMPTY00000"] = frame([], [], isin="EMPTY00000")
    performance, _, _ = compute_all(universe)

    assert performance.num_rows == 4
    assert performance.column("isin").to_pylist()[-1] == "EMPTY00000"
    assert performance.column("history_days").to_pylist()[-1] is None
    assert performance.column("ret_1y").to_pylist()[-1] is None


def test_compute_all_accepts_a_long_frame_and_an_arrow_table():
    universe = small_universe(n=8, seed=83)
    expected = compute_all(universe)[0].to_pandas()

    checked = ("ret_1y", "ret_max", "vol_3y", "max_drawdown_max")

    # A long frame carries the same float64 prices, so it must agree bit for bit.
    from_frame = compute_all(pd.concat(universe.values(), ignore_index=True))[0].to_pandas()
    assert from_frame["isin"].tolist() == expected["isin"].tolist()
    for field in checked:
        pd.testing.assert_series_equal(from_frame[field], expected[field], check_names=False)

    # The arrow path goes through the PRICES schema, where adj_close is float32.
    # Seven significant digits on the price is ~1e-7 on a log difference, so the
    # returns agree in absolute terms but not to float64 precision.
    long_frame = pd.concat(universe.values(), ignore_index=True)
    table = schema.conform(pa.Table.from_pandas(long_frame, preserve_index=False), "prices")
    from_arrow = compute_all(table)[0].to_pandas()
    assert from_arrow["isin"].tolist() == expected["isin"].tolist()
    for field in checked:
        pd.testing.assert_series_equal(
            from_arrow[field], expected[field], atol=1e-6, rtol=0, check_names=False
        )


def test_compute_all_uses_the_configured_benchmark_isin_by_default():
    from pipeline.config import BENCHMARK_ISIN

    universe = small_universe(n=4, seed=89)
    world = next(iter(universe.values())).copy()
    world["isin"] = BENCHMARK_ISIN
    universe[BENCHMARK_ISIN] = world

    performance, _, _ = compute_all(universe)
    table = performance.to_pandas().set_index("isin")
    assert table.loc[BENCHMARK_ISIN, "beta_vs_world"] == pytest.approx(1.0, abs=1e-6)
    assert table.loc[BENCHMARK_ISIN, "correlation_vs_world"] == pytest.approx(1.0, abs=1e-6)


# --------------------------------------------------------------------------- #
# Scale
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    not os.environ.get("ETF_BENCH"),
    reason="scale benchmark; set ETF_BENCH=1 to run (needs ~4GB and a minute)",
)
def test_benchmark_scale():
    """13,000 funds x 15 years must finish in minutes, not hours."""
    import time

    funds, dates = 13_000, business_days("2010-01-01", "2024-12-31")
    rng = np.random.default_rng(101)
    universe = {}
    for i in range(funds):
        prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.011, len(dates))))
        universe[f"IE{i:010d}"] = pd.DataFrame(
            {"isin": f"IE{i:010d}", "date": dates, "close": prices,
             "adj_close": prices, "currency": "EUR"}
        )

    started = time.perf_counter()
    performance, yearly, monthly = compute_all(universe)
    elapsed = time.perf_counter() - started

    assert performance.num_rows == funds
    assert elapsed < 600, f"compute_all took {elapsed:.0f}s"
