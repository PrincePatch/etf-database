"""Tests for the total-return reconstruction.

Three layers, deliberately.

The first is arithmetic. Each hand-built series is constructed so the event
produces the mechanical price drop it is supposed to produce, which makes the
adjusted series checkable two independent ways: against the back-adjustment
formula, and against the cash a holder would actually have ended up with after
reinvesting the distribution. Both must give the same number, and both are round
enough to verify by eye.

The second is invariants -- an accumulating fund's adjusted series must equal
its close *bit for bit*, not within a tolerance, and no event outside a fund's
price window may touch it.

The third is a regression against real vendor data trimmed into
`tests/fixtures/`. That layer exists because this module's whole justification
is empirical: it must agree with the vendor where the vendor is right (IUSA.AS)
and disagree where it is wrong (ISF.L, where Yahoo publishes the dividends and
applies none of them). A self-consistent fixture can never prove that.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pipeline import adjust

FIXTURES = Path(__file__).parent / "fixtures"


# --------------------------------------------------------------------------- #
# Builders
# --------------------------------------------------------------------------- #


def bars(closes: list[float], start: str = "2024-01-01", isin: str = "IE00TEST0001") -> pd.DataFrame:
    """A fund's daily bars on consecutive business days."""
    dates = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame(
        {
            "isin": isin,
            "date": dates,
            "open": closes,
            "high": closes,
            "low": closes,
            "close": [float(c) for c in closes],
            "volume": 1_000.0,
            "currency": "EUR",
        }
    )


def events(rows: list[tuple[str, str, float]], isin: str = "IE00TEST0001") -> pd.DataFrame:
    """Rows are (iso date, kind, value); value is an amount or a split ratio."""
    return pd.DataFrame(
        [
            {
                "isin": isin,
                "date": pd.Timestamp(day),
                "kind": kind,
                "amount": value if kind == "dividend" else np.nan,
                "ratio": value if kind == "split" else np.nan,
                "currency": "EUR",
            }
            for day, kind, value in rows
        ]
    )


def no_events(isin: str = "IE00TEST0001") -> pd.DataFrame:
    return events([], isin)


# --------------------------------------------------------------------------- #
# The arithmetic proof
#
# Four business days from 2024-01-01: Mon 01, Tue 02, Wed 03, Thu 04.
# --------------------------------------------------------------------------- #


def test_dividend_back_adjustment_is_exact():
    """100 -> 110, then 5.5 goes ex and the price drops to 104.5 and stays.

    f = (110 - 5.5) / 110 = 0.95, applied to every bar before the ex-date.
    The holder's story: one share bought at 100 is worth 110 on day two; on the
    ex-date the price drops to 104.5 and 5.5 in cash arrives, which buys
    5.5/104.5 = 0.0526316 more shares, so 1.0526316 shares x 104.5 = 110.00.
    +10% total return against a +4.5% price return, and the adjusted series has
    to say exactly that.
    """
    prices = bars([100.0, 110.0, 104.5, 104.5])
    actions = events([("2024-01-03", "dividend", 5.5)])

    adjusted = adjust.total_return_series(prices, actions)

    assert adjusted.tolist() == pytest.approx([95.0, 104.5, 104.5, 104.5])
    # Total return, computed two ways that must agree.
    assert adjusted.iloc[-1] / adjusted.iloc[0] == pytest.approx(1.10)
    assert prices["close"].iloc[-1] / prices["close"].iloc[0] == pytest.approx(1.045)


def test_split_back_adjustment_is_exact():
    """A 2:1 split halves the quote; the holder owns twice as many shares.

    Only meaningful with `close_is_split_adjusted=False`, i.e. a source that
    prints the tape unretouched. 100 -> 120, split, 60 -> 66: one share bought
    at 100 becomes two worth 66 = 132, so +32%.
    """
    prices = bars([100.0, 120.0, 60.0, 66.0])
    actions = events([("2024-01-03", "split", 2.0)])

    adjusted = adjust.total_return_series(prices, actions, close_is_split_adjusted=False)

    assert adjusted.tolist() == pytest.approx([50.0, 60.0, 60.0, 66.0])
    assert adjusted.iloc[-1] / adjusted.iloc[0] == pytest.approx(1.32)


def test_split_and_dividend_on_the_same_date_compose():
    """A 2:1 split and a 6.00 distribution on one ex-date.

    f = (1/2) x (120 - 6)/120 = 0.475, and the ex-date close is
    (120 - 6)/2 = 57, which is what the tape would print. Holder: one share at
    100 becomes two at 57 = 114, plus 6 cash reinvested at 57 = 0.105263 shares,
    so 2.105263 shares x 60 on the last day = 126.32, i.e. +26.3158%.
    """
    prices = bars([100.0, 120.0, 57.0, 60.0])
    actions = events(
        [("2024-01-03", "split", 2.0), ("2024-01-03", "dividend", 6.0)]
    )

    adjusted = adjust.total_return_series(prices, actions, close_is_split_adjusted=False)

    assert adjusted.tolist() == pytest.approx([47.5, 57.0, 57.0, 60.0])
    assert adjusted.iloc[-1] / adjusted.iloc[0] == pytest.approx(1.2631578947)


def test_same_date_events_are_order_independent():
    prices = bars([100.0, 120.0, 57.0, 60.0])
    forward = events([("2024-01-03", "split", 2.0), ("2024-01-03", "dividend", 6.0)])
    reversed_rows = forward.iloc[::-1].reset_index(drop=True)

    a = adjust.total_return_series(prices, forward, close_is_split_adjusted=False)
    b = adjust.total_return_series(prices, reversed_rows, close_is_split_adjusted=False)

    assert a.tolist() == b.tolist()


def test_two_dividends_compound_rather_than_add():
    prices = bars([100.0, 100.0, 100.0, 100.0, 100.0])
    actions = events(
        [("2024-01-03", "dividend", 10.0), ("2024-01-05", "dividend", 10.0)]
    )

    adjusted = adjust.total_return_series(prices, actions)

    # Both factors are 0.9; the first bar sees the product, not the sum.
    assert adjusted.iloc[0] == pytest.approx(81.0)
    assert adjusted.iloc[2] == pytest.approx(90.0)
    assert adjusted.iloc[-1] == pytest.approx(100.0)


def test_splits_are_not_reapplied_under_the_yahoo_convention():
    """The default asserts the source already divided the history by the ratio.

    Getting this wrong halves fifteen years of prices a second time, and the
    resulting chart still looks entirely plausible.
    """
    prices = bars([50.0, 60.0, 60.0, 66.0])
    actions = events([("2024-01-03", "split", 2.0)])

    default = adjust.total_return_series(prices, actions)
    assert default.tolist() == pytest.approx(prices["close"].tolist())

    raw_source = adjust.total_return_series(prices, actions, close_is_split_adjusted=False)
    assert raw_source.iloc[0] == pytest.approx(25.0)


# --------------------------------------------------------------------------- #
# Invariants
# --------------------------------------------------------------------------- #


def test_accumulating_fund_is_bit_identical_to_close():
    """An accumulating ETF distributes nothing, so adj_close must *be* close.

    Exact equality, not `approx`: half the universe accumulates, and a series
    that is off by one ulp everywhere is a silent sign that a no-op adjustment
    is not actually a no-op.
    """
    prices = bars([100.0, 101.5, 99.25, 103.125, 107.0])

    adjusted = adjust.total_return_series(prices, no_events())

    assert list(adjusted) == list(prices["close"])
    assert (adjusted.to_numpy() == prices["close"].to_numpy()).all()


def test_no_actions_argument_at_all_is_the_identity():
    prices = bars([100.0, 101.5, 99.25])
    assert list(adjust.total_return_series(prices)) == list(prices["close"])
    assert list(adjust.total_return_series(prices, None)) == list(prices["close"])


def test_last_bar_always_equals_its_close():
    """The series is quoted in today's money, so the most recent bar is untouched."""
    prices = bars([100.0, 110.0, 104.5, 104.5])
    actions = events([("2024-01-03", "dividend", 5.5)])

    adjusted = adjust.total_return_series(prices, actions)

    assert adjusted.iloc[-1] == prices["close"].iloc[-1]


def test_dividend_before_the_first_bar_is_ignored():
    """No earlier bar exists to scale, so there is nothing the event can mean."""
    prices = bars([100.0, 110.0, 120.0], start="2024-02-01")
    actions = events([("2024-01-15", "dividend", 5.0)])

    adjusted = adjust.total_return_series(prices, actions)

    assert list(adjusted) == list(prices["close"])
    assert adjusted.attrs["adjustment"].skipped == {"outside the fund's price window": 1}


def test_dividend_on_the_first_bar_is_ignored():
    prices = bars([100.0, 110.0, 120.0])
    actions = events([("2024-01-01", "dividend", 5.0)])

    adjusted = adjust.total_return_series(prices, actions)

    assert list(adjusted) == list(prices["close"])


def test_announced_future_dividend_does_not_rebase_the_history():
    """Vendors publish upcoming ex-dates; unreceived cash must not move anything."""
    prices = bars([100.0, 110.0, 120.0])
    actions = events([("2025-06-01", "dividend", 5.0)])

    adjusted = adjust.total_return_series(prices, actions)

    assert list(adjusted) == list(prices["close"])
    assert adjusted.attrs["adjustment"].applied == 0


def test_distribution_larger_than_the_price_is_skipped():
    """A non-positive factor would annihilate or sign-flip every earlier bar."""
    prices = bars([100.0, 10.0, 5.0, 5.0])
    actions = events([("2024-01-03", "dividend", 25.0)])

    adjusted = adjust.total_return_series(prices, actions)

    assert np.isfinite(adjusted).all()
    assert (adjusted > 0).all()
    assert list(adjusted) == list(prices["close"])
    assert adjusted.attrs["adjustment"].skipped == {"non-positive adjustment factor": 1}


def test_zero_reference_close_is_skipped_not_divided_by():
    prices = bars([100.0, 0.0, 50.0, 50.0])
    actions = events([("2024-01-03", "dividend", 1.0)])

    adjusted = adjust.total_return_series(prices, actions)

    # The zero bar has no usable price of its own, but the event still resolves
    # against the last real quote before it rather than dividing by zero.
    assert np.isfinite(adjusted[adjusted.index != 1]).all()
    assert not np.isinf(adjusted).any()
    assert adjusted.iloc[0] == pytest.approx(100.0 * 0.99)


def test_negative_price_never_produces_a_negative_factor():
    prices = bars([100.0, -5.0, 50.0, 50.0])
    actions = events([("2024-01-03", "dividend", 1.0)])

    adjusted = adjust.total_return_series(prices, actions)

    assert not np.isnan(adjusted.iloc[0])
    assert adjusted.iloc[0] > 0


def test_nan_close_falls_back_to_the_last_real_quote():
    """A halted session must not make the following dividend unadjustable."""
    prices = bars([100.0, 100.0, np.nan, 99.0, 99.0])
    actions = events([("2024-01-04", "dividend", 1.0)])

    adjusted = adjust.total_return_series(prices, actions)

    # Reference is the last priced session (100.0), so f = 0.99.
    assert adjusted.iloc[0] == pytest.approx(99.0)
    assert np.isnan(adjusted.iloc[2])
    assert adjusted.iloc[-1] == pytest.approx(99.0)


def test_index_is_preserved_for_unsorted_and_relabelled_input():
    prices = bars([100.0, 110.0, 104.5, 104.5])
    actions = events([("2024-01-03", "dividend", 5.5)])
    shuffled = prices.iloc[[3, 0, 2, 1]].copy()
    shuffled.index = ["d", "a", "c", "b"]

    adjusted = adjust.total_return_series(shuffled, actions)

    assert list(adjusted.index) == ["d", "a", "c", "b"]
    assert adjusted.loc["a"] == pytest.approx(95.0)
    assert adjusted.loc["d"] == pytest.approx(104.5)


def test_duplicate_index_labels_do_not_scramble_the_result():
    prices = bars([100.0, 110.0, 104.5, 104.5])
    prices.index = [0, 0, 0, 0]
    actions = events([("2024-01-03", "dividend", 5.5)])

    adjusted = adjust.total_return_series(prices, actions)

    assert adjusted.to_numpy().tolist() == pytest.approx([95.0, 104.5, 104.5, 104.5])


def test_multi_fund_frame_is_rejected_by_the_single_fund_helper():
    frame = pd.concat([bars([1.0, 2.0], isin="A"), bars([1.0, 2.0], isin="B")])
    with pytest.raises(ValueError, match="one fund"):
        adjust.total_return_series(frame)


def test_empty_input():
    empty = bars([]).iloc[0:0]
    assert adjust.total_return_series(empty).empty
    assert adjust.apply_adjustment(empty).empty


# --------------------------------------------------------------------------- #
# Universe-wide application
# --------------------------------------------------------------------------- #


def test_apply_adjustment_isolates_funds():
    """Adding a second fund must not shift the first one by a single ulp.

    The kernel is one global cumulative sum over every fund at once, so this is
    the test that the per-fund telescoping is actually per-fund.
    """
    distributing = bars([100.0, 110.0, 104.5, 104.5], isin="IE00DIST0001")
    accumulating = bars([200.0, 210.0, 220.0, 230.0], isin="IE00ACCU0001")
    universe = pd.concat([distributing, accumulating], ignore_index=True)
    actions = events([("2024-01-03", "dividend", 5.5)], isin="IE00DIST0001")

    result = adjust.apply_adjustment(universe, actions)

    solo = adjust.apply_adjustment(distributing, actions)
    assert (
        result.loc[result["isin"] == "IE00DIST0001", "adj_close"].to_numpy()
        == solo["adj_close"].to_numpy()
    ).all()
    accu = result.loc[result["isin"] == "IE00ACCU0001"]
    assert (accu["adj_close"].to_numpy() == accu["close"].to_numpy()).all()
    assert result.attrs["adjustment"].funds == 2


def test_apply_adjustment_preserves_row_order():
    universe = pd.concat(
        [bars([100.0, 110.0, 104.5], isin="ZZ00LAST0001"),
         bars([10.0, 11.0, 12.0], isin="AA00FIRST001")],
        ignore_index=True,
    )
    result = adjust.apply_adjustment(universe, no_events())

    assert list(result["isin"]) == list(universe["isin"])
    assert list(result["date"]) == list(universe["date"])


def test_apply_adjustment_recomputes_an_existing_column():
    """A stored adj_close is never trusted -- it is the column that goes stale."""
    prices = bars([100.0, 110.0, 104.5, 104.5])
    prices["adj_close"] = 1.0
    actions = events([("2024-01-03", "dividend", 5.5)])

    result = adjust.apply_adjustment(prices, actions)

    assert result["adj_close"].iloc[0] == pytest.approx(95.0)


def test_events_for_an_unknown_fund_are_counted_not_applied():
    prices = bars([100.0, 110.0, 120.0], isin="IE00KNOWN001")
    actions = events([("2024-01-02", "dividend", 5.0)], isin="IE00GHOST001")

    result = adjust.apply_adjustment(prices, actions)

    assert (result["adj_close"].to_numpy() == result["close"].to_numpy()).all()
    assert result.attrs["adjustment"].skipped == {"fund not in price panel": 1}


def test_missing_required_column_is_an_error():
    with pytest.raises(ValueError, match="missing required columns"):
        adjust.apply_adjustment(pd.DataFrame({"isin": ["A"], "date": [pd.Timestamp("2024-01-01")]}))


def test_scale_invariance_against_a_reference_loop():
    """Cross-check the vectorised kernel against a transparent per-bar loop."""
    rng = np.random.default_rng(20260810)
    closes = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, 250)))
    prices = bars(list(closes))
    dates = prices["date"]
    picks = [30, 90, 150, 210]
    actions = events(
        [(dates.iloc[i].strftime("%Y-%m-%d"), "dividend", 0.4) for i in picks]
    )

    adjusted = adjust.total_return_series(prices, actions).to_numpy()

    expected = closes.astype("float64").copy()
    for i in picks:
        factor = (closes[i - 1] - 0.4) / closes[i - 1]
        expected[:i] *= factor
    assert adjusted == pytest.approx(expected, rel=1e-12)


# --------------------------------------------------------------------------- #
# Regression against real vendor data
#
# Fixed 2020-08-03 .. 2025-08-01 window, trimmed from Yahoo's chart endpoint so
# the expectations below never move. `vendor_adj_close` is Yahoo's own adjusted
# close, kept only so these two tests can compare against it -- the pipeline
# itself never requests that column.
# --------------------------------------------------------------------------- #


def load_fixture(name: str, isin: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    prices = pd.read_csv(FIXTURES / f"{name}_prices.csv", parse_dates=["date"])
    actions = pd.read_csv(FIXTURES / f"{name}_actions.csv", parse_dates=["date"])
    prices.insert(0, "isin", isin)
    actions.insert(0, "isin", isin)
    return prices, actions


def annualised(series: pd.Series, dates: pd.Series) -> float:
    years = (dates.iloc[-1] - dates.iloc[0]).days / 365.25
    return (series.iloc[-1] / series.iloc[0]) ** (1 / years) - 1


def test_fixtures_are_present_and_daily():
    for name in ("ISF_L", "IUSA_AS"):
        prices, _ = load_fixture(name, "X")
        assert len(prices) > 1_200
        gaps = prices["date"].diff().dt.days.dropna()
        assert gaps.median() == 1.0, "fixture is not a daily series"


def test_iusa_as_reproduces_the_vendor_exactly():
    """Where Yahoo adjusts correctly, we must land on its number, not near it.

    This is the test that says the formula, the reference-close choice (previous
    session, not ex-date) and the event alignment are all right. Compared as a
    growth path normalised at the first bar, because the vendor's series is
    scaled to a bar outside this window.
    """
    prices, actions = load_fixture("IUSA_AS", "IE0031442068")

    ours = adjust.total_return_series(prices, actions)

    theirs = prices["vendor_adj_close"]
    deviation = (ours / ours.iloc[0]) / (theirs / theirs.iloc[0]) - 1
    assert deviation.abs().max() < 1e-6

    assert annualised(ours, prices["date"]) == pytest.approx(
        annualised(theirs, prices["date"]), abs=1e-6
    )
    # ... and it is genuinely a total-return correction, not a copy of close.
    assert annualised(ours, prices["date"]) - annualised(
        prices["close"], prices["date"]
    ) == pytest.approx(0.0142, abs=0.0005)


def test_isf_l_dividends_are_recovered_where_the_vendor_drops_them():
    """Yahoo publishes ISF.L's dividends and applies none of them.

    Its adjusted close moves 0.04pp/yr away from the raw price over five years
    of ~3%/yr distributions, which is the signature of no adjustment at all. Our
    reconstruction has to put those 4pp/yr back, and the same events on the same
    fund's Amsterdam line (above) prove the arithmetic is not simply inflating
    everything.
    """
    prices, actions = load_fixture("ISF_L", "IE0005042456")
    assert (actions["kind"] == "dividend").sum() == 20

    ours = adjust.total_return_series(prices, actions)

    price_cagr = annualised(prices["close"], prices["date"])
    vendor_cagr = annualised(prices["vendor_adj_close"], prices["date"])
    our_cagr = annualised(ours, prices["date"])

    # The vendor's "adjustment" is indistinguishable from none at all.
    assert abs(vendor_cagr - price_cagr) < 0.001
    # Ours recovers the distributions: >4pp/yr, and above the vendor's figure.
    assert our_cagr - price_cagr > 0.035
    assert our_cagr - vendor_cagr > 0.035
    assert our_cagr == pytest.approx(0.1246, abs=0.001)

    assert ours.iloc[-1] == prices["close"].iloc[-1]
    assert (ours <= prices["close"]).all()


def test_both_fixtures_apply_every_event_they_carry():
    for name, isin in (("ISF_L", "IE0005042456"), ("IUSA_AS", "IE0031442068")):
        prices, actions = load_fixture(name, isin)
        series = adjust.total_return_series(prices, actions)
        report = series.attrs["adjustment"]
        assert report.applied == len(actions), f"{name}: {report.skipped}"
