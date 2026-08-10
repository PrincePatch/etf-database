"""Tests for the EUR conversion layer.

Two kinds of test live here on purpose.

Most run against a small hand-written rate table, so the alignment and
sub-unit rules are pinned exactly and the suite stays offline and deterministic.

A few run against the real ECB series, because the one error this module must
never make -- inverting the quote -- cannot be caught by a self-consistent
fixture. Multiplying instead of dividing produces a series that is smooth,
plausible and wrong, so the direction is nailed to rates the ECB actually
published on dates anyone can look up. Those tests skip, rather than fail, when
the source is unreachable: a network blip is not a code regression.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline import fx
from pipeline.config import BASE_CURRENCY


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def _rates(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=fx.RATES_COLUMNS)
    frame["date"] = pd.to_datetime(frame["date"])
    return frame


def _bars(dates: list[str], close: float = 100.0, isin: str = "IE00TEST0001") -> pd.DataFrame:
    index = pd.to_datetime(pd.Series(dates))
    return pd.DataFrame(
        {
            "isin": isin,
            "date": index,
            "close": float(close),
            "adj_close": float(close),
            "volume": 1_000.0,
            "currency": "XXX",
        }
    )


@pytest.fixture(scope="module")
def fake_rates() -> pd.DataFrame:
    """Easter 2024, when the ECB published on neither Friday nor Monday.

    Good Friday was 2024-03-29 and Easter Monday 2024-04-01; both are TARGET
    holidays, and the weekend sits between them. That is a four-day hole in the
    rate series that plenty of exchanges traded straight through.
    """
    return _rates(
        [
            ("2024-03-27", "USD", 1.0816),
            ("2024-03-28", "USD", 1.0811),
            ("2024-04-02", "USD", 1.0749),
            ("2024-04-03", "USD", 1.0783),
            ("2024-03-28", "GBP", 0.85510),
            ("2024-04-02", "GBP", 0.85510),
            ("2024-03-28", "ZAR", 20.4000),
            # A currency the ECB only started publishing partway through.
            ("2024-04-02", "ILS", 4.0000),
        ]
    )


@pytest.fixture(scope="module")
def ecb_rates() -> pd.DataFrame:
    try:
        return fx.load_rates()
    except Exception as exc:  # network down, source moved, integrity gate tripped
        pytest.skip(f"ECB reference rates unavailable: {exc}")


# --------------------------------------------------------------------------- #
# Direction of the quote -- the expensive mistake
# --------------------------------------------------------------------------- #


def test_price_is_divided_by_the_rate_not_multiplied(fake_rates):
    """ECB quotes foreign units per euro, so converting to EUR divides."""
    out = fx.to_eur(_bars(["2024-03-28"], close=108.11), "USD", fake_rates)

    assert out["close"].iloc[0] == pytest.approx(100.0)
    # 108.11 * 1.0811 == 116.86, the shape of the inverted bug.
    assert out["close"].iloc[0] != pytest.approx(116.86, abs=0.01)


def test_quote_direction_against_published_ecb_rates(ecb_rates):
    """Pinned to reference rates the ECB published on dates one can look up.

    On 2010-02-10 the euro bought 1.3740 dollars, so a fund quoted at USD 137.40
    that day was worth exactly EUR 100. On the euro's first fixing day,
    1999-01-04, the rate was 1.1789.
    """
    usd = ecb_rates[ecb_rates["currency"] == "USD"].set_index("date")["rate_to_eur"]

    assert usd.loc["2010-02-10"] == pytest.approx(1.3740)
    assert usd.loc["1999-01-04"] == pytest.approx(1.1789)

    out = fx.to_eur(_bars(["2010-02-10"], close=137.40), "USD", ecb_rates)
    assert out["close"].iloc[0] == pytest.approx(100.0, abs=1e-9)

    # A weak euro must make a dollar asset worth *more* in euros, not less.
    strong_dollar = fx.to_eur(_bars(["2015-01-15"], close=100.0), "USD", ecb_rates)
    weak_dollar = fx.to_eur(_bars(["2008-07-15"], close=100.0), "USD", ecb_rates)
    assert strong_dollar["close"].iloc[0] > weak_dollar["close"].iloc[0]


def test_a_flat_foreign_price_still_moves_in_eur(ecb_rates):
    """The whole reason the module exists: flat in USD is not flat for the holder."""
    bars = _bars(["2008-07-15", "2015-01-15"], close=100.0)
    out = fx.to_eur(bars, "USD", ecb_rates)

    assert out["close"].iloc[0] != pytest.approx(out["close"].iloc[1], rel=0.1)


# --------------------------------------------------------------------------- #
# Sub-unit quotes
# --------------------------------------------------------------------------- #


def test_pence_is_one_hundredth_of_a_pound(fake_rates):
    """GBp and GBP differ by one character's case and a factor of 100."""
    pence = fx.to_eur(_bars(["2024-03-28"], close=1000.0), "GBp", fake_rates)
    pounds = fx.to_eur(_bars(["2024-03-28"], close=1000.0), "GBP", fake_rates)

    assert pounds["close"].iloc[0] / pence["close"].iloc[0] == pytest.approx(100.0)
    assert pence["close"].iloc[0] == pytest.approx(10.0 / 0.85510)


def test_currency_normalisation_is_case_sensitive_for_pence():
    assert fx.normalise_currency("GBp") == ("GBP", 0.01)
    assert fx.normalise_currency("GBP") == ("GBP", 1.0)
    assert fx.normalise_currency("GBX") == ("GBP", 0.01)
    assert fx.normalise_currency("gbx") == ("GBP", 0.01)
    assert fx.normalise_currency(" usd ") == ("USD", 1.0)


def test_other_sub_units_are_handled(fake_rates):
    assert fx.normalise_currency("ZAc") == ("ZAR", 0.01)
    assert fx.normalise_currency("ILA") == ("ILS", 0.01)

    cents = fx.to_eur(_bars(["2024-03-28"], close=2040.0), "ZAc", fake_rates)
    assert cents["close"].iloc[0] == pytest.approx(1.0)


def test_missing_quote_currency_is_rejected():
    with pytest.raises(fx.UnsupportedCurrencyError):
        fx.normalise_currency(None)
    with pytest.raises(fx.UnsupportedCurrencyError):
        fx.normalise_currency("")


# --------------------------------------------------------------------------- #
# Aligning the two calendars
# --------------------------------------------------------------------------- #


def test_forward_fill_across_a_weekend(fake_rates):
    """Saturday and Sunday take Friday's rate; the bar is never dropped."""
    bars = _bars(["2024-03-28", "2024-03-30", "2024-03-31"], close=108.11)
    out = fx.to_eur(bars, "USD", fake_rates)

    assert len(out) == 3
    assert out["close"].notna().all()
    assert out["close"].nunique() == 1


def test_forward_fill_across_a_holiday_without_looking_ahead(fake_rates):
    """Easter Monday takes the previous Thursday's rate, not Tuesday's.

    This is the anti-interpolation assertion: filling forward from 2024-03-28
    is a rate that existed on the day; blending toward 2024-04-02 would leak
    information a holder could not have had.
    """
    out = fx.to_eur(_bars(["2024-04-01"], close=100.0), "USD", fake_rates)

    assert out["close"].iloc[0] == pytest.approx(100.0 / 1.0811)
    assert out["close"].iloc[0] != pytest.approx(100.0 / 1.0749)


def test_every_bar_survives_conversion(fake_rates):
    dates = ["2024-03-28", "2024-03-29", "2024-03-30", "2024-03-31", "2024-04-01", "2024-04-02"]
    out = fx.to_eur(_bars(dates), "USD", fake_rates)

    assert len(out) == len(dates)
    assert list(out["date"]) == list(pd.to_datetime(pd.Series(dates)))


def test_dates_before_the_first_rate_are_null_not_back_projected(fake_rates):
    """ILS starts 2024-04-02 in the fixture; the day before must be unknown."""
    out = fx.to_eur(_bars(["2024-04-01", "2024-04-02"], close=40.0), "ILS", fake_rates)

    assert pd.isna(out["close"].iloc[0])
    assert out["close"].iloc[1] == pytest.approx(10.0)


def test_ils_history_really_does_start_in_2011(ecb_rates):
    """The ECB added the shekel on 2011-01-03; 2010 bars cannot be converted."""
    out = fx.to_eur(_bars(["2010-12-30", "2011-01-03"], close=100.0), "ILS", ecb_rates)

    assert pd.isna(out["close"].iloc[0])
    assert out["close"].notna().iloc[1]


def test_a_withdrawn_quote_is_not_dragged_forward_for_years(ecb_rates):
    """RUB publication stopped on 2022-03-01 and never resumed."""
    bars = _bars(["2022-03-02", "2024-01-02"], close=100.0)
    out = fx.to_eur(bars, "RUB", ecb_rates)

    assert out["close"].notna().iloc[0]  # one day later is a normal gap
    assert pd.isna(out["close"].iloc[1])  # two years later is not


def test_euro_legacy_currency_converts_at_its_statutory_rate(ecb_rates):
    """After entry the rate is fixed by law, so it never expires.

    Slovakia adopted the euro on 2009-01-01 at 30.1260 SKK. A bar dated well
    after that still converts, at exactly that rate.
    """
    out = fx.to_eur(_bars(["2012-06-15"], close=30.1260), "SKK", ecb_rates)
    assert out["close"].iloc[0] == pytest.approx(1.0)

    # Before entry it is a market rate, and a different one.
    before = fx.to_eur(_bars(["2005-06-15"], close=30.1260), "SKK", ecb_rates)
    assert before["close"].iloc[0] != pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# Currencies the source does not carry
# --------------------------------------------------------------------------- #


def test_unsupported_currency_raises_rather_than_returning_nulls(fake_rates):
    """TWD is a real currency the ECB has never published a euro rate for."""
    with pytest.raises(fx.UnsupportedCurrencyError, match="TWD"):
        fx.to_eur(_bars(["2024-03-28"]), "TWD", fake_rates)


def test_the_ecb_really_does_not_carry_these(ecb_rates):
    available = fx.supported_currencies(ecb_rates)

    for code in ("TWD", "ARS", "AED", "SAR", "CLP"):
        assert code not in available


def test_every_currency_the_database_needs_is_covered(ecb_rates):
    """The quote currencies of the universe must all be convertible."""
    required = [
        "USD", "GBP", "CHF", "JPY", "SEK", "NOK", "DKK", "CAD", "AUD", "HKD",
        "SGD", "PLN", "CZK", "HUF", "KRW", "CNY", "MXN", "BRL", "ZAR", "ILS",
        "TRY", "NZD", "INR",
    ]
    report = fx.coverage(required, ecb_rates)

    unsupported = report.loc[~report["supported"], "currency"].tolist()
    assert unsupported == []


# --------------------------------------------------------------------------- #
# Pass-through and the batch driver
# --------------------------------------------------------------------------- #


def test_an_already_eur_series_passes_through_unchanged(fake_rates):
    bars = _bars(["2024-03-28", "2024-04-01"], close=123.45)
    bars["currency"] = BASE_CURRENCY

    out = fx.to_eur(bars, "EUR", fake_rates)

    pd.testing.assert_frame_equal(out, bars)


def test_eur_pass_through_ignores_the_rate_calendar(fake_rates):
    """A euro price needs no rate, so it must not inherit the table's 1999 start."""
    bars = _bars(["1990-01-02"], close=50.0)
    out = fx.to_eur(bars, "EUR", fake_rates)

    assert out["close"].iloc[0] == pytest.approx(50.0)
    assert out["currency"].unique().tolist() == [BASE_CURRENCY]


def test_volume_is_not_converted(fake_rates):
    """Volume is a share count, not an amount of money."""
    bars = _bars(["2024-03-28"])
    out = fx.to_eur(bars, "USD", fake_rates)

    assert out["volume"].iloc[0] == pytest.approx(bars["volume"].iloc[0])


def test_convert_all_agrees_with_to_eur(fake_rates):
    dates = ["2024-03-28", "2024-04-01", "2024-04-02"]
    prices = {"A": _bars(dates, 100.0), "B": _bars(dates, 1000.0)}
    currencies = {"A": "USD", "B": "GBp"}

    report = fx.convert_all(prices, currencies, fake_rates)

    for isin, currency in currencies.items():
        expected = fx.to_eur(prices[isin], currency, fake_rates)
        np.testing.assert_allclose(
            report.prices[isin]["close"].to_numpy(), expected["close"].to_numpy()
        )


def test_convert_all_reports_gaps_instead_of_hiding_them(fake_rates):
    prices = {
        "USD_FUND": _bars(["2024-03-28"]),
        "EUR_FUND": _bars(["2024-03-28"]),
        "TWD_FUND": _bars(["2024-03-28"]),
        "EARLY_ILS": _bars(["2024-03-28"]),
    }
    currencies = {
        "USD_FUND": "USD",
        "EUR_FUND": "EUR",
        "TWD_FUND": "TWD",
        "EARLY_ILS": "ILS",
    }

    report = fx.convert_all(prices, currencies, fake_rates)

    assert report.unsupported == {"TWD_FUND": "TWD"}
    assert "TWD_FUND" not in report.prices
    assert report.bars_without_rate == {"EARLY_ILS": 1}
    assert set(report.prices) == {"USD_FUND", "EUR_FUND", "EARLY_ILS"}


def test_convert_all_stamps_every_output_as_eur(fake_rates):
    prices = {"A": _bars(["2024-03-28"]), "B": _bars(["2024-03-28"])}
    report = fx.convert_all(prices, {"A": "USD", "B": "EUR"}, fake_rates)

    for frame in report.prices.values():
        assert frame["currency"].unique().tolist() == [BASE_CURRENCY]
        assert list(frame.columns) == list(prices["A"].columns)


def test_convert_all_preserves_python_date_objects(fake_rates):
    bars = _bars(["2024-03-28", "2024-04-01"])
    bars["date"] = bars["date"].dt.date

    report = fx.convert_all({"A": bars}, {"A": "USD"}, fake_rates)
    out = report.prices["A"]

    assert out["date"].dtype == object
    assert list(out["date"]) == list(bars["date"])


# --------------------------------------------------------------------------- #
# Source integrity
#
# These encode the failure that decided the source choice: the ECB's plain-CSV
# mirror served a stale copy carrying rows of non-rate values, one dated on a
# Sunday. Reference rates exist only on TARGET business days, so that is not an
# odd number -- it is proof the payload is not what it claims to be.
# --------------------------------------------------------------------------- #


def test_validate_rejects_rows_dated_at_a_weekend():
    corrupt = _rates([("2024-03-28", "USD", 1.0811), ("2010-02-14", "USD", 2.0)])

    with pytest.raises(ValueError, match="weekend"):
        fx._validate(corrupt)


def test_validate_rejects_non_positive_rates():
    corrupt = _rates([("2024-03-28", "USD", 0.0)])

    with pytest.raises(ValueError, match="non-positive"):
        fx._validate(corrupt)


def test_validate_rejects_an_empty_table():
    with pytest.raises(ValueError, match="empty"):
        fx._validate(_rates([]))


def test_published_rates_never_fall_at_a_weekend(ecb_rates):
    published = ecb_rates[~ecb_rates["currency"].isin(fx.EURO_LEGACY)]

    assert (published["date"].dt.dayofweek < 5).all()


def test_loaded_table_is_tidy_and_positive(ecb_rates):
    assert list(ecb_rates.columns) == fx.RATES_COLUMNS
    assert (ecb_rates["rate_to_eur"] > 0).all()
    assert not ecb_rates.duplicated(subset=["date", "currency"]).any()
    assert ecb_rates["date"].min().year == 1999


def test_cache_avoids_a_second_fetch(ecb_rates, monkeypatch):
    """A pipeline run must not re-download the history for every stage."""
    def explode(url):  # pragma: no cover - only runs if the cache is bypassed
        raise AssertionError(f"cache miss: refetched {url}")

    monkeypatch.setattr(fx, "_get", explode)
    assert len(fx.load_rates()) == len(ecb_rates)
