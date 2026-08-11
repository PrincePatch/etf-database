"""Tests for the price fetcher and its on-disk store.

The transport is injectable, so almost everything here runs offline against a
fake Yahoo that honours `period1`/`period2` the way the real one does. That is
what lets the incremental-refresh tests be end-to-end rather than mocked at the
seam that matters.

Two tests do need the network, and they are the two that cannot be faked: the
live endpoint has to be shown to return *daily* bars, and `range=max` has to be
shown to silently return monthly ones. A fixture asserting our own payload
builder returns what our own parser expects would prove nothing about the trap
that made this module's parameter choice necessary. They skip, rather than fail,
when the network is unavailable.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from pipeline import adjust, prices, schema


# --------------------------------------------------------------------------- #
# A fake Yahoo
# --------------------------------------------------------------------------- #


def session_epoch(day: str | pd.Timestamp, zone: str = "Europe/Paris", hour: int = 9) -> int:
    """Epoch seconds of a session open, the way Yahoo stamps a daily bar."""
    stamp = pd.Timestamp(day).normalize() + pd.Timedelta(hours=hour)
    return int(stamp.tz_localize(zone).timestamp())


def chart_payload(
    symbol: str,
    days: list[str] | pd.DatetimeIndex,
    closes: list[float],
    *,
    dividends: list[tuple[str, float]] | None = None,
    splits: list[tuple[str, float, float]] | None = None,
    currency: str = "EUR",
    zone: str = "Europe/Paris",
) -> bytes:
    stamps = [session_epoch(d, zone) for d in days]
    quote = {
        "open": [None if c is None else c * 0.99 for c in closes],
        "high": [None if c is None else c * 1.01 for c in closes],
        "low": [None if c is None else c * 0.98 for c in closes],
        "close": list(closes),
        "volume": [None if c is None else 1000 for c in closes],
    }
    events: dict[str, dict] = {}
    for day, amount in dividends or []:
        stamp = session_epoch(day, zone)
        events.setdefault("dividends", {})[str(stamp)] = {"amount": amount, "date": stamp}
    for day, numerator, denominator in splits or []:
        stamp = session_epoch(day, zone)
        events.setdefault("splits", {})[str(stamp)] = {
            "date": stamp,
            "numerator": numerator,
            "denominator": denominator,
            "splitRatio": f"{numerator:g}:{denominator:g}",
        }
    return json.dumps(
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": symbol,
                            "currency": currency,
                            "exchangeTimezoneName": zone,
                            "gmtoffset": 3600,
                            "instrumentType": "ETF",
                        },
                        "timestamp": stamps,
                        "events": events,
                        "indicators": {"quote": [quote]},
                    }
                ],
                "error": None,
            }
        }
    ).encode()


ERROR_PAYLOAD = json.dumps(
    {"chart": {"result": None, "error": {"code": "Not Found",
                                         "description": "No data found, symbol may be delisted"}}}
).encode()


class FakeMarket:
    """A stand-in Yahoo that slices its series on `period1`/`period2`.

    Honouring the window is the point: an incremental refresh that quietly asks
    for the full history would still pass a test whose fake ignored the params.
    """

    def __init__(self) -> None:
        self.series: dict[str, dict] = {}
        self.calls: list[tuple[str, dict]] = []
        self.concurrent = 0
        self.peak_concurrent = 0
        self._lock = threading.Lock()

    def add(self, symbol: str, start: str, closes: list[float], **kwargs) -> "FakeMarket":
        self.series[symbol] = {
            "days": pd.bdate_range(start, periods=len(closes)),
            "closes": closes,
            **kwargs,
        }
        return self

    def __call__(self, symbol: str, params) -> bytes:
        with self._lock:
            self.calls.append((symbol, dict(params)))
            self.concurrent += 1
            self.peak_concurrent = max(self.peak_concurrent, self.concurrent)
        try:
            time.sleep(0.001)  # long enough for concurrency to actually overlap
            if symbol not in self.series:
                raise prices.FetchError("HTTP 404", status=404)
            spec = dict(self.series[symbol])
            days, closes = spec.pop("days"), spec.pop("closes")
            lo = pd.Timestamp(params["period1"], unit="s", tz="UTC").tz_convert(None)
            hi = pd.Timestamp(params["period2"], unit="s", tz="UTC").tz_convert(None)
            keep = [i for i, d in enumerate(days) if lo - pd.Timedelta(days=1) <= d <= hi]
            if not keep:
                raise prices.FetchError("payload carries no timestamps")
            window = {days[i].date().isoformat() for i in keep}
            spec["dividends"] = [e for e in spec.get("dividends", []) if e[0] in window]
            spec["splits"] = [e for e in spec.get("splits", []) if e[0] in window]
            return chart_payload(
                symbol, [days[i] for i in keep], [closes[i] for i in keep], **spec
            )
        finally:
            with self._lock:
                self.concurrent -= 1

    def params_for(self, symbol: str) -> list[dict]:
        return [p for s, p in self.calls if s == symbol]


# --------------------------------------------------------------------------- #
# Request construction -- the monthly-bar trap
# --------------------------------------------------------------------------- #


def test_chart_params_never_emit_range():
    """`interval=1d` is ignored when `range` is present and the response silently
    degrades to monthly bars. The parameter must not appear at any value."""
    for params in (prices._chart_params(), prices._chart_params(date(2020, 1, 1))):
        assert "range" not in params
        assert params["interval"] == "1d"
        assert "period1" in params and "period2" in params


def test_chart_params_full_history_starts_at_the_epoch():
    params = prices._chart_params()
    assert params["period1"] == 0
    assert params["period2"] > time.time() - 60


def test_chart_params_window_includes_both_endpoints():
    params = prices._chart_params(date(2024, 3, 1), date(2024, 3, 5))
    assert params["period1"] == 1709251200  # 2024-03-01T00:00:00Z
    # period2 must clear the last session's open, not sit on its midnight.
    assert params["period2"] >= 1709596800 + 86400  # 2024-03-05T00:00:00Z + 1d


def test_events_are_requested():
    assert "div" in prices._chart_params()["events"]
    assert "splits" in prices._chart_params()["events"]


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #


def test_parse_chart_returns_bars_events_and_currency():
    payload = chart_payload(
        "AAA.PA", ["2024-01-02", "2024-01-03", "2024-01-04"], [10.0, 11.0, 12.0],
        dividends=[("2024-01-04", 0.5)], splits=[("2024-01-03", 2.0, 1.0)],
        currency="GBp",
    )
    bars, events, currency, gains = prices.parse_chart(payload)

    assert list(bars["close"]) == [10.0, 11.0, 12.0]
    assert list(bars["date"].dt.strftime("%Y-%m-%d")) == ["2024-01-02", "2024-01-03", "2024-01-04"]
    assert currency == "GBp"  # sub-unit code preserved verbatim for fx.py
    assert gains == 0
    assert set(events["kind"]) == {"dividend", "split"}
    assert events.loc[events["kind"] == "split", "ratio"].iloc[0] == 2.0
    assert events.loc[events["kind"] == "dividend", "amount"].iloc[0] == 0.5


def test_bar_dates_use_the_exchange_calendar_not_utc():
    """A New York session opens at 13:30 or 14:30 UTC depending on the season;
    both must land on the local trading day."""
    payload = chart_payload(
        "SPY", ["2024-01-03", "2024-07-03"], [1.0, 2.0], zone="America/New_York",
    )
    bars, _, _, _ = prices.parse_chart(payload)
    assert list(bars["date"].dt.strftime("%Y-%m-%d")) == ["2024-01-03", "2024-07-03"]


def test_null_closes_are_dropped():
    payload = chart_payload(
        "AAA.PA", ["2024-01-02", "2024-01-03", "2024-01-04"], [10.0, None, 12.0]
    )
    bars, _, _, _ = prices.parse_chart(payload)
    assert len(bars) == 2
    assert bars["close"].notna().all()


def test_truncated_quote_arrays_do_not_raise():
    payload = json.loads(chart_payload("AAA.PA", ["2024-01-02", "2024-01-03"], [1.0, 2.0]))
    payload["chart"]["result"][0]["indicators"]["quote"][0]["volume"] = [7]
    bars, _, _, _ = prices.parse_chart(payload)
    assert len(bars) == 2
    assert np.isnan(bars["volume"].iloc[1])


@pytest.mark.parametrize(
    "payload",
    [
        b"{not json at all",
        b"",
        b"<html>Too Many Requests</html>",
        ERROR_PAYLOAD,
        json.dumps({"chart": {"result": [], "error": None}}).encode(),
        json.dumps({"chart": {"result": [{"meta": {}}], "error": None}}).encode(),
        json.dumps({"finance": {"error": "x"}}).encode(),
    ],
)
def test_malformed_payloads_raise_fetcherror_not_something_else(payload):
    with pytest.raises(prices.FetchError):
        prices.parse_chart(payload)


# --------------------------------------------------------------------------- #
# Per-ticker isolation
# --------------------------------------------------------------------------- #


def test_fetch_one_never_raises_on_a_404():
    result = prices.fetch_one("NOPE.XX", transport=FakeMarket(), retries=1)
    assert result.ok is False
    assert result.status == 404
    assert "404" in result.error


def test_fetch_one_does_not_retry_a_404():
    attempts = []

    def transport(symbol, params):
        attempts.append(symbol)
        raise prices.FetchError("HTTP 404", status=404)

    prices.fetch_one("NOPE.XX", transport=transport, retries=4)
    assert len(attempts) == 1, "a delisted symbol must not be retried"


def test_fetch_one_retries_a_429_then_gives_up_cleanly():
    attempts = []

    def transport(symbol, params):
        attempts.append(symbol)
        raise prices.FetchError("HTTP 429", status=429)

    result = prices.fetch_one("AAA.PA", transport=transport, retries=3)
    assert len(attempts) == 3
    assert result.ok is False and result.status == 429


def test_fetch_one_recovers_when_a_retry_succeeds():
    state = {"n": 0}

    def transport(symbol, params):
        state["n"] += 1
        if state["n"] == 1:
            raise prices.FetchError("HTTP 503", status=503)
        return chart_payload(symbol, ["2024-01-02"], [1.0])

    result = prices.fetch_one("AAA.PA", transport=transport, retries=3)
    assert result.ok and result.attempts == 2


def test_fetch_one_does_not_retry_a_malformed_body():
    attempts = []

    def transport(symbol, params):
        attempts.append(symbol)
        return b"{not json"

    result = prices.fetch_one("AAA.PA", transport=transport, retries=4)
    assert result.ok is False
    assert len(attempts) == 1


def test_transport_exceptions_are_contained():
    def transport(symbol, params):
        raise RuntimeError("socket exploded")

    result = prices.fetch_one("AAA.PA", transport=transport, retries=1)
    assert result.ok is False and "RuntimeError" in result.error


# --------------------------------------------------------------------------- #
# Batch behaviour
# --------------------------------------------------------------------------- #


def mixed_market() -> FakeMarket:
    market = FakeMarket()
    market.add("GOOD1.PA", "2024-01-01", [10.0, 11.0, 12.0, 13.0])
    market.add("GOOD2.DE", "2024-01-01", [20.0, 21.0, 22.0, 23.0],
               dividends=[("2024-01-03", 1.0)])
    return market


def test_one_bad_ticker_does_not_kill_the_batch():
    market = mixed_market()
    frame, actions = prices.fetch_history(
        ["GOOD1.PA", "DEAD.XX", "GOOD2.DE"], transport=market, progress=False, retries=1
    )
    report = frame.attrs["report"]

    assert report.requested == 3 and report.succeeded == 2
    assert set(report.failed) == {"DEAD.XX"}
    assert set(frame["ticker"]) == {"GOOD1.PA", "GOOD2.DE"}
    assert len(frame) == 8
    assert len(actions) == 1


def test_a_429_storm_degrades_to_skipped_tickers():
    """A gate change must cost us the batch's rows, never an exception."""
    def transport(symbol, params):
        if symbol == "GOOD1.PA":
            return chart_payload(symbol, ["2024-01-02"], [1.0])
        raise prices.FetchError("HTTP 429", status=429)

    frame, _ = prices.fetch_history(
        ["GOOD1.PA", "A.PA", "B.PA"], transport=transport, progress=False, retries=1
    )
    report = frame.attrs["report"]

    assert report.succeeded == 1
    assert report.status_counts[429] == 2
    assert report.success_rate == pytest.approx(1 / 3)


def test_every_ticker_failing_returns_typed_empty_frames():
    frame, actions = prices.fetch_history(
        ["A.XX", "B.XX"], transport=FakeMarket(), progress=False, retries=1
    )
    assert frame.empty and actions.empty
    assert list(frame.columns) == prices.PRICE_COLUMNS
    assert list(actions.columns) == prices.ACTION_COLUMNS
    assert prices._to_arrow(frame, "prices").schema.equals(schema.PRICES)


def test_isin_mapping_keys_the_output():
    market = mixed_market()
    frame, actions = prices.fetch_history(
        {"IE00AAAAAAA1": "GOOD1.PA", "IE00BBBBBBB2": "GOOD2.DE"},
        transport=market, progress=False,
    )
    assert set(frame["isin"]) == {"IE00AAAAAAA1", "IE00BBBBBBB2"}
    assert set(actions["isin"]) == {"IE00BBBBBBB2"}


def test_two_funds_sharing_a_listing_are_fetched_once():
    market = mixed_market()
    frame, _ = prices.fetch_history(
        {"IE00AAAAAAA1": "GOOD1.PA", "IE00BBBBBBB2": "GOOD1.PA"},
        transport=market, progress=False,
    )
    assert len(market.calls) == 1
    assert set(frame["isin"]) == {"IE00AAAAAAA1", "IE00BBBBBBB2"}


def test_concurrency_is_capped_at_the_measured_optimum():
    """Past the measured knee more workers are slower *and* louder upstream."""
    market = FakeMarket()
    for i in range(120):
        market.add(f"T{i}.PA", "2024-01-01", [1.0, 2.0])

    prices.fetch_history(
        list(market.series), workers=500, transport=market, progress=False
    )
    assert market.peak_concurrent <= prices.MAX_WORKERS


def test_empty_ticker_list_is_not_an_error():
    frame, actions = prices.fetch_history([], progress=False)
    assert frame.empty and actions.empty
    assert frame.attrs["report"].requested == 0


def test_null_tickers_are_dropped():
    market = mixed_market()
    frame, _ = prices.fetch_history(
        {"IE00AAAAAAA1": "GOOD1.PA", "IE00NONE00001": None, "IE00NAN000001": np.nan},
        transport=market, progress=False,
    )
    assert set(frame["isin"]) == {"IE00AAAAAAA1"}


def test_fetched_frame_conforms_to_the_declared_schema():
    market = mixed_market()
    frame, actions = prices.fetch_history(
        {"IE00AAAAAAA1": "GOOD1.PA", "IE00BBBBBBB2": "GOOD2.DE"},
        transport=market, progress=False,
    )
    table = prices._to_arrow(frame, "prices")
    assert table.schema.equals(schema.PRICES)
    assert prices._to_arrow(actions, "corporate_actions").schema.equals(schema.CORPORATE_ACTIONS)


# --------------------------------------------------------------------------- #
# Store layout
# --------------------------------------------------------------------------- #


def test_bucket_is_stable_across_processes():
    """Python salts str hashing per process; a store sharded with `hash()` would
    be unreadable tomorrow. These values are pinned deliberately."""
    assert prices.bucket_of("IE00B4L5Y983") == prices.bucket_of("IE00B4L5Y983")
    assert 0 <= prices.bucket_of("IE00B4L5Y983") < prices.BUCKETS
    known = {isin: prices.bucket_of(isin) for isin in ("IE00B4L5Y983", "IE0031442068")}
    assert known == {"IE00B4L5Y983": 9, "IE0031442068": 41}


def test_buckets_spread_the_universe():
    used = {prices.bucket_of(f"IE00{i:08d}") for i in range(2_000)}
    assert len(used) == prices.BUCKETS


def test_write_then_read_roundtrip(tmp_path):
    market = mixed_market()
    frame, _ = prices.fetch_history(
        {"IE00AAAAAAA1": "GOOD1.PA", "IE00BBBBBBB2": "GOOD2.DE"},
        transport=market, progress=False,
    )
    prices.write_prices(frame, tmp_path)

    back = prices.read_prices(tmp_path)
    assert len(back) == len(frame)
    assert set(back["isin"]) == {"IE00AAAAAAA1", "IE00BBBBBBB2"}
    assert list(back.columns) == [f.name for f in schema.PRICES]


def test_per_fund_read_touches_one_bucket(tmp_path):
    market = FakeMarket()
    mapping = {}
    for i in range(40):
        symbol = f"T{i}.PA"
        market.add(symbol, "2024-01-01", [1.0, 2.0, 3.0])
        mapping[f"IE00{i:08d}"] = symbol
    frame, _ = prices.fetch_history(mapping, transport=market, progress=False)
    prices.write_prices(frame, tmp_path)

    assert len(list(tmp_path.glob("bucket=*/part.parquet"))) > 1
    one = prices.read_prices(tmp_path, isins=["IE0000000005"])
    assert set(one["isin"]) == {"IE0000000005"}
    assert len(one) == 3


def test_write_is_idempotent(tmp_path):
    market = mixed_market()
    frame, _ = prices.fetch_history(
        {"IE00AAAAAAA1": "GOOD1.PA"}, transport=market, progress=False
    )
    prices.write_prices(frame, tmp_path)
    first = prices.read_prices(tmp_path)
    prices.write_prices(frame, tmp_path)
    second = prices.read_prices(tmp_path)

    pd.testing.assert_frame_equal(first, second)


def test_a_restated_bar_replaces_rather_than_duplicates(tmp_path):
    original = pd.DataFrame(
        {"isin": ["IE00AAAAAAA1"] * 2, "ticker": ["A.PA"] * 2,
         "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
         "open": [1.0, 2.0], "high": [1.0, 2.0], "low": [1.0, 2.0],
         "close": [1.0, 2.0], "volume": [10.0, 10.0], "currency": ["EUR"] * 2}
    )
    prices.write_prices(original, tmp_path)

    settled = original.tail(1).copy()
    settled["close"] = 2.5  # the provisional intraday bar, now final
    prices.write_prices(settled, tmp_path)

    back = prices.read_prices(tmp_path)
    assert len(back) == 2
    assert back["close"].iloc[-1] == pytest.approx(2.5)


def test_store_index_tracks_each_fund_span(tmp_path):
    market = mixed_market()
    frame, _ = prices.fetch_history(
        {"IE00AAAAAAA1": "GOOD1.PA", "IE00BBBBBBB2": "GOOD2.DE"},
        transport=market, progress=False,
    )
    prices.write_prices(frame, tmp_path)

    index = prices.store_index(tmp_path).set_index("isin")
    assert index.loc["IE00AAAAAAA1", "bars"] == 4
    assert set(index["ticker"]) == {"GOOD1.PA", "GOOD2.DE"}
    assert prices.last_dates(tmp_path)["IE00AAAAAAA1"] == date(2024, 1, 4)


def exportable_store(tmp_path):
    market = FakeMarket()
    mapping = {}
    for i in range(20):
        symbol = f"T{i}.PA"
        market.add(symbol, "2024-01-01", [10.0, 20.0, 30.0],
                   dividends=[("2024-01-02", 1.0)] if i % 2 else [])
        mapping[f"IE00{i:08d}"] = symbol
    frame, actions = prices.fetch_history(mapping, transport=market, progress=False)
    prices.write_prices(frame, tmp_path / "store")
    prices.write_actions(actions, tmp_path / "actions.parquet")
    return tmp_path / "store", tmp_path / "actions.parquet"


def test_export_single_file_coalesces_and_fills_total_return(tmp_path):
    """The store keeps adj_close null; the published artefact must not."""
    store, actions = exportable_store(tmp_path)

    dest = prices.export_single_file(store, tmp_path / "prices.parquet", actions=actions)
    coalesced = pd.read_parquet(dest)

    assert len(coalesced) == 60
    assert list(coalesced.columns) == [f.name for f in schema.PRICES]
    assert coalesced["adj_close"].notna().all()
    assert prices.read_prices(store)["adj_close"].isna().all()

    # Distributing funds are back-adjusted, accumulating ones are untouched.
    # 1.00 goes ex on the second bar, so f = (10 - 1)/10 and only bar one moves.
    distributing = coalesced[coalesced["isin"] == "IE0000000001"]
    assert distributing["adj_close"].tolist() == pytest.approx([9.0, 20.0, 30.0])
    accumulating = coalesced[coalesced["isin"] == "IE0000000000"]
    assert (accumulating["adj_close"].to_numpy() == accumulating["close"].to_numpy()).all()


def test_export_refuses_to_publish_a_price_return_series_as_total_return(tmp_path):
    store, _ = exportable_store(tmp_path)
    with pytest.raises(FileNotFoundError, match="corporate actions"):
        prices.export_single_file(store, tmp_path / "out.parquet", actions=tmp_path / "gone.parquet")

    raw = prices.export_single_file(
        store, tmp_path / "raw.parquet", actions=tmp_path / "gone.parquet", total_return=False
    )
    assert pd.read_parquet(raw)["adj_close"].isna().all()


def test_actions_are_written_without_duplicates(tmp_path):
    path = tmp_path / "corporate_actions.parquet"
    actions = pd.DataFrame(
        {"isin": ["IE00AAAAAAA1"] * 2, "ticker": ["A.PA"] * 2,
         "date": pd.to_datetime(["2024-01-03", "2024-01-03"]),
         "kind": ["dividend", "split"], "amount": [1.0, np.nan],
         "ratio": [np.nan, 2.0], "currency": ["EUR"] * 2}
    )
    prices.write_actions(actions, path)
    prices.write_actions(actions, path)

    stored = pd.read_parquet(path)
    assert len(stored) == 2  # one dividend and one split on the same day survive
    assert set(stored["kind"]) == {"dividend", "split"}


# --------------------------------------------------------------------------- #
# Incremental refresh
# --------------------------------------------------------------------------- #


def seeded_store(tmp_path, closes: int = 10):
    market = FakeMarket()
    market.add("GOOD1.PA", "2024-01-01", [float(i) for i in range(1, closes + 1)])
    market.add("GOOD2.DE", "2024-01-01", [float(i) * 2 for i in range(1, closes + 1)],
               dividends=[("2024-01-03", 1.0)])
    mapping = {"IE00AAAAAAA1": "GOOD1.PA", "IE00BBBBBBB2": "GOOD2.DE"}

    frame, actions = prices.fetch_history(mapping, transport=market, progress=False)
    prices.write_prices(frame, tmp_path / "store")
    prices.write_actions(actions, tmp_path / "actions.parquet")
    return market, mapping


def test_refresh_requests_only_the_tail(tmp_path):
    market, mapping = seeded_store(tmp_path)
    market.calls.clear()

    prices.refresh(
        tmp_path / "store", mapping, transport=market, progress=False,
        actions_path=tmp_path / "actions.parquet",
    )

    stored_last = prices.last_dates(tmp_path / "store")["IE00AAAAAAA1"]
    requested = pd.Timestamp(market.params_for("GOOD1.PA")[0]["period1"], unit="s")
    expected = pd.Timestamp(stored_last) - timedelta(days=prices.REFRESH_OVERLAP_DAYS)
    assert requested.normalize() == expected.normalize()
    assert market.params_for("GOOD1.PA")[0]["period1"] > 0, "must not refetch history"


def test_refresh_fetches_a_new_fund_from_inception(tmp_path):
    market, mapping = seeded_store(tmp_path)
    market.add("NEW.MI", "2024-01-01", [5.0, 6.0, 7.0])
    market.calls.clear()

    frame, _ = prices.refresh(
        tmp_path / "store", {**mapping, "IE00CCCCCCC3": "NEW.MI"},
        transport=market, progress=False, actions_path=tmp_path / "actions.parquet",
    )

    assert market.params_for("NEW.MI")[0]["period1"] == 0
    assert len(frame[frame["isin"] == "IE00CCCCCCC3"]) == 3


def test_refresh_is_idempotent(tmp_path):
    """Two runs against an unchanged upstream must leave an unchanged store."""
    market, mapping = seeded_store(tmp_path)
    store, actions_path = tmp_path / "store", tmp_path / "actions.parquet"

    for _ in range(2):
        new_prices, new_actions = prices.refresh(
            store, mapping, transport=market, progress=False, actions_path=actions_path
        )
        prices.write_prices(new_prices, store)
        prices.write_actions(new_actions, actions_path)

    after_two = prices.read_prices(store)
    stored_actions = pd.read_parquet(actions_path)

    assert len(after_two) == 20, "refresh duplicated rows"
    assert len(stored_actions) == 1
    for isin, expected in (("IE00AAAAAAA1", 1.0), ("IE00BBBBBBB2", 2.0)):
        fund = after_two[after_two["isin"] == isin]
        assert fund["date"].is_monotonic_increasing
        assert not fund["date"].duplicated().any()
        assert fund["close"].tolist() == pytest.approx(
            [expected * i for i in range(1, 11)]
        ), "an existing bar was rewritten"

    # And a third pass changes nothing at all, byte for byte.
    third_prices, _ = prices.refresh(
        store, mapping, transport=market, progress=False, actions_path=actions_path
    )
    prices.write_prices(third_prices, store)
    pd.testing.assert_frame_equal(after_two, prices.read_prices(store))


def test_refresh_appends_genuinely_new_bars(tmp_path):
    market, mapping = seeded_store(tmp_path, closes=10)
    store = tmp_path / "store"
    market.add("GOOD1.PA", "2024-01-01", [float(i) for i in range(1, 14)])

    new_prices, _ = prices.refresh(
        store, mapping, transport=market, progress=False,
        actions_path=tmp_path / "actions.parquet",
    )
    prices.write_prices(new_prices, store)

    good1 = prices.read_prices(store, isins=["IE00AAAAAAA1"])
    assert len(good1) == 13
    assert good1["close"].iloc[-1] == pytest.approx(13.0)


def test_refresh_recovers_the_full_history_after_a_split(tmp_path):
    """Yahoo restates the whole series on a split, so an append would splice
    post-split bars onto pre-split ones and invent an overnight 2x return."""
    market, mapping = seeded_store(tmp_path, closes=10)
    store = tmp_path / "store"

    # Same fund, whole history halved, plus a split event in the refresh window.
    last_day = pd.bdate_range("2024-01-01", periods=12)[-1].date().isoformat()
    market.add("GOOD1.PA", "2024-01-01", [i / 2 for i in range(1, 13)],
               splits=[(last_day, 2.0, 1.0)])

    new_prices, new_actions = prices.refresh(
        store, mapping, transport=market, progress=False,
        actions_path=tmp_path / "actions.parquet",
    )

    refetched = new_prices[new_prices["ticker"] == "GOOD1.PA"]
    assert len(refetched) == 12, "the whole history should have been refetched"
    assert refetched["close"].iloc[0] == pytest.approx(0.5)
    assert (new_actions["kind"] == "split").sum() == 1

    prices.write_prices(new_prices, store)
    stored = prices.read_prices(store, isins=["IE00AAAAAAA1"])
    assert stored["close"].iloc[0] == pytest.approx(0.5), "pre-split units survived"


def test_refresh_without_a_ticker_list_uses_the_store_manifest(tmp_path):
    market, _ = seeded_store(tmp_path)
    market.calls.clear()

    prices.refresh(
        tmp_path / "store", transport=market, progress=False,
        actions_path=tmp_path / "actions.parquet",
    )
    assert {symbol for symbol, _ in market.calls} == {"GOOD1.PA", "GOOD2.DE"}


def test_refresh_on_an_empty_store_is_a_cold_backfill(tmp_path):
    market = mixed_market()
    frame, _ = prices.refresh(
        tmp_path / "empty", {"IE00AAAAAAA1": "GOOD1.PA"},
        transport=market, progress=False, actions_path=tmp_path / "none.parquet",
    )
    assert market.params_for("GOOD1.PA")[0]["period1"] == 0
    assert len(frame) == 4


def test_refresh_output_feeds_the_adjuster(tmp_path):
    """The two modules meet here: raw bars in, adj_close out, nothing stored."""
    market, mapping = seeded_store(tmp_path)
    frame, actions = prices.refresh(
        tmp_path / "store", mapping, transport=market, progress=False,
        actions_path=tmp_path / "actions.parquet",
    )
    stored = prices.read_prices(tmp_path / "store")
    stored_actions = pd.read_parquet(tmp_path / "actions.parquet")

    adjusted = adjust.apply_adjustment(stored, stored_actions)

    assert adjusted["adj_close"].notna().all()
    accumulating = adjusted[adjusted["isin"] == "IE00AAAAAAA1"]
    assert (accumulating["adj_close"].to_numpy() == accumulating["close"].to_numpy()).all()
    distributing = adjusted[adjusted["isin"] == "IE00BBBBBBB2"]
    assert (distributing["adj_close"].iloc[:1] < distributing["close"].iloc[:1]).all()


# --------------------------------------------------------------------------- #
# Live endpoint
#
# The only two facts a fake cannot establish.
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def live():
    try:
        result = prices.fetch_one("SPY", date.today() - timedelta(days=7), retries=1)
    except Exception as exc:  # no network, no DNS, no curl_cffi
        pytest.skip(f"Yahoo unreachable: {exc}")
    if not result.ok:
        pytest.skip(f"Yahoo unreachable: {result.error}")
    return True


@pytest.mark.network
def test_live_endpoint_returns_daily_bars(live):
    """The whole reason `_chart_params` exists. Monthly bars would sail through
    every offline test in this file and quietly wreck every long-window statistic.
    """
    result = prices.fetch_one("SPY")
    assert result.ok, result.error

    bars = result.bars
    assert len(bars) > 8_000, "SPY has ~8,400 daily bars since 1993"
    gaps = bars["date"].diff().dt.days.dropna()
    assert gaps.median() == 1.0
    assert (gaps <= 5).mean() > 0.99, "spacing is not a daily calendar"
    assert bars["date"].iloc[0].year == 1993
    assert result.currency == "USD"


@pytest.mark.network
def test_range_max_silently_returns_monthly_bars(live):
    """Pin the trap itself, so nobody 'simplifies' `_chart_params` back into it."""
    trap = dict(prices._chart_params())
    trap.pop("period1"), trap.pop("period2")
    trap["range"] = "max"

    payload = prices.http_transport("SPY", trap)
    monthly, _, _, _ = prices.parse_chart(payload)
    daily = prices.fetch_one("SPY").bars

    assert len(monthly) < 600, "the trap has closed; re-check the recon"
    assert len(daily) > 8_000
    assert len(daily) > 10 * len(monthly)
    assert monthly["date"].diff().dt.days.dropna().median() > 25


@pytest.mark.network
def test_live_batch_reports_a_plausible_success_rate(live):
    universe = ["SPY", "IWDA.AS", "EUNL.DE", "CW8.PA", "ISF.L", "NOT-A-TICKER.XX"]
    frame, actions = prices.fetch_history(
        universe, date.today() - timedelta(days=30), workers=6, progress=False
    )
    report = frame.attrs["report"]

    assert report.succeeded >= 5
    assert set(report.failed) == {"NOT-A-TICKER.XX"}
    assert frame["currency"].notna().all()
    assert set(frame["ticker"]) <= set(universe)
