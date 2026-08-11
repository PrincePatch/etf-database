"""Tests for the US source adapter and the cross-source enrichment adapters.

Almost everything here runs offline against `tests/fixtures/`. The nasdaqtraded
fixture is a trimmed copy of the real file that keeps one of every shape the
parser has to survive: non-ETF rows, ETFs on all four listing exchanges, a test
issue flagged as an ETF, an ETF on an exchange code we do not map, a truncated
line, and the trailing "File Creation Time" line that is metadata rather than a
fund.

The assertion this file exists for is `test_no_adapter_emits_a_fake_isin`. 58%
of US ETFs cannot be given an ISIN from any free source, and the tempting fix --
deriving something ISIN-shaped from a ticker or a CUSIP-like fragment -- would
not fail loudly. It would merge two unrelated funds into one row and quietly
corrupt every statistic computed from them. So every key the adapters emit must
be either a real ISIN that passes its ISO 6166 check digit, or a surrogate that
cannot be mistaken for one.

The few network tests are marked and skip rather than fail when the endpoint is
unreachable: an upstream outage is not a code regression.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline import schema
from pipeline.sources import gleif, openfigi, us, yahoo_meta
from pipeline.sources._http import valid_isin

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "nasdaqtraded_sample.txt"
TICKER_ISINS = FIXTURES / "openfigi_us_ticker_isin.json"

# The fixture's ETF-flagged rows, minus the test issue and the unmapped exchange.
EXPECTED_ETFS = 12


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _directory() -> pd.DataFrame:
    return us.parse_symbol_directory(SAMPLE.read_bytes())


def _artefact() -> dict[str, str]:
    return us._isin_by_ticker(TICKER_ISINS)


def _synthetic_isins(count: int) -> list[str]:
    """`count` well-formed ISINs, for exercising batching without a network.

    Generated rather than hard-coded because the batching tests need hundreds.
    The generator is checked against the pipeline's own validator before the
    values are used, so a broken one cannot quietly weaken those tests by
    feeding them identifiers the adapter silently discards.
    """
    isins = []
    for serial in range(count):
        body = f"US{serial:09d}"
        expanded = "".join(str(int(char, 36)) for char in body)
        total = 0
        for position, char in enumerate(reversed(expanded)):
            digit = int(char)
            if position % 2 == 0:
                digit *= 2
                if digit > 9:
                    digit -= 9
            total += digit
        isins.append(f"{body}{(10 - total % 10) % 10}")

    assert all(map(valid_isin, isins))
    return isins


class _Response:
    def __init__(self, status_code: int, payload=None, headers: dict | None = None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload

    def json(self):
        return self._payload


class _FakeSession:
    """Records the size of every batch and replays a scripted status sequence."""

    def __init__(self, script=None, records=()):
        self.batches: list[int] = []
        self.headers_seen: list[dict] = []
        self.script = list(script or [])
        self.records = list(records)

    def post(self, url, json=None, headers=None, timeout=None):  # noqa: A002
        self.batches.append(len(json))
        self.headers_seen.append(headers or {})
        status, extra = self.script.pop(0) if self.script else (200, None)
        if status != 200:
            return _Response(status, None, extra)
        return _Response(200, [{"data": self.records} for _ in json])


@pytest.fixture()
def uncached_yahoo(monkeypatch):
    """Ignore the day-cache, so these tests do not depend on what a build left behind."""
    monkeypatch.setattr(yahoo_meta, "_cached_quotes", lambda region, refresh: None)
    monkeypatch.setattr(yahoo_meta, "_cache_quotes", lambda region, quotes: None)


@pytest.fixture()
def isolated_openfigi(tmp_path, monkeypatch):
    """Point the permanent OpenFIGI cache at a temporary directory."""
    monkeypatch.setattr(openfigi, "MAPPING_CACHE_PATH", tmp_path / "mapping.jsonl")
    monkeypatch.setattr(openfigi, "US_TICKER_ISIN_PATH", tmp_path / "us_ticker_isin.json")
    return tmp_path


# --------------------------------------------------------------------------- #
# Parsing the symbol directory
# --------------------------------------------------------------------------- #


def test_etf_flag_filters_on_field_six():
    """Field 6 is the ETF flag; reading another column silently changes the universe."""
    directory = _directory()
    funds, listings, stats = us.build_frames(directory)

    assert stats["etf_flagged"] == EXPECTED_ETFS + 2  # + test issue, + unmapped venue
    assert len(funds) == EXPECTED_ETFS
    assert len(listings) == EXPECTED_ETFS
    assert "Agilent Technologies, Inc. Common Stock" not in set(funds["name"])


def test_trailing_creation_time_line_is_not_a_fund():
    directory = _directory()
    funds, _, _ = us.build_frames(directory)

    assert not any(str(symbol).startswith("File Creation") for symbol in directory["Symbol"])
    assert not any("File Creation" in name for name in funds["name"])


def test_malformed_row_is_dropped_not_shifted():
    """A truncated line must lose its own row, not misalign every column after it."""
    directory = _directory()

    assert "BROKEN" not in set(directory["Symbol"])
    assert set(directory["ETF"]) <= {"Y", "N"}


def test_test_issues_and_unmapped_exchanges_are_dropped():
    funds, _, stats = us.build_frames(_directory())

    assert stats["dropped_test_issues"] == 1
    assert stats["dropped_unknown_exchange"] == 1
    assert not any("ZTEST" in key or "XXETF" in key for key in funds["isin"])


def test_listing_exchange_becomes_a_mic():
    _, listings, _ = us.build_frames(_directory())
    mics = dict(zip(listings["ticker"], listings["exchange_mic"]))

    assert mics["AAA"] == "ARCX"  # P, NYSE Arca
    assert mics["AAAA"] == "BATS"  # Z, Cboe BZX
    assert mics["AAAP"] == "XNAS"  # Q, Nasdaq
    assert mics["ACLO"] == "XNYS"  # N, NYSE


# --------------------------------------------------------------------------- #
# Identifiers
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "isin",
    ["US0378331005", "IE00B4L5Y983", "US78462F1030", "LU1681043599"],
)
def test_published_isins_pass_the_check_digit(isin):
    assert valid_isin(isin)


@pytest.mark.parametrize(
    "candidate",
    [
        "US0378331004",  # Apple's ISIN with the check digit shifted by one
        "US037833100Z",  # check digit is not a digit
        "US03783310055",  # too long
        "us0378331005",  # ISINs are upper case
        "US:ARCX:SPY",  # our own surrogate
        None,
        "",
    ],
)
def test_bad_isins_are_rejected(candidate):
    assert not valid_isin(candidate)


def test_surrogate_key_is_ticker_and_venue():
    assert us.surrogate_key("ARCX", "SPY") == "US:ARCX:SPY"
    assert us.is_surrogate("US:ARCX:SPY")
    assert not us.is_surrogate("US78462F1030")


def test_funds_without_a_mapping_take_a_surrogate_key():
    funds, listings, stats = us.build_frames(_directory())

    assert stats["isin_resolved"] == 0
    assert stats["isin_surrogate"] == EXPECTED_ETFS
    assert all(us.is_surrogate(key) for key in funds["isin"])
    assert set(funds["isin"]) == set(listings["isin"])


def test_openfigi_mapping_attaches_only_defensible_isins():
    """The artefact carries a valid pair, a bad check digit and a contested ISIN."""
    funds, _, stats = us.build_frames(_directory(), _artefact())
    keys = dict(zip(funds["name"].str.split().str[0], funds["isin"]))

    assert stats["isin_resolved"] == 2
    assert keys["Alternative"] == "US00162Q4525"  # AAA, valid
    assert keys["Columbia"] == "US00110G4082"  # AAAC, valid
    # AAAD's ISIN fails its check digit; AAAU and AAOG claim the same ISIN, and
    # keeping either would fuse two different funds into one row.
    assert us.is_surrogate(keys["PGIM"])
    assert us.is_surrogate(keys["Goldman"])
    assert us.is_surrogate(keys["Leverage"])


def test_no_adapter_emits_a_fake_isin():
    """Every key is a real ISIN or an unmistakable surrogate -- never in between."""
    funds, listings, _ = us.build_frames(_directory(), _artefact())

    for frame in (funds, listings):
        for key in frame["isin"]:
            assert valid_isin(key) or us.is_surrogate(key), key
            # The dangerous middle ground: anything ISIN-shaped must be a real
            # ISIN, because that is what every downstream join will treat it as.
            if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", key):
                assert valid_isin(key), key


# --------------------------------------------------------------------------- #
# Schema conformance
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("table", ["funds", "listings"])
def test_frames_conform_to_the_declared_schema(tmp_path, table):
    funds, listings, _ = us.build_frames(_directory(), _artefact())
    frame = funds if table == "funds" else listings

    conformed = schema.conform(pa.Table.from_pandas(frame, preserve_index=False), table)

    assert conformed.schema.equals(schema.TABLES[table])
    # Parquet enforces the non-nullable key, which is why the surrogate exists.
    pq.write_table(conformed, tmp_path / f"{table}.parquet")


def test_eligibility_columns_are_left_to_the_classifier():
    funds, _, _ = us.build_frames(_directory())
    conformed = schema.conform(pa.Table.from_pandas(funds, preserve_index=False), "funds")

    for column in ("pea_eligible", "cto_accessible"):
        assert conformed.column(column).null_count == conformed.num_rows


# --------------------------------------------------------------------------- #
# Fail-soft
# --------------------------------------------------------------------------- #


def test_upstream_failure_yields_ok_false(monkeypatch):
    def explode(*_args, **_kwargs):
        raise ConnectionError("nasdaqtrader.com is down")

    monkeypatch.setattr(us, "_download", explode)
    result = us.fetch()

    assert result.ok is False
    assert "ConnectionError" in result.error
    assert result.funds.empty and result.listings.empty


def test_a_layout_change_is_reported_not_swallowed(monkeypatch):
    monkeypatch.setattr(us, "_download", lambda *a, **k: b"Symbol|Name\nAAA|Something\n")
    result = us.fetch()

    assert result.ok is False
    assert "header" in result.error


# --------------------------------------------------------------------------- #
# OpenFIGI
# --------------------------------------------------------------------------- #


def test_batches_are_chunked_at_the_keyed_job_limit(isolated_openfigi):
    session = _FakeSession()
    client = openfigi.Client(api_key="key", session=session, sleep=lambda _: None)

    openfigi.map_isins(_synthetic_isins(250), client=client)

    assert client.tier is openfigi.KEYED
    assert session.batches == [100, 100, 50]
    assert session.headers_seen[0]["X-OPENFIGI-APIKEY"] == "key"


def test_batches_are_chunked_at_the_keyless_job_limit(isolated_openfigi):
    session = _FakeSession()
    client = openfigi.Client(api_key=None, session=session, sleep=lambda _: None)

    openfigi.map_isins(_synthetic_isins(25), client=client)

    assert client.tier is openfigi.KEYLESS
    assert session.batches == [10, 10, 5]
    assert "X-OPENFIGI-APIKEY" not in session.headers_seen[0]


def test_throttling_backs_off_and_retries(isolated_openfigi):
    slept: list[float] = []
    session = _FakeSession(script=[(429, {"Retry-After": "3"}), (200, None)])
    client = openfigi.Client(api_key="key", session=session, sleep=slept.append)

    openfigi.map_isins(_synthetic_isins(5), client=client)

    assert client.retries == 1
    assert 3.0 in slept  # the server's own Retry-After, obeyed
    assert session.batches == [5, 5]  # the same batch, sent again


def test_backoff_doubles_when_the_server_says_nothing():
    bare = _Response(429)

    assert [openfigi._backoff(bare, attempt) for attempt in (1, 2, 3)] == [2.0, 4.0, 8.0]


def test_a_rejected_key_falls_back_to_the_keyless_tier(isolated_openfigi):
    session = _FakeSession(script=[(401, None)])
    client = openfigi.Client(api_key="stale", session=session, sleep=lambda _: None)

    openfigi.map_isins(_synthetic_isins(30), client=client)

    assert client.tier is openfigi.KEYLESS
    assert client.api_key is None
    # 100 jobs attempted, refused, then re-cut into keyless batches of ten.
    assert session.batches == [30, 10, 10, 10]


def test_mappings_are_cached_permanently(isolated_openfigi):
    session = _FakeSession(records=[{"ticker": "SPY", "exchCode": "US"}])
    client = openfigi.Client(api_key="key", session=session, sleep=lambda _: None)
    isins = _synthetic_isins(5)

    first = openfigi.map_isins(isins, client=client)
    second = openfigi.map_isins(isins, client=client)

    assert first == second
    assert session.batches == [5]  # the second call asked nobody


def test_only_the_us_composite_becomes_a_ticker_mapping():
    mapped = {
        "US00162Q4525": [
            {"ticker": "AMLP", "exchCode": "US"},
            {"ticker": "AMLP", "exchCode": "UP"},
            {"ticker": "ALPS", "exchCode": "GY"},
        ],
        "US00162Q2058": [{"ticker": "BAD", "exchCode": "US"}],  # bad check digit
        "IE00B4L5Y983": [{"ticker": "IWDA", "exchCode": "NA"}],
    }

    assert openfigi.us_ticker_isins(mapped) == {"AMLP": "US00162Q4525"}


def test_a_ticker_two_isins_claim_is_dropped():
    """Deciding it arbitrarily would give one fund another fund's identity."""
    mapped = {
        "US00162Q4525": [{"ticker": "DUP", "exchCode": "US"}],
        "US00110G4082": [{"ticker": "DUP", "exchCode": "US"}],
        "US00162Q2057": [{"ticker": "OK", "exchCode": "US"}],
    }

    assert openfigi.us_ticker_isins(mapped) == {"OK": "US00162Q2057"}


def test_the_artefact_is_rebuilt_from_every_mapping_ever_cached(isolated_openfigi):
    """A run over a subset must not unresolve the funds it did not ask about."""
    session = _FakeSession(records=[{"ticker": "AAA", "exchCode": "US"}])
    client = openfigi.Client(api_key="key", session=session, sleep=lambda _: None)
    first, second = _synthetic_isins(2)

    openfigi.map_isins([first], exch_code="US", client=client)
    openfigi.map_isins([second], client=client)

    assert set(openfigi.cached_mappings()) == {first, second}


def test_venue_listings_use_mapped_mics_only():
    listings = openfigi._listings(
        {
            "IE00B4L5Y983": [
                {"ticker": "IWDA", "exchCode": "NA"},
                {"ticker": "SWDA", "exchCode": "LN"},
                {"ticker": "SPY2", "exchCode": "EU"},  # not a venue we can name
                {"ticker": "IWDA", "exchCode": "US"},  # composite, not a venue
            ]
        }
    )

    assert set(zip(listings["exchange_mic"], listings["ticker"])) == {
        ("XAMS", "IWDA"),
        ("XLON", "SWDA"),
    }


# --------------------------------------------------------------------------- #
# Enrichment adapters invent nothing
# --------------------------------------------------------------------------- #


def test_openfigi_without_a_universe_returns_nothing(isolated_openfigi):
    result = openfigi.fetch(isins=[])

    assert result.ok and result.funds.empty and result.listings.empty


def test_gleif_without_a_universe_returns_nothing():
    result = gleif.fetch(isins=[])

    assert result.ok and result.funds.empty and result.listings.empty


def test_yahoo_without_listings_returns_nothing():
    result = yahoo_meta.fetch(listings=pd.DataFrame())

    assert result.ok and result.funds.empty
    assert result.stats["note"] == "no listings to enrich"


def test_yahoo_failure_is_quiet_and_soft(uncached_yahoo):
    class _Broken:
        requests_sent = 0

        def quotes(self, region):
            raise RuntimeError("no Yahoo crumb (HTTP 429)")

    listings = pd.DataFrame(
        [{"isin": "US:ARCX:SPY", "exchange_mic": "ARCX", "ticker": "SPY", "yahoo_ticker": "SPY"}]
    )
    result = yahoo_meta.fetch(listings=listings, regions=("us",), client=_Broken())

    assert result.ok is False
    assert "crumb" in result.error
    assert result.funds.empty


def test_yahoo_keys_on_the_us_surrogate(monkeypatch, uncached_yahoo):
    class _Fixed:
        requests_sent = 1

        def quotes(self, region):
            return [
                {
                    "symbol": "SPY",
                    "exchange": "PCX",
                    "currency": "USD",
                    "netExpenseRatio": 0.0945,
                    "netAssets": 1_000_000_000.0,
                },
                {"symbol": "NOTOURS", "exchange": "PCX", "netExpenseRatio": 0.5},
            ]

    monkeypatch.setattr(yahoo_meta, "_latest_rates", lambda: {"USD": 1.1})

    listings = pd.DataFrame(
        [{"isin": "US:ARCX:SPY", "exchange_mic": "ARCX", "ticker": "SPY", "yahoo_ticker": "SPY"}]
    )
    result = yahoo_meta.fetch(listings=listings, regions=("us",), client=_Fixed())

    assert list(result.funds["isin"]) == ["US:ARCX:SPY"]
    # Yahoo quotes the expense ratio in percent; the schema stores a fraction.
    assert result.funds["ter"].iloc[0] == pytest.approx(0.000945)
    assert result.funds["aum_eur"].iloc[0] == pytest.approx(1_000_000_000.0 / 1.1)


@pytest.mark.parametrize(
    "category, expected",
    [
        ("Large Blend", ("equity", "broad-market")),
        ("Intermediate Core Bond", ("bond", "unknown")),
        ("Commodities Broad Basket", ("commodity", "unknown")),
        ("Equity Precious Metals", ("equity", "sector")),
        ("Trading--Leveraged Equity", ("equity", "leveraged")),
        ("Digital Assets", ("crypto", "unknown")),
        ("Something Nobody Has Heard Of", ("unknown", "unknown")),
    ],
)
def test_categories_map_onto_the_controlled_vocabulary(category, expected):
    asset_class, strategy = yahoo_meta.map_category(category)

    assert (asset_class, strategy) == expected
    assert asset_class in schema.ASSET_CLASS
    assert strategy in schema.STRATEGY


def test_a_missing_category_stays_null():
    """Absent is not the same statement as unclassifiable."""
    assert yahoo_meta.map_category(None) == (None, None)
    assert yahoo_meta.map_category("  ") == (None, None)


# --------------------------------------------------------------------------- #
# Network
# --------------------------------------------------------------------------- #


@pytest.mark.network
def test_live_symbol_directory_still_has_the_expected_shape():
    result = us.fetch()
    if not result.ok:
        pytest.skip(f"nasdaqtrader.com unavailable: {result.error}")

    # 5,587 ETFs on 2026-08-10; the count drifts, the order of magnitude does not.
    assert 4_000 < result.stats["etf_flagged"] < 8_000
    assert set(result.listings["exchange_mic"]) <= set(m for m, _ in us.EXCHANGE_MIC.values())
    assert all(valid_isin(k) or us.is_surrogate(k) for k in result.funds["isin"])


@pytest.mark.network
def test_live_openfigi_maps_a_known_isin_to_its_us_ticker(isolated_openfigi):
    try:
        mapped = openfigi.map_isins(["US78462F1030"], exch_code="US")
    except Exception as exc:  # rate limited, endpoint moved
        pytest.skip(f"OpenFIGI unavailable: {exc}")

    assert openfigi.us_ticker_isins(mapped) == {"SPY": "US78462F1030"}
