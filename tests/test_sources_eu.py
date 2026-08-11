"""Tests for the European source adapters.

Two layers, and the split is deliberate.

Offline tests run against small fixtures trimmed out of real downloads and pin
the *transformations* -- the CFI filter, the RMKT join, the vocabulary maps, the
primary rule, the ISIN check digit, the currency casing. Those are where the
bugs that silently corrupt a database live, and none of them need a network.

Network tests are marked and skip themselves when there is no route out. They
pin the things a fixture cannot: that the endpoints still exist and still answer
in the shape the adapters expect. Every one of these URLs is undocumented,
content-hashed or month-indexed, so "does it still resolve" is a real question
with a real expiry date -- and one that must never fail a CI run that is merely
offline.
"""

from __future__ import annotations

import json
import socket
from functools import lru_cache
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from pipeline import schema
from pipeline.sources import _http, euronext, firds, lse, six, xetra

FIXTURES = Path(__file__).parent / "fixtures"

ADAPTERS = (firds, euronext, xetra, six, lse)


@lru_cache(maxsize=1)
def online() -> bool:
    try:
        socket.create_connection(("registers.esma.europa.eu", 443), timeout=5).close()
        return True
    except OSError:
        return False


requires_network = pytest.mark.skipif(not online(), reason="no network route")


# --------------------------------------------------------------------------- #
# Fixture loading
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def markets() -> pd.DataFrame:
    return firds.parse_register((FIXTURES / "mic_sample.csv").read_bytes())


@pytest.fixture(scope="module")
def firds_result(markets: pd.DataFrame):
    with (FIXTURES / "firds_sample.xml").open("rb") as stream:
        return firds.build(firds.iter_records(stream), markets)


@pytest.fixture(scope="module")
def euronext_result():
    return euronext.build(euronext.parse((FIXTURES / "euronext_sample.csv").read_bytes()))


@pytest.fixture(scope="module")
def xetra_result():
    return xetra.build(xetra.parse((FIXTURES / "xetra_sample.csv").read_bytes()))


@pytest.fixture(scope="module")
def six_result():
    return six.build([six.parse_page((FIXTURES / "six_sample.csv").read_bytes())])


@pytest.fixture(scope="module")
def lse_result():
    return lse.build([lse.parse_page((FIXTURES / "lse_sample.json").read_bytes())])


@pytest.fixture(scope="module")
def all_results(firds_result, euronext_result, xetra_result, six_result, lse_result):
    return {
        "firds": firds_result,
        "euronext": euronext_result,
        "xetra": xetra_result,
        "six": six_result,
        "lse": lse_result,
    }


# --------------------------------------------------------------------------- #
# ISIN validation
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "isin",
    ["IE00B4L5Y983", "LU2533812058", "FR0010315770", "CH0008899764", "DE000ETFL011"],
)
def test_real_isins_pass_the_check_digit(isin: str) -> None:
    assert _http.valid_isin(isin)


@pytest.mark.parametrize(
    "isin",
    [
        "IE00B4L5Y984",  # last digit altered
        "IE00B4L5Y98",  # too short
        "ie00b4l5y983",  # lower case is not an ISIN
        "1E00B4L5Y983",  # country prefix must be letters
        "",
        None,
        12345,
    ],
)
def test_malformed_isins_are_rejected(isin: object) -> None:
    assert not _http.valid_isin(isin)


def test_every_adapter_drops_the_bad_isin_rather_than_inserting_it(all_results) -> None:
    """A synthesised or corrupt identifier must never reach a table."""
    for name, (funds, listings, stats) in all_results.items():
        assert stats["invalid_isin_dropped"] == 1, name
        assert "IE00B4L5Y984" not in set(funds["isin"]), name
        assert "IE00B4L5Y984" not in set(listings["isin"]), name


# --------------------------------------------------------------------------- #
# Schema conformance
# --------------------------------------------------------------------------- #


def test_frames_match_the_declared_schema(all_results) -> None:
    funds_columns = [field.name for field in schema.FUNDS]
    listings_columns = [field.name for field in schema.LISTINGS]

    for name, (funds, listings, _stats) in all_results.items():
        assert list(funds.columns) == funds_columns, name
        assert list(listings.columns) == listings_columns, name
        # Round-tripping through conform must be a no-op: if it is not, the
        # frame only looked schema-shaped.
        schema.conform(pa.Table.from_pandas(funds, preserve_index=False), "funds")
        schema.conform(pa.Table.from_pandas(listings, preserve_index=False), "listings")


def test_no_adapter_writes_an_isin_free_row(all_results) -> None:
    for name, (funds, listings, _stats) in all_results.items():
        assert funds["isin"].notna().all(), name
        assert listings["isin"].notna().all(), name
        assert listings["exchange_mic"].notna().all(), name


def test_eligibility_is_left_for_the_classifier(all_results) -> None:
    """`pea_eligible` and `cto_accessible` belong to a different stage entirely."""
    for name, (funds, _listings, _stats) in all_results.items():
        assert funds["pea_eligible"].isna().all(), name
        assert funds["cto_accessible"].isna().all(), name


# --------------------------------------------------------------------------- #
# Controlled vocabularies
# --------------------------------------------------------------------------- #


def test_vocabulary_columns_never_carry_a_raw_upstream_string(all_results) -> None:
    allowed = {
        "replication": set(schema.REPLICATION),
        "distribution_policy": set(schema.DISTRIBUTION_POLICY),
        "asset_class": set(schema.ASSET_CLASS),
        "strategy": set(schema.STRATEGY),
    }
    for name, (funds, _listings, _stats) in all_results.items():
        for column, vocabulary in allowed.items():
            values = set(funds[column].dropna())
            assert values <= vocabulary, f"{name}.{column} leaked {values - vocabulary}"


def test_firds_maps_the_cfi_onto_the_vocabularies(firds_result) -> None:
    funds, _listings, _stats = firds_result
    by_isin = funds.set_index("isin")

    # CEOGES: open-end, Growth -> accumulating, Equities.
    assert by_isin.loc["IE00B4L5Y983", "distribution_policy"] == "accumulating"
    assert by_isin.loc["IE00B4L5Y983", "asset_class"] == "equity"
    # CEOIES: Income -> distributing.
    assert by_isin.loc["FR0010315770", "distribution_policy"] == "distributing"
    # CECGBS: Growth, debt instruments.
    assert by_isin.loc["LU2533812058", "asset_class"] == "bond"
    # CEXXXX: the issuer declared nothing, and nothing is what we publish.
    assert by_isin.loc["DE000ETFL011", "distribution_policy"] == "unknown"
    assert by_isin.loc["DE000ETFL011", "asset_class"] == "unknown"


def test_xetra_maps_its_shelf_labels_onto_the_vocabularies(xetra_result) -> None:
    funds, _listings, _stats = xetra_result
    by_isin = funds.set_index("isin")

    assert by_isin.loc["LU2533812058", "asset_class"] == "bond"  # ...- RENTEN
    assert by_isin.loc["DE000A0S9GB0", "asset_class"] == "commodity"  # an ETC
    assert by_isin.loc["IE00BJ0KDQ92", "strategy"] == "active"  # ...- AKTIV
    # "PASSIV" says the fund tracks an index, not what it holds.
    assert by_isin.loc["IE00B4L5Y983", "asset_class"] == "unknown"
    # ETNs wrap crypto, volatility and leverage indiscriminately.
    assert by_isin.loc["CH1199067674", "asset_class"] == "unknown"


def test_euronext_reads_the_sfdr_column(euronext_result) -> None:
    funds, _listings, _stats = euronext_result
    by_isin = funds.set_index("isin")
    assert bool(by_isin.loc["FR0010315770", "esg"]) is True  # art. 8
    assert bool(by_isin.loc["LU2533812058", "esg"]) is True  # art. 9
    assert bool(by_isin.loc["IE00B4L5Y983", "esg"]) is False  # classified as neither


# --------------------------------------------------------------------------- #
# FIRDS: the CFI filter and the RMKT join
# --------------------------------------------------------------------------- #


def test_cfi_filter_selects_exactly_the_etfs(firds_result) -> None:
    funds, _listings, stats = firds_result

    assert stats["records"] == 8
    assert stats["etf_records"] == 7  # the CI record is a standard fund, not an ETF
    assert "GB00B465TP48" not in set(funds["isin"])
    assert set(funds["isin"]) == {
        "IE00B4L5Y983",
        "LU2533812058",
        "FR0010315770",
        "DE000ETFL011",
    }


def test_rmkt_filter_drops_the_mtf_rows(firds_result) -> None:
    """The join that stops the listings table being ten times too large."""
    _funds, listings, stats = firds_result

    assert set(listings["exchange_mic"]) == {"XPAR", "XMSM", "XETA"}
    assert "BTFE" not in set(listings["exchange_mic"])  # Bloomberg MTF
    assert "TWEM" not in set(listings["exchange_mic"])  # Tradeweb MTF
    assert stats["pairs_dropped_not_regulated"] == 2
    assert stats["listings"] == 4


def test_a_fund_with_only_mtf_venues_keeps_its_fund_row(firds_result) -> None:
    """It exists, it is simply not reachable on a regulated market."""
    funds, listings, _stats = firds_result
    assert "DE000ETFL011" in set(funds["isin"])
    assert "DE000ETFL011" not in set(listings["isin"])


def test_firds_leaves_the_venue_quote_currency_out_of_the_fund_row(firds_result) -> None:
    """NtnlCcy is per venue -- the same ISIN reports EUR here and USD there."""
    funds, listings, _stats = firds_result
    assert funds["fund_currency"].isna().all()
    world = listings[listings["isin"] == "IE00B4L5Y983"]
    assert set(world["trading_currency"]) == {"EUR", "USD"}


def test_firds_reads_the_domicile_off_the_isin(firds_result) -> None:
    funds, _listings, _stats = firds_result
    by_isin = funds.set_index("isin")
    assert by_isin.loc["IE00B4L5Y983", "domicile"] == "IE"
    assert by_isin.loc["LU2533812058", "domicile"] == "LU"


def test_firds_streaming_parse_reads_every_record() -> None:
    with (FIXTURES / "firds_sample.xml").open("rb") as stream:
        records = list(firds.iter_records(stream))
    assert len(records) == 8
    assert records[0]["isin"] == "IE00B4L5Y983"
    assert records[0]["mic"] == "XPAR"
    assert records[0]["issuer_lei"] == "549300MDMRB2NCJXG562"


# --------------------------------------------------------------------------- #
# is_primary
# --------------------------------------------------------------------------- #


def test_never_more_than_one_primary_listing_per_isin(all_results) -> None:
    for name, (_funds, listings, _stats) in all_results.items():
        per_isin = listings.groupby("isin")["is_primary"].sum()
        assert per_isin.max() <= 1, f"{name}: {per_isin[per_isin > 1]}"


@pytest.mark.parametrize("source", ["firds", "euronext", "six", "lse"])
def test_exactly_one_primary_listing_per_isin(all_results, source: str) -> None:
    """Xetra is excluded on purpose: it reports the primary venue rather than
    inferring one, so an ISIN primary-listed in Paris correctly has none here."""
    _funds, listings, _stats = all_results[source]
    per_isin = listings.groupby("isin")["is_primary"].sum()
    assert set(per_isin.unique()) == {1}, f"{source}: {per_isin[per_isin != 1]}"


def test_primary_prefers_the_venue_in_the_funds_own_country(firds_result) -> None:
    """IE00B4L5Y983 is on Paris and Dublin; Dublin is where it is domiciled."""
    _funds, listings, _stats = firds_result
    world = listings[listings["isin"] == "IE00B4L5Y983"].set_index("exchange_mic")
    assert bool(world.loc["XMSM", "is_primary"]) is True
    assert bool(world.loc["XPAR", "is_primary"]) is False


def test_primary_falls_back_to_the_preference_order() -> None:
    """No venue in the fund's country: the fixed order decides, not the data."""
    flags = _http.primary_flags(
        ["LU2533812058"] * 3,
        ["XETA", "XPAR", "ETFP"],
        mic_country={"XETA": "DE", "XPAR": "FR", "ETFP": "IT"},
        preference=firds.MIC_PREFERENCE,
    )
    assert flags == [False, True, False]  # XPAR is first in MIC_PREFERENCE


def test_primary_falls_back_to_alphabetical_for_unranked_venues() -> None:
    flags = _http.primary_flags(
        ["LU2533812058"] * 2,
        ["ZZZZ", "AAAA"],
        mic_country={},
        preference=(),
    )
    assert flags == [False, True]


def test_primary_does_not_depend_on_row_order(markets: pd.DataFrame) -> None:
    """The rule is a minimum over the ISIN's rows, not "first one wins"."""
    with (FIXTURES / "firds_sample.xml").open("rb") as stream:
        records = list(firds.iter_records(stream))

    forward = firds.build(iter(records), markets)[1]
    reversed_ = firds.build(iter(list(reversed(records))), markets)[1]

    key = ["isin", "exchange_mic"]
    assert (
        forward.sort_values(key).reset_index(drop=True)["is_primary"].tolist()
        == reversed_.sort_values(key).reset_index(drop=True)["is_primary"].tolist()
    )


def test_xetra_takes_the_primary_flag_from_the_venue_not_from_a_rule(
    xetra_result,
) -> None:
    _funds, listings, _stats = xetra_result
    by_isin = listings.set_index("isin")
    assert bool(by_isin.loc["IE00B4L5Y983", "is_primary"]) is True  # primary XETR
    assert bool(by_isin.loc["LU2533812058", "is_primary"]) is False  # primary XPAR


# --------------------------------------------------------------------------- #
# Currency handling
# --------------------------------------------------------------------------- #


def test_lse_preserves_the_case_of_gbp_and_gbx(lse_result) -> None:
    """GBp is pence. Upper-casing it multiplies every London price by 100."""
    _funds, listings, _stats = lse_result
    currencies = dict(zip(listings["isin"], listings["trading_currency"]))
    assert currencies["IE00B4L5Y983"] == "GBp"
    assert currencies["IE0031442068"] == "GBX"
    assert currencies["FR0010315770"] == "EUR"


def test_no_adapter_upper_cases_a_quote_currency(all_results) -> None:
    quoted = set()
    for _name, (_funds, listings, _stats) in all_results.items():
        quoted |= set(listings["trading_currency"].dropna())
    assert "GBp" in quoted
    assert quoted & {"GBX", "GBp"}


def test_six_converts_the_management_fee_to_a_fraction(six_result) -> None:
    funds, _listings, _stats = six_result
    by_isin = funds.set_index("isin")
    assert by_isin.loc["CH0008899764", "ter"] == pytest.approx(0.0035)
    assert by_isin.loc["LU2533812058", "ter"] == pytest.approx(0.0012)


# --------------------------------------------------------------------------- #
# Per-source shape handling
# --------------------------------------------------------------------------- #


def test_euronext_splits_a_multi_venue_row_into_two_listings(euronext_result) -> None:
    _funds, listings, _stats = euronext_result
    venues = listings[listings["isin"] == "LU2533812058"]
    assert set(venues["exchange_mic"]) == {"XAMS", "XPAR"}


def test_euronext_keeps_the_multi_currency_segment_as_its_own_mic(
    euronext_result,
) -> None:
    """XAMC is not XAMS: collapsing them would drop one of the two quote lines."""
    _funds, listings, _stats = euronext_result
    world = listings[listings["isin"] == "IE00B4L5Y983"].set_index("exchange_mic")
    assert set(world.index) == {"XAMS", "XAMC"}
    assert world.loc["XAMS", "trading_currency"] == "EUR"
    assert world.loc["XAMC", "trading_currency"] == "USD"


def test_euronext_drops_an_unrecognised_market_rather_than_guessing(
    euronext_result,
) -> None:
    _funds, listings, stats = euronext_result
    assert stats["unknown_market_dropped"] == 1
    assert "XS2337085422" not in set(listings["isin"])


def test_euronext_carries_borsa_italiana(euronext_result) -> None:
    """Euronext owns Milan, which is why ETFplus needs no separate scrape."""
    _funds, listings, _stats = euronext_result
    assert "ETFP" in set(listings["exchange_mic"])


def test_xetra_selects_etps_and_discards_shares(xetra_result) -> None:
    funds, _listings, stats = xetra_result
    assert "DE0007164600" not in set(funds["isin"])  # SAP, an ordinary share
    # Counted as the file presents them, before the ISIN check removes one ETF.
    assert stats["by_type"] == {"ETF": 5, "ETC": 1, "ETN": 1}
    assert len(funds) == 6


def test_xetra_treats_the_truncated_instrument_name_as_a_short_name(
    xetra_result,
) -> None:
    funds, _listings, _stats = xetra_result
    by_isin = funds.set_index("isin")
    assert by_isin.loc["IE00B4L5Y983", "name"] is None
    assert by_isin.loc["IE00B4L5Y983", "short_name"] == "ISHSIII-CORE MSCI WLD DLA"


def test_six_collapses_the_currency_lines_to_the_funds_own(six_result) -> None:
    _funds, listings, stats = six_result
    world = listings[listings["isin"] == "IE00B4L5Y983"]
    assert len(world) == 1
    assert world.iloc[0]["ticker"] == "SWDA"  # the USD line, matching FundCurrency
    assert stats["duplicate_listings_dropped"] == 1


def test_six_decodes_latin_1(six_result) -> None:
    """The payload is ISO-8859-1; read as UTF-8 the issuer names mojibake."""
    page = six.parse_page((FIXTURES / "six_sample.csv").read_bytes())
    assert "Zürcher Vermögensverwaltung AG" in set(page["IssuerNameFull"])


def test_lse_collapses_the_currency_lines_to_the_pence_one(lse_result) -> None:
    _funds, listings, stats = lse_result
    bitcoin = listings[listings["isin"] == "CH1199067674"]
    assert len(bitcoin) == 1
    assert bitcoin.iloc[0]["ticker"] == "CBTC"  # the GBX line, not the USD one
    assert stats["duplicate_listings_dropped"] == 1


def test_lse_finds_rows_by_shape_not_by_path() -> None:
    """The response is a page-layout document with an undocumented nesting."""
    payload = json.loads((FIXTURES / "lse_sample.json").read_bytes())
    assert len(list(lse.iter_rows(payload))) == 6
    assert list(lse.iter_rows({"a": {"b": []}})) == []


# --------------------------------------------------------------------------- #
# Fail-soft
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("module", ADAPTERS, ids=lambda m: m.NAME)
def test_a_dead_upstream_yields_ok_false_and_not_an_exception(module, monkeypatch) -> None:
    """Every one of these URLs will rotate, 404 or change shape one day."""

    def explode(*_args, **_kwargs):
        raise ConnectionError("simulated upstream outage")

    for attribute in ("get", "get_cached", "cached_file"):
        if hasattr(module, attribute):
            monkeypatch.setattr(module, attribute, explode)

    result = module.fetch()

    assert result.ok is False
    assert result.error and "simulated upstream outage" in result.error
    assert result.funds.empty and result.listings.empty
    assert list(result.funds.columns) == [field.name for field in schema.FUNDS]
    assert list(result.listings.columns) == [field.name for field in schema.LISTINGS]


@pytest.mark.parametrize("module", ADAPTERS, ids=lambda m: m.NAME)
def test_a_changed_upstream_shape_yields_ok_false(module, monkeypatch) -> None:
    """A 200 response full of the wrong bytes must fail as loudly as a 404."""

    def wrong_bytes(*_args, **_kwargs):
        return b"<html><body>Service temporarily unavailable</body></html>"

    monkeypatch.setattr(module, "get_cached", wrong_bytes, raising=False)
    if module is firds:
        monkeypatch.setattr(module, "latest_file", lambda *a, **k: {}, raising=False)

    result = module.fetch()
    assert result.ok is False
    assert result.funds.empty and result.listings.empty


def test_adapters_declare_the_contract() -> None:
    for module in ADAPTERS:
        assert isinstance(module.NAME, str) and module.NAME
        assert module.TRUST in {100, 80}
        assert callable(module.fetch)


# --------------------------------------------------------------------------- #
# Network: the endpoints still exist and still answer in the expected shape
# --------------------------------------------------------------------------- #


@requires_network
@pytest.mark.network
def test_firds_solr_still_publishes_a_fulins_c_file() -> None:
    document = firds.latest_file()
    assert document["file_name"].startswith("FULINS_C")
    assert document["download_link"].startswith("https://")


@requires_network
@pytest.mark.network
def test_iso_10383_register_still_carries_the_category_column() -> None:
    markets = firds.regulated_markets(refresh=True)
    assert len(markets) > 200  # ~302 regulated markets worldwide
    assert {"XPAR", "XETA", "ETFP"} <= set(markets["mic"])


@requires_network
@pytest.mark.network
def test_euronext_download_still_answers_without_a_key() -> None:
    result = euronext.fetch(refresh=True)
    assert result.ok, result.error
    assert len(result.listings) > 3000


@requires_network
@pytest.mark.network
def test_xetra_blob_url_is_still_discoverable() -> None:
    url = xetra.current_url()
    assert url.endswith("t7-xetr-allTradableInstruments.csv")
    assert "/resource/blob/" in url


@requires_network
@pytest.mark.network
def test_six_still_needs_and_honours_the_product_line_filter() -> None:
    page = next(iter(six.pages("ET", refresh=True)))
    assert len(page) == 50  # pageSize is capped at 50 whatever we ask for
    assert set(page["MarketCode"]) == {"XSWX"}


@requires_network
@pytest.mark.network
def test_lse_price_explorer_still_returns_instrument_rows() -> None:
    page = next(iter(lse.pages(refresh=True)))
    assert len(page) > 100
    assert {"isin", "tidm", "currency"} <= set(page.columns)
