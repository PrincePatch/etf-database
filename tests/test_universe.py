"""Tests for the merge -- the stage where an identity error becomes permanent.

Everything here runs offline against hand-built `SourceResult`s, because the
merge is a pure function of what the adapters returned and the questions worth
asking are about disagreement, not about download. Four of them matter more than
the rest:

* `test_lower_trust_never_overwrites` -- the rule the whole trust ladder exists
  for. Yahoo (TRUST 40) supplying an AUM must not displace a venue-published one.
* `test_output_is_independent_of_adapter_order` -- every permutation of the same
  six sources must produce a byte-identical table. Adapters run in dependency
  order, that order changes when one of them fails, and a published value that
  moved because of it would be indefensible.
* `test_no_two_adapters_emit_different_mics_for_one_venue` -- the hazard the
  adapter authors flagged. Borsa Italiana's ETF market is `ETFP` to Euronext and
  FIRDS and `XMIL` to OpenFIGI and Yahoo; unreconciled, one venue becomes two and
  the same listing is stored twice.
* `test_surrogate_and_isin_keys_cannot_collide` -- 3,377 US rows carry the
  `US:{MIC}:{ticker}` surrogate, and the day one of those is mistaken for an ISIN
  is the day two unrelated funds become one row.

The MIC register used below is a hand-written extract of ISO 10383 rather than a
fetch, so these run in CI with no network and no cache. The network-marked test
at the end checks the same alias map against the live register, which is what
catches ISO retiring a code.
"""

from __future__ import annotations

import itertools
import random
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from pipeline import schema, universe
from pipeline.sources import REGISTRY, SourceResult, euronext, firds, lse, openfigi, six
from pipeline.sources import us, xetra, yahoo_meta
from pipeline.sources._http import to_frame

# --------------------------------------------------------------------------- #
# A hand-written extract of ISO 10383, covering every venue any adapter names.
#
# Verbatim `MIC` / `OPERATING MIC` / category from the register downloaded on
# 2026-08-11. Inline rather than a fixture file so the assertions below and the
# data they rest on can be read together.
# --------------------------------------------------------------------------- #

REGISTER = """MIC,OPERATING MIC,OPRT/SGMT,MARKET NAME-INSTITUTION DESCRIPTION,MARKET CATEGORY CODE,ISO COUNTRY CODE (ISO 3166),STATUS
XPAR,XPAR,OPRT,EURONEXT - EURONEXT PARIS,RMKT,FR,ACTIVE
XPMC,XPAR,SGMT,EURONEXT PARIS - MULTI-CURRENCY TRADING,RMKT,FR,ACTIVE
XAMS,XAMS,OPRT,EURONEXT - EURONEXT AMSTERDAM,RMKT,NL,ACTIVE
XAMC,XAMS,SGMT,EURONEXT AMSTERDAM - MULTI-CURRENCY TRADING,RMKT,NL,ACTIVE
XBRU,XBRU,OPRT,EURONEXT - EURONEXT BRUSSELS,RMKT,BE,ACTIVE
XLIS,XLIS,OPRT,EURONEXT - EURONEXT LISBON,RMKT,PT,ACTIVE
XDUB,XDUB,OPRT,IRISH STOCK EXCHANGE - ALL MARKET,NSPD,IE,ACTIVE
XMSM,XDUB,SGMT,EURONEXT DUBLIN,RMKT,IE,ACTIVE
XMIL,XMIL,OPRT,BORSA ITALIANA S.P.A.,NSPD,IT,ACTIVE
ETFP,XMIL,SGMT,ELECTRONIC ETF ETC/ETN AND OPEN-END FUNDS MARKET,RMKT,IT,ACTIVE
MTAA,XMIL,SGMT,EURONEXT MILAN,RMKT,IT,ACTIVE
MOTX,XMIL,SGMT,ELECTRONIC BOND MARKET,RMKT,IT,ACTIVE
SEDX,XMIL,SGMT,SECURITISED DERIVATIVES MARKET,MLTF,IT,ACTIVE
XETR,XETR,OPRT,XETRA,NSPD,DE,ACTIVE
XETA,XETR,SGMT,XETRA - REGULIERTER MARKT,RMKT,DE,ACTIVE
XETU,XETR,SGMT,XETRA - REGULIERTERMARKT - OFF-BOOK,RMKT,DE,ACTIVE
XFRA,XFRA,OPRT,DEUTSCHE BOERSE AG,NSPD,DE,ACTIVE
FRAA,XFRA,SGMT,BOERSE FRANKFURT - REGULIERTER MARKT,RMKT,DE,ACTIVE
FRAU,XFRA,SGMT,BOERSE FRANKFURT - REGULIERTERMARKT - OFF-BOOK,RMKT,DE,ACTIVE
XLON,XLON,OPRT,LONDON STOCK EXCHANGE,RMKT,GB,ACTIVE
XSWX,XSWX,OPRT,SIX SWISS EXCHANGE,RMKT,CH,ACTIVE
XSTO,XSTO,OPRT,NASDAQ STOCKHOLM AB,RMKT,SE,ACTIVE
XCSE,XCSE,OPRT,NASDAQ COPENHAGEN A/S,RMKT,DK,ACTIVE
XOSL,XOSL,OPRT,OSLO BORS,RMKT,NO,ACTIVE
BMEX,BMEX,OPRT,BME - BOLSAS Y MERCADOS ESPANOLES,NSPD,ES,ACTIVE
XMAD,BMEX,SGMT,BOLSA DE MADRID,RMKT,ES,ACTIVE
XWBO,XWBO,OPRT,WIENER BOERSE AG,NSPD,AT,ACTIVE
XHKG,XHKG,OPRT,HONG KONG EXCHANGES AND CLEARING LTD,NSPD,HK,ACTIVE
XTSE,XTSE,OPRT,TORONTO STOCK EXCHANGE,NSPD,CA,ACTIVE
XASX,XASX,OPRT,ASX - ALL MARKETS,NSPD,AU,ACTIVE
XNYS,XNYS,OPRT,NEW YORK STOCK EXCHANGE INC.,NSPD,US,ACTIVE
ARCX,XNYS,SGMT,NYSE ARCA,RMKT,US,ACTIVE
XASE,XNYS,SGMT,NYSE AMERICAN LLC,RMKT,US,ACTIVE
XNAS,XNAS,OPRT,NASDAQ - ALL MARKETS,RMKT,US,ACTIVE
XCBO,XCBO,OPRT,CBOE GLOBAL MARKETS INC.,NSPD,US,ACTIVE
BATS,XCBO,SGMT,CBOE BZX U.S. EQUITIES EXCHANGE,NSPD,US,ACTIVE
IEXG,IEXG,OPRT,INVESTORS EXCHANGE,NSPD,US,ACTIVE
"""

# Real ISINs, all checksum-valid; `BAD_ISIN` is `IE00B4L5Y983` with its check
# digit changed, which is exactly the shape a transposition typo produces.
WORLD = "IE00B4L5Y983"
SP500 = "IE00B5BMR087"
AMUNDI = "LU1681043599"
BAD_ISIN = "IE00B4L5Y984"
SURROGATE = "US:ARCX:VTI"

TRUST = {
    "firds": 100,
    "gleif": 100,
    "euronext": 80,
    "xetra": 80,
    "lse": 80,
    "six": 80,
    "nasdaqtrader": 80,
    "openfigi": 40,
    "yahoo": 40,
}


@pytest.fixture(scope="module")
def registry() -> universe.MicRegistry:
    return universe.MicRegistry.from_csv(REGISTER)


def source(name, funds=None, listings=None, *, ok=True, error=None) -> SourceResult:
    """A SourceResult from plain dicts, conformed like a real adapter's output."""
    return SourceResult(
        name=name,
        funds=to_frame(funds or [], "funds"),
        listings=to_frame(listings or [], "listings"),
        ok=ok,
        error=error,
    )


def merged(results, registry, **kwargs) -> universe.MergeResult:
    return universe.merge(results, registry=registry, trust=TRUST, **kwargs)


def value(frame: pd.DataFrame, key: str, column: str):
    row = frame[frame["isin"] == key]
    assert len(row) == 1, f"{key} appears {len(row)} times"
    return row.iloc[0][column]


# --------------------------------------------------------------------------- #
# The registry
# --------------------------------------------------------------------------- #


def test_registry_is_populated_at_import_time():
    """`sources/__init__` says universe.py fills REGISTRY; this is that promise."""
    assert set(REGISTRY) >= {"firds", "euronext", "xetra", "lse", "six", "nasdaqtrader"}
    for name, module in REGISTRY.items():
        assert module.NAME == name
        assert isinstance(module.TRUST, int)
        assert callable(module.fetch)


def test_declared_trust_matches_the_documented_ladder():
    levels = universe.trust_levels()
    assert levels["firds"] == 100
    assert levels["euronext"] == 80
    assert levels["yahoo"] == 40
    assert levels["openfigi"] == 40


def test_adapters_are_selectable_by_module_name_too():
    """`sources/us.py` publishes itself as "nasdaqtrader"; both names must work.

    Naming the file is not a mistake, and `--sources us` quietly running nothing
    is a nasty way to discover the difference.
    """
    by_name = universe.select_adapters(["nasdaqtrader", "yahoo"])
    by_module = universe.select_adapters(["us", "yahoo_meta"])
    assert [a.name for a in by_name] == [a.name for a in by_module] == ["nasdaqtrader", "yahoo"]


def test_an_unknown_adapter_name_fails_loudly():
    """A typo must not silently produce a plausible dataset missing a source."""
    with pytest.raises(KeyError, match="euronxt"):
        universe.select_adapters(["firds", "euronxt"])


def test_a_module_that_cannot_be_imported_is_skipped_not_fatal():
    adapters = universe.register_adapters(("firds", "no_such_adapter"))
    assert "firds" in adapters
    assert len(adapters) == 1


# --------------------------------------------------------------------------- #
# Trust precedence
# --------------------------------------------------------------------------- #


def test_higher_trust_wins_a_contested_field(registry):
    result = merged(
        [
            source("euronext", [{"isin": WORLD, "name": "Euronext's name"}]),
            source("firds", [{"isin": WORLD, "name": "the register's name"}]),
        ],
        registry,
    )
    assert value(result.funds, WORLD, "name") == "the register's name"
    assert result.source_of(WORLD, "name") == "firds"
    assert result.report.contested_fields["name"] == 1


def test_lower_trust_never_overwrites(registry):
    """The rule the trust ladder exists for, stated as its own assertion.

    Yahoo's `netAssets` is the least reliable number in the pipeline. Where a
    venue has published one, Yahoo must not be able to touch it -- whatever order
    the adapters happen to run in, and whatever Yahoo says.
    """
    venue_aum = 8.0e10
    for order in itertools.permutations(
        [
            source("yahoo", [{"isin": WORLD, "aum_eur": 1.0e9, "ter": 0.0020}]),
            source("six", [{"isin": WORLD, "aum_eur": venue_aum}]),
        ]
    ):
        result = merged(list(order), registry)
        assert value(result.funds, WORLD, "aum_eur") == pytest.approx(venue_aum)
        assert result.source_of(WORLD, "aum_eur") == "six"
        # ...while still taking the field the venue did not supply.
        assert value(result.funds, WORLD, "ter") == pytest.approx(0.0020)
        assert result.source_of(WORLD, "ter") == "yahoo"


def test_a_null_from_a_higher_trust_source_does_not_block_a_lower_one(registry):
    """Null is "not established", not "established as nothing"."""
    result = merged(
        [
            source("firds", [{"isin": WORLD, "name": "the register's name", "ter": None}]),
            source("six", [{"isin": WORLD, "ter": 0.0020}]),
        ],
        registry,
    )
    assert value(result.funds, WORLD, "ter") == pytest.approx(0.0020)
    assert value(result.funds, WORLD, "name") == "the register's name"


def test_false_is_a_value_and_is_not_skipped(registry):
    """`ucits = False` must survive the merge; only null defers to a weaker source."""
    result = merged(
        [
            source("nasdaqtrader", [{"isin": SURROGATE, "ucits": False}]),
            source("yahoo", [{"isin": SURROGATE, "ucits": True}]),
        ],
        registry,
    )
    assert bool(value(result.funds, SURROGATE, "ucits")) is False


def test_a_source_with_no_declared_trust_ranks_lowest(registry, caplog):
    result = universe.merge(
        [
            source("mystery", [{"isin": WORLD, "name": "unknown provenance"}]),
            source("yahoo", [{"isin": WORLD, "name": "Yahoo's name"}]),
        ],
        registry=registry,
        trust=TRUST,
    )
    assert value(result.funds, WORLD, "name") == "Yahoo's name"


# --------------------------------------------------------------------------- #
# Determinism
# --------------------------------------------------------------------------- #


def _six_sources():
    today = date(2026, 8, 11)
    return [
        source(
            "firds",
            [{"isin": WORLD, "name": "iShares Core MSCI World", "domicile": "IE",
              "last_updated": today}],
            [{"isin": WORLD, "exchange_mic": "ETFP", "trading_currency": "EUR"},
             {"isin": WORLD, "exchange_mic": "XETA", "trading_currency": "EUR"}],
        ),
        source(
            "euronext",
            [{"isin": WORLD, "short_name": "IWDA", "esg": True}],
            [{"isin": WORLD, "exchange_mic": "XAMS", "ticker": "IWDA", "trading_currency": "EUR"}],
        ),
        source(
            "xetra",
            [{"isin": WORLD, "asset_class": "equity"}],
            [{"isin": WORLD, "exchange_mic": "XETR", "ticker": "EUNL", "trading_currency": "EUR"}],
        ),
        source("six", [{"isin": WORLD, "ter": 0.0020}]),
        source("openfigi", listings=[{"isin": WORLD, "exchange_mic": "XMIL", "ticker": "SWDA"}]),
        source("yahoo", [{"isin": WORLD, "aum_eur": 8.0e10}]),
    ]


def test_output_is_independent_of_adapter_order(registry):
    """Shuffle the adapters; the published tables must not move a single cell.

    Thirty of the 720 permutations, drawn with a fixed seed: enough to catch an
    order dependency, cheap enough that nobody is tempted to skip the suite.
    """
    baseline = merged(_six_sources(), registry)
    shuffler = random.Random(20260811)
    for _ in range(30):
        order = _six_sources()
        shuffler.shuffle(order)
        result = merged(order, registry)
        pd.testing.assert_frame_equal(result.funds, baseline.funds)
        pd.testing.assert_frame_equal(result.listings, baseline.listings)


def test_ties_break_on_the_source_name_not_the_run_order(registry):
    """Two venues at TRUST 80 disagree; the answer must still be reproducible."""
    forward = merged(
        [
            source("xetra", [{"isin": WORLD, "name": "Xetra's name"}]),
            source("euronext", [{"isin": WORLD, "name": "Euronext's name"}]),
        ],
        registry,
    )
    backward = merged(
        [
            source("euronext", [{"isin": WORLD, "name": "Euronext's name"}]),
            source("xetra", [{"isin": WORLD, "name": "Xetra's name"}]),
        ],
        registry,
    )
    assert value(forward.funds, WORLD, "name") == "Euronext's name"  # alphabetically first
    assert value(backward.funds, WORLD, "name") == "Euronext's name"


def test_sample_is_deterministic_and_keeps_the_benchmark(registry):
    keys = [WORLD, SP500, AMUNDI, SURROGATE]
    result = merged([source("firds", [{"isin": key} for key in keys])], registry)

    first = universe.sample(result.funds, result.listings, 2, keep=(WORLD,))[0]
    second = universe.sample(result.funds, result.listings, 2, keep=(WORLD,))[0]
    assert list(first["isin"]) == list(second["isin"])
    assert WORLD in set(first["isin"])
    assert len(first) == 2


# --------------------------------------------------------------------------- #
# Keys
# --------------------------------------------------------------------------- #


def test_surrogate_and_isin_keys_cannot_collide(registry):
    result = merged(
        [
            source("firds", [{"isin": WORLD, "name": "a real ISIN"}]),
            source("nasdaqtrader", [{"isin": SURROGATE, "name": "no ISIN exists"}]),
        ],
        registry,
    )
    assert len(result.funds) == 2
    assert universe.is_surrogate(SURROGATE)
    assert not universe.is_surrogate(WORLD)
    # The two shapes are told apart by the same test the US adapter publishes.
    assert us.is_surrogate(SURROGATE) and not us.is_surrogate(WORLD)
    assert result.report.surrogate_keys == 1


def test_a_surrogate_is_taken_verbatim():
    """Ticker punctuation must survive: ISIN normalisation would strip it."""
    assert universe.normalise_key("US:XNAS:BRK.B") == "US:XNAS:BRK.B"
    assert universe.normalise_key("US:ARCX:RDS-A") == "US:ARCX:RDS-A"
    assert universe.normalise_key(f"  {WORLD.lower()}  ") == WORLD


def test_checksum_invalid_isins_are_rejected_and_counted(registry):
    result = merged(
        [
            source(
                "euronext",
                [{"isin": WORLD, "name": "good"}, {"isin": BAD_ISIN, "name": "typo"}],
                [{"isin": BAD_ISIN, "exchange_mic": "XPAR", "ticker": "NOPE"}],
            )
        ],
        registry,
    )
    assert list(result.funds["isin"]) == [WORLD]
    assert result.report.rejected_isins["euronext"] == (BAD_ISIN,)
    assert result.report.rejected_isin_count == 1
    # ...and the listing hanging off the bad key went with it.
    assert BAD_ISIN not in set(result.listings["isin"])


def test_a_rejected_isin_is_reported_per_source(registry):
    result = merged(
        [
            source("euronext", [{"isin": BAD_ISIN}]),
            source("xetra", [{"isin": "NOT-AN-ISIN"}]),
            source("firds", [{"isin": WORLD}]),
        ],
        registry,
    )
    assert set(result.report.rejected_isins) == {"euronext", "xetra"}
    statuses = {s.name: s for s in result.report.sources}
    assert statuses["euronext"].rejected_isins == (BAD_ISIN,)
    assert statuses["firds"].rejected_isins == ()


def test_data_sources_names_every_contributor(registry):
    result = merged(
        [
            source("firds", [{"isin": WORLD, "name": "n"}]),
            source("euronext", [{"isin": WORLD, "short_name": "IWDA"}]),
            source("yahoo", [{"isin": WORLD, "ter": 0.002}]),
            source("xetra", [{"isin": AMUNDI, "name": "other fund"}]),
        ],
        registry,
    )
    assert list(value(result.funds, WORLD, "data_sources")) == ["euronext", "firds", "yahoo"]
    assert list(value(result.funds, AMUNDI, "data_sources")) == ["xetra"]


def test_provenance_answers_where_did_this_come_from(registry):
    result = merged(_six_sources(), registry)
    assert result.source_of(WORLD, "ter") == "six"
    assert result.source_of(WORLD, "aum_eur") == "yahoo"
    assert result.source_of(WORLD, "name") == "firds"
    trail = result.provenance
    assert set(trail.columns) == {"isin", "field", "source", "trust", "contenders"}
    assert (trail["trust"] > 0).all()


# --------------------------------------------------------------------------- #
# Hazard 1: MIC disagreement
# --------------------------------------------------------------------------- #

# Every MIC each adapter can put in a listing row, read off the adapters' own
# declared tables. FIRDS is the exception: it emits whatever RMKT MIC ESMA
# reports, so its preference list stands for the codes it is known to produce.
ADAPTER_MIC_TABLES: dict[str, set[str]] = {
    "firds": set(firds.MIC_PREFERENCE),
    "euronext": set(euronext.MIC_NAMES),
    "xetra": {xetra.MIC},
    "lse": {lse.MIC},
    "six": {six.MIC},
    "nasdaqtrader": {mic for mic, _name in us.EXCHANGE_MIC.values()},
    "openfigi": set(openfigi.EXCH_CODE_MIC.values()),
    "yahoo": set(yahoo_meta.YAHOO_EXCHANGE_MIC.values()),
}

# Operating-MIC groups that genuinely hold more than one venue, with the reason.
# Everything else must collapse to exactly one code.
EXPECTED_PARALLEL_BOOKS: dict[str, set[str]] = {
    # Three distinct US listing venues that happen to share an operator.
    "XNYS": {"XNYS", "ARCX", "XASE"},
    # Euronext's multi-currency books carry the USD quote line of a fund also
    # listed in EUR on the main book: two real listings, not one venue twice.
    "XAMS": {"XAMS", "XAMC"},
    "XPAR": {"XPAR", "XPMC"},
}


def test_no_two_adapters_emit_different_mics_for_one_venue(registry):
    """The hazard, asserted over the adapters' own code rather than over data.

    Group every MIC any adapter can emit by the operating MIC ISO 10383 assigns
    it. After canonicalisation each group must be a single code -- otherwise two
    adapters are naming one venue two ways, and the same listing lands twice in
    the listings table under two keys.
    """
    groups: dict[str, dict[str, set[str]]] = {}
    for adapter, mics in ADAPTER_MIC_TABLES.items():
        for mic in mics:
            canonical = registry.canonical(mic)
            operating = registry.operating(canonical) or canonical
            groups.setdefault(operating, {}).setdefault(str(canonical), set()).add(adapter)

    offenders = {
        operating: {code: sorted(who) for code, who in codes.items()}
        for operating, codes in groups.items()
        if len(codes) > 1 and set(codes) != EXPECTED_PARALLEL_BOOKS.get(operating, set())
    }
    assert not offenders, f"one venue under several MICs: {offenders}"


def test_borsa_italiana_is_one_venue_not_two(registry):
    """The named case: ETFP from Euronext/FIRDS, XMIL from OpenFIGI and Yahoo."""
    result = merged(
        [
            source("euronext", [{"isin": WORLD}],
                   [{"isin": WORLD, "exchange_mic": "ETFP", "ticker": "SWDA",
                     "trading_currency": "EUR"}]),
            source("openfigi", listings=[{"isin": WORLD, "exchange_mic": "XMIL", "ticker": "SWDA"}]),
        ],
        registry,
    )
    assert list(result.listings["exchange_mic"]) == ["XMIL"]
    assert result.report.mic_rewrites == {"ETFP->XMIL": 1}
    assert result.report.duplicate_listings == 1


def test_xetra_segments_collapse_onto_the_venue(registry):
    result = merged(
        [
            source("firds", [{"isin": AMUNDI}],
                   [{"isin": AMUNDI, "exchange_mic": "XETA", "trading_currency": "EUR"},
                    {"isin": AMUNDI, "exchange_mic": "XETU", "trading_currency": "EUR"}]),
            source("xetra", [{"isin": AMUNDI}],
                   [{"isin": AMUNDI, "exchange_mic": "XETR", "ticker": "LCUW",
                     "trading_currency": "EUR"}]),
        ],
        registry,
    )
    assert list(result.listings["exchange_mic"]) == ["XETR"]
    assert value(result.listings, AMUNDI, "ticker") == "LCUW"


def test_multi_currency_books_stay_separate_listings(registry):
    """The counter-case. XAMC is not XAMS: it is the USD line of the same fund.

    Collapsing it would drop one of the two currencies, which is exactly what
    `sources/euronext.py` warns about.
    """
    result = merged(
        [
            source("euronext", [{"isin": WORLD}],
                   [{"isin": WORLD, "exchange_mic": "XAMS", "ticker": "IWDA",
                     "trading_currency": "EUR"},
                    {"isin": WORLD, "exchange_mic": "XAMC", "ticker": "IWDA",
                     "trading_currency": "USD"}]),
        ],
        registry,
    )
    assert sorted(result.listings["exchange_mic"]) == ["XAMC", "XAMS"]
    assert sorted(result.listings["trading_currency"]) == ["EUR", "USD"]


def test_the_alias_map_agrees_with_the_register(registry):
    assert registry.alias_problems() == []
    for source_mic, target in universe.MIC_ALIASES.items():
        assert registry.operating(source_mic) == target
        assert registry.canonical(target) == target


def test_an_alias_may_not_rewrite_a_surrogate_key_mic(registry):
    """A surrogate embeds its MIC, so aliasing one would orphan stored prices."""
    reserved = {mic for mic, _name in us.EXCHANGE_MIC.values()}
    hostile = universe.MicRegistry.from_csv(REGISTER, aliases={"ARCX": "XNYS"})
    problems = hostile.alias_problems(reserved)
    assert any("surrogate" in problem for problem in problems)
    # ...and the sane map is untouched by the same check.
    assert registry.alias_problems(reserved) == []


def test_an_unavailable_register_degrades_the_run_it_does_not_abort_it(monkeypatch):
    """ISO's host being down must cost the venue *names*, not the database.

    The alias map is this pipeline's own knowledge, so Borsa Italiana is still
    one venue; what is lost is the validation of that map and the register's
    names and countries.
    """

    def unreachable(refresh: bool = False):
        raise ConnectionError("iso20022.org unreachable")

    monkeypatch.setattr(universe.MicRegistry, "load", staticmethod(unreachable))

    def fetch(refresh: bool = False):
        return source("firds", [{"isin": WORLD, "name": "still built"}],
                      [{"isin": WORLD, "exchange_mic": "ETFP", "ticker": "SWDA"}])

    adapter = universe.Adapter("firds", 100, SimpleNamespace(fetch=fetch))
    result = universe.run(adapters=[adapter])

    assert len(result.funds) == 1
    assert list(result.listings["exchange_mic"]) == ["XMIL"]
    assert pd.isna(value(result.listings, WORLD, "exchange_name"))
    # Every code is now unknown, and says so, rather than being invented.
    assert result.report.unknown_mics == {"XMIL": 1}
    assert universe.MicRegistry.from_csv(universe._EMPTY_REGISTER).unknown_aliases()


def test_an_unknown_mic_passes_through_and_is_counted(registry):
    result = merged(
        [source("euronext", [{"isin": WORLD}],
                [{"isin": WORLD, "exchange_mic": "ZZZZ", "ticker": "X"}])],
        registry,
    )
    assert list(result.listings["exchange_mic"]) == ["ZZZZ"]
    assert result.report.unknown_mics == {"ZZZZ": 1}


def test_exactly_one_primary_listing_per_fund(registry):
    result = merged(_six_sources(), registry)
    primaries = result.listings.groupby("isin")["is_primary"].sum()
    assert (primaries == 1).all(), result.listings


def test_a_listing_whose_fund_is_unknown_is_dropped_and_counted(registry):
    result = merged(
        [
            source("firds", [{"isin": WORLD}]),
            source("openfigi", listings=[{"isin": SP500, "exchange_mic": "XLON", "ticker": "X"}]),
        ],
        registry,
    )
    assert set(result.listings["isin"]) == {WORLD} or result.listings.empty
    assert result.report.orphan_listings == 1


# --------------------------------------------------------------------------- #
# Hazard 2: the Yahoo AUM
# --------------------------------------------------------------------------- #


def test_an_implausible_aggregator_aum_is_nulled(registry):
    """Yahoo returns ~EUR 2.0e12 for VTI: whole-fund assets, not the ETF's."""
    result = merged([source("yahoo", [{"isin": SURROGATE, "aum_eur": 2.0e12}])], registry)
    assert pd.isna(value(result.funds, SURROGATE, "aum_eur"))
    assert result.report.aum_nulled == 1
    assert result.report.aum_examples[0][0] == SURROGATE


def test_a_plausible_aggregator_aum_survives(registry):
    result = merged([source("yahoo", [{"isin": WORLD, "aum_eur": 8.0e10}])], registry)
    assert value(result.funds, WORLD, "aum_eur") == pytest.approx(8.0e10)
    assert result.report.aum_nulled == 0


def test_the_bound_does_not_touch_a_venue_published_figure(registry):
    """A venue is accountable for its own number; the bound is for aggregators."""
    result = merged([source("six", [{"isin": WORLD, "aum_eur": 5.0e12}])], registry)
    assert value(result.funds, WORLD, "aum_eur") == pytest.approx(5.0e12)
    assert result.report.aum_nulled == 0


def test_a_nonpositive_aggregator_aum_is_nulled(registry):
    result = merged([source("yahoo", [{"isin": WORLD, "aum_eur": 0.0}])], registry)
    assert pd.isna(value(result.funds, WORLD, "aum_eur"))


# --------------------------------------------------------------------------- #
# Fail-soft
# --------------------------------------------------------------------------- #


def test_a_failed_adapter_degrades_the_result_without_aborting(registry):
    result = merged(
        [
            source("firds", [{"isin": WORLD, "name": "still here"}]),
            source("lse", ok=False, error="HTTPError: 404 Not Found"),
            source("six", ok=False, error="ReadTimeout"),
        ],
        registry,
    )
    assert len(result.funds) == 1
    assert {s.name for s in result.report.failed_sources} == {"lse", "six"}
    assert dict(
        (s.name, s.error) for s in result.report.failed_sources
    )["lse"].startswith("HTTPError")


def test_an_adapter_that_raises_is_absorbed():
    """`fetch` promises not to raise. If one does, the build must survive it."""

    def explode(refresh: bool = False):
        raise RuntimeError("upstream changed shape")

    adapter = universe.Adapter("boom", 80, SimpleNamespace(fetch=explode, NAME="boom", TRUST=80))
    result = universe._run_one(adapter, universe.Context(), refresh=False, exch_code=None)
    assert result.ok is False
    assert "RuntimeError" in (result.error or "")
    assert result.funds.empty


def test_run_survives_a_wave_of_failures(registry):
    """A whole run where nothing works still produces an (empty) universe."""

    def dead(refresh: bool = False):
        raise ConnectionError("no network")

    adapters = [
        universe.Adapter(name, trust, SimpleNamespace(fetch=dead, NAME=name, TRUST=trust))
        for name, trust in (("firds", 100), ("euronext", 80))
    ]
    result = universe.run(adapters=adapters, registry=registry)
    assert result.funds.empty
    assert len(result.report.failed_sources) == 2


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def test_plan_respects_the_declared_dependencies():
    waves = universe.plan(list(universe.ADAPTERS.values()))
    first = [adapter.name for adapter in waves[0]]
    second = [adapter.name for adapter in waves[1]]

    assert first.index("firds") < first.index("openfigi")
    # The US adapter reads the artefact OpenFIGI writes, so it must run after it
    # or every US ISIN resolves a build late.
    assert first.index("openfigi") < first.index("nasdaqtrader")
    assert first.index("firds") < first.index("gleif")
    assert second == ["yahoo"]


def test_plan_is_stable_under_a_shuffled_registry():
    adapters = list(universe.ADAPTERS.values())
    forward = [a.name for wave in universe.plan(adapters) for a in wave]
    backward = [a.name for wave in universe.plan(list(reversed(adapters))) for a in wave]
    assert forward == backward


def test_run_gives_the_enrichment_wave_a_merged_universe(registry):
    """Yahoo is handed listings; the US pass is handed only US-domiciled ISINs."""
    seen: dict[str, dict] = {}

    def firds_fetch(refresh: bool = False):
        return source(
            "firds",
            [{"isin": WORLD, "name": "world"}, {"isin": "US0378331005", "name": "us fund"}],
            [{"isin": WORLD, "exchange_mic": "XPAR", "ticker": "CW8"}],
        )

    def figi_fetch(refresh: bool = False, isins=None, exch_code=None, client=None):
        seen["openfigi"] = {"isins": list(isins or []), "exch_code": exch_code}
        return source("openfigi")

    def yahoo_fetch(refresh: bool = False, listings=None, regions=(), categories=0, client=None):
        seen["yahoo"] = {"listings": None if listings is None else len(listings)}
        return source("yahoo", [{"isin": WORLD, "ter": 0.002}])

    adapters = [
        universe.Adapter("firds", 100, SimpleNamespace(fetch=firds_fetch)),
        universe.Adapter("openfigi", 40, SimpleNamespace(fetch=figi_fetch)),
        universe.Adapter("yahoo", 40, SimpleNamespace(fetch=yahoo_fetch)),
    ]
    result = universe.run(adapters=adapters, registry=registry)

    assert seen["openfigi"]["exch_code"] == "US"
    assert seen["openfigi"]["isins"] == ["US0378331005"]
    assert seen["yahoo"]["listings"] == 1
    assert value(result.funds, WORLD, "ter") == pytest.approx(0.002)


# --------------------------------------------------------------------------- #
# Output shape
# --------------------------------------------------------------------------- #


def test_merged_tables_conform_to_the_declared_schema(registry):
    result = merged(_six_sources(), registry)
    assert universe.conform(result.funds, "funds").schema.equals(schema.FUNDS)
    assert universe.conform(result.listings, "listings").schema.equals(schema.LISTINGS)


def test_an_empty_merge_still_produces_the_declared_columns(registry):
    result = merged([source("lse", ok=False, error="dead")], registry)
    assert list(result.funds.columns) == [f.name for f in schema.FUNDS]
    assert list(result.listings.columns) == [f.name for f in schema.LISTINGS]
    assert result.report.funds == 0


# --------------------------------------------------------------------------- #
# Price symbol resolution
# --------------------------------------------------------------------------- #


def test_yahoo_candidates_prefer_the_primary_then_the_preference_order(registry):
    result = merged(_six_sources(), registry)
    candidates = universe.yahoo_candidates(result.listings)
    symbols = list(candidates[candidates["isin"] == WORLD]["yahoo_ticker"])
    assert symbols[0] == "IWDA.AS"  # Amsterdam is primary for an IE fund here
    assert set(symbols) == {"IWDA.AS", "EUNL.DE", "SWDA.MI"}
    assert list(candidates[candidates["isin"] == WORLD]["rank"]) == [0, 1, 2]


def test_a_resolved_yahoo_ticker_outranks_a_derived_one():
    listings = pd.DataFrame(
        [{"isin": SURROGATE, "exchange_mic": "ARCX", "ticker": "VTI",
          "yahoo_ticker": "VTI", "trading_currency": "USD", "is_primary": True}]
    )
    assert list(universe.yahoo_candidates(listings)["yahoo_ticker"]) == ["VTI"]


def test_no_symbol_is_invented_for_an_unmapped_venue():
    listings = pd.DataFrame(
        [{"isin": WORLD, "exchange_mic": "ZZZZ", "ticker": "X",
          "yahoo_ticker": None, "trading_currency": "EUR", "is_primary": True}]
    )
    assert universe.yahoo_candidates(listings).empty


def test_share_class_dots_become_hyphens():
    listings = pd.DataFrame(
        [{"isin": "US:XNAS:BRK.B", "exchange_mic": "XNAS", "ticker": "BRK.B",
          "yahoo_ticker": None, "trading_currency": "USD", "is_primary": True}]
    )
    assert list(universe.yahoo_candidates(listings)["yahoo_ticker"]) == ["BRK-B"]


# --------------------------------------------------------------------------- #
# Against the live register
# --------------------------------------------------------------------------- #


@pytest.mark.network
def test_alias_map_still_matches_the_published_iso_10383_register():
    """Catches ISO retiring or reparenting a code, which would split a venue."""
    try:
        live = universe.MicRegistry.load()
    except Exception as exc:  # pragma: no cover - offline
        pytest.skip(f"ISO 10383 register unreachable: {exc}")

    reserved = {mic for mic, _name in us.EXCHANGE_MIC.values()}
    assert live.alias_problems(reserved) == []
    for mic in ADAPTER_MIC_TABLES["openfigi"] | ADAPTER_MIC_TABLES["euronext"]:
        assert live.knows(mic), f"{mic} is no longer in the register"
