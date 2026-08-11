"""Tests for the name heuristic -- the one place this pipeline is allowed to guess.

The module fills `asset_class` for roughly a third of the universe, and that
column is the screener's most-used filter, so the interesting question is never
"does it classify a lot" but "is what it classifies right". Almost everything
below is therefore either a trap (a name that looks like one asset and is
another) or an abstention (a name that says nothing, which must stay null).

The four traps that motivated the module are named in the task and each has its
own assertion here: a bond fund whose name contains an equity index, an equity
fund whose name contains a commodity, a leveraged product that names its
underlying, and a currency-hedged share class that is not a currency fund.
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline import classify, schema
from pipeline.sources._http import to_frame

WORLD = "IE00B4L5Y983"
SP500 = "IE00B5BMR087"
AMUNDI = "LU1681043599"


# --------------------------------------------------------------------------- #
# The headline failure this module exists to repair
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "name",
    [
        "iShares Core S&P 500 ETF",
        "Invesco Nasdaq 100 ETF",
        "iShares S&P 500 Growth ETF",
        "Invesco S&P 500 Equal Weight",
        "Amundi S&P 500 UCITS ETF",
        "Xtrackers MSCI World Swap UCITS ETF",
        "Vanguard FTSE Developed Europe UCITS ETF",
    ],
)
def test_an_unambiguous_equity_index_reads_as_equity(name):
    """The funds FIRDS filed as "mixed"; a screener that hides these is broken."""
    assert classify.classify(name)[0] == "equity"


# --------------------------------------------------------------------------- #
# The traps
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "name,expected",
    [
        # A debt word beats an equity index: both of these are bond funds.
        ("iShares Global Corp Bond UCITS ETF", "bond"),
        ("SPDR Bloomberg S&P 500 Bond Index ETF", "bond"),
        ("Vanguard Total Bond Market ETF", "bond"),
        # ...and a fund with no debt word is not one, however global it sounds.
        ("Invesco Global Clean Energy UCITS ETF", "equity"),
        ("iShares Global Water UCITS ETF", "equity"),
        # "Corporate class" is a Canadian share structure, not a credit fund.
        ("Global X S&P 500 Index Corporate Class ETF", "equity"),
        # Companies that dig the commodity out of the ground are equities.
        ("VanEck Gold Miners UCITS ETF", "equity"),
        ("iShares MSCI Global Gold Miners ETF", "equity"),
        ("iShares Oil & Gas Exploration & Production UCITS ETF", "equity"),
        ("Global X Blockchain ETF", "equity"),
        # Found by hand-checking 30 classified funds: a fund of the corporations
        # that hold an asset is equity, however loudly it names the asset.
        ("Bitwise Bitcoin Standard Corporations ETF", "equity"),
        ("Global X Uranium Companies ETF", "equity"),
        # ...while "corporate" as a credit word must survive that rule intact.
        ("Amundi Gold Corporate Bond UCITS ETF", "bond"),
        # ...while the metal itself is a commodity, "Shares" in the name or not.
        ("abrdn Physical Gold Shares ETF", "commodity"),
        ("SPROTT PHYSICAL GOLD TRUST", "commodity"),
        ("L&G All Commodities UCITS ETF", "commodity"),
        # Named sleeves outrank the sleeve's own asset word.
        ("Vanguard LifeStrategy 60% Equity UCITS ETF", "multi-asset"),
        ("JPMorgan Strategic Allocation UCITS ETF", "multi-asset"),
        ("iShares ESG Aware 60/40 Balanced Allocation ETF", "multi-asset"),
        # ...but "allocation" qualified by an asset means a single-asset fund.
        ("SEI QiM U.S. Equity Factor Allocation Active ETF", "equity"),
        ("Columbia Diversified Fixed Income Allocation ETF", "bond"),
        ("NYLI MacKay Muni Allocation ETF", "bond"),
        # Cash-rate funds are money market, not bond.
        ("Xtrackers II EUR Overnight Rate Swap UCITS ETF", "money-market"),
        ("Amundi EUR Overnight Return UCITS ETF", "money-market"),
        # Property, and the crypto ETPs FIRDS files as commodities.
        ("iShares Core Japan REIT ETF", "real-estate"),
        ("Grayscale Bitcoin Trust ETF", "crypto"),
        ("21Shares Ethereum Staking ETP", "crypto"),
    ],
)
def test_the_named_traps(name, expected):
    assert classify.classify(name)[0] == expected


@pytest.mark.parametrize(
    "name",
    [
        "iShares Core MSCI World UCITS ETF USD Hedged (Acc)",
        "Amundi S&P 500 UCITS ETF EUR Hedged Dist",
        "AMUNDI USD HIGH YIELD CORPORATE BOND ESG UCITS ETF EUR Hedged Dist",
        "State Street SPDR Commodity GBP Hdg UCITS ETF (Acc)",
    ],
)
def test_a_currency_hedged_share_class_is_not_a_currency_fund(name):
    """Half this database mentions a currency; almost none of it holds one."""
    assert classify.classify(name)[0] != "currency"


def test_a_genuine_currency_fund_still_reaches_the_class():
    assert classify.classify("Invesco CurrencyShares Japanese Yen Trust")[0] == "currency"
    assert classify.classify("Invesco DB US Dollar Index Bullish Fund")[0] == "currency"


@pytest.mark.parametrize(
    "name,asset_class,strategy",
    [
        # Leveraged and inverse products name their underlying, never themselves.
        ("ProShares UltraShort S&P500", "equity", "inverse"),
        ("Direxion Daily Real Estate Bull 3X Shares", "real-estate", "leveraged"),
        ("BetaPro Silver 2x Daily Bull ETF", "commodity", "leveraged"),
        ("ProShares UltraShort Bitcoin ETF", "crypto", "inverse"),
        ("Xtrackers S&P 500 2x Inverse Daily Swap UCITS ETF", "equity", "inverse"),
        # ...and "ultrashort" is a duration in half its uses, which is why it
        # only means inverse once the asset rules have ruled out a bond fund.
        ("iShares GBP Ultrashort Bond UCITS ETF", "bond", None),
        ("Invesco Ultra Short Duration ETF", "bond", None),
    ],
)
def test_leverage_names_the_underlying(name, asset_class, strategy):
    assert classify.classify(name) == (asset_class, strategy)


def test_a_covered_call_fund_on_an_equity_index_is_equity():
    assert classify.classify("Global X S&P 500 Covered Call ETF") == (
        "equity",
        "covered-call",
    )
    assert classify.classify("BMO Europe High Dividend Covered Call ETF")[1] == "covered-call"


# --------------------------------------------------------------------------- #
# Abstention -- the behaviour that keeps the column trustworthy
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "name",
    [
        "ETFBW20ST",
        "Beta ETF TBSP Portfolio Closed",
        "Landsbref - LEQ UCITS ETF",
        "BNP Paribas Easy ESG Enhanced",
        "Amundi Index Solutions",
        "Expat Bulgaria Portfolio",
        "",
        "   ",
    ],
)
def test_a_name_that_says_nothing_produces_no_class(name):
    """No match means null. A guess here is a wrong filter result, not a gap."""
    assert classify.classify(name)[0] is None


def test_a_missing_name_is_not_an_error():
    assert classify.classify(None) == (None, None)
    assert classify.classify(float("nan")) == (None, None)


def test_every_value_produced_is_in_the_declared_vocabulary():
    """A raw string in a controlled column breaks every filter downstream."""
    for _pattern, value in classify._EQUITY_FIRST + classify._ASSET_RULES:
        assert value in schema.ASSET_CLASS
    for _pattern, value in classify._STRATEGY_RULES:
        assert value in schema.STRATEGY


def test_the_index_name_is_read_when_the_fund_name_will_not_say():
    """`index_name` is null everywhere today; the day it is not, it must count."""
    assert classify.classify("Amundi Index Solutions II") == (None, None)
    assert classify.classify(
        "Amundi Index Solutions II", "FTSE World Government Bond Index"
    )[0] == "bond"


# --------------------------------------------------------------------------- #
# `apply` -- the rules about what it may touch
# --------------------------------------------------------------------------- #


def funds(rows) -> pd.DataFrame:
    return to_frame(rows, "funds")


def test_a_value_from_a_real_source_is_never_overwritten():
    """The whole point of running last: a filing outranks a name, always."""
    frame, filled = classify.apply(
        funds(
            [
                {
                    "isin": SP500,
                    "name": "iShares Core S&P 500 UCITS ETF",
                    "asset_class": "bond",  # wrong, but a source said it
                    "strategy": "active",
                    "data_sources": ["firds"],
                }
            ]
        )
    )
    assert frame.loc[0, "asset_class"] == "bond"
    assert frame.loc[0, "strategy"] == "active"
    assert filled == {"asset_class": [], "strategy": []}
    assert list(frame.loc[0, "data_sources"]) == ["firds"]


@pytest.mark.parametrize("empty", [None, "unknown", "UNKNOWN", ""])
def test_an_empty_cell_is_filled(empty):
    """Null and "unknown" both mean "nobody established this"."""
    frame, filled = classify.apply(
        funds(
            [
                {
                    "isin": SP500,
                    "name": "iShares Core S&P 500 UCITS ETF",
                    "asset_class": empty,
                    "data_sources": ["firds"],
                }
            ]
        )
    )
    assert frame.loc[0, "asset_class"] == "equity"
    assert filled["asset_class"] == [SP500]


def test_a_filled_row_names_the_heuristic_in_data_sources():
    """The schema has no "inferred" column; `data_sources` already says who."""
    frame, _ = classify.apply(
        funds(
            [
                {"isin": SP500, "name": "iShares Core S&P 500 UCITS ETF",
                 "data_sources": ["firds", "xetra"]},
                {"isin": AMUNDI, "name": "ETFBW20ST", "data_sources": ["firds"]},
            ]
        )
    )
    by_isin = frame.set_index("isin")
    assert list(by_isin.loc[SP500, "data_sources"]) == ["firds", "name-heuristic", "xetra"]
    # ...and a row it could not classify is left exactly as it was found.
    assert list(by_isin.loc[AMUNDI, "data_sources"]) == ["firds"]


def test_the_heuristic_ranks_below_every_real_source():
    """`TRUST` is documentation, but documentation that must not drift."""
    from pipeline import universe

    assert classify.TRUST < min(universe.trust_levels().values())


def test_an_empty_universe_is_not_a_special_case():
    frame, filled = classify.apply(funds([]))
    assert frame.empty
    assert filled == {"asset_class": [], "strategy": []}
