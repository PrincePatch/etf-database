"""Last resort: read the fund's name when no source would say what it holds.

Why this exists
---------------
`asset_class` is the screener's most-used filter, and after `sources/firds.py`
stopped asserting the CFI classes that do not survive contact with the data, half
the universe carries no answer at all. The register knows an ISIN is a collective
investment vehicle; it does not reliably know what is inside it. Nobody free does.
What is left is the one thing every fund has: a name its issuer chose to describe
it, in which "iShares Core S&P 500 ETF" is not ambiguous to any human.

So this module reads names. That is a guess, and it is treated as one.

Three rules make the guess safe
-------------------------------
**It never wins an argument.** `TRUST` is below the lowest real source, and
`apply` only writes into a cell that is null or "unknown". A name heuristic
losing to a regulator's filing is the entire point of the trust ordering; a
heuristic that could overwrite one would be a liability dressed as coverage.

**No match means null.** Every rule below is a positive statement about a phrase
that means one thing. A name that matches nothing keeps its null. A wrong "bond"
on an equity fund is worse than a null, because the user filters on `bond`,
receives a set that silently excludes what they wanted, and has no way to tell.
Recall is cheap to add later; precision lost here is invisible.

**Order encodes the traps.** These rules are read top to bottom and the first
match wins, which is how the collisions are resolved rather than by ever-longer
patterns:

* `_EQUITY_FIRST` runs before everything. A gold *miner*, an oil & gas
  *exploration* company, a *blockchain* firm are equities whose names are full of
  commodity and crypto words. Same for a fund of *bitcoin miners*.
* Multi-asset runs before equity and bond, because "LifeStrategy 60% Equity"
  and "Global Allocation Bond & Stock" name their sleeves.
* Bond runs before equity, which is what makes "S&P 500 Bond Index" a bond fund
  and "iShares Global Corp Bond" a bond fund, while "Invesco Global Clean
  Energy" -- which contains no debt word at all -- stays equity.
* Leveraged and inverse products name their *underlying*, never themselves, so
  "Direxion Daily Real Estate Bull 3X" is a real-estate product with
  `strategy = leveraged`. The asset rules see the underlying and are right.
* `currency` is deliberately almost unreachable. A currency-hedged share class
  mentions a currency in every second name in this database and is not a currency
  fund, and genuine FX funds are a rounding error, so the only patterns here are
  ones no hedged share class can produce.

`index_name`
------------
`classify` accepts the tracked index alongside the name and matches over both,
because "FTSE World Government Bond" settles a fund whose own name is a house
brand. It is null for every row in the current database -- no adapter populates
it yet -- so it costs nothing today and works the day one does.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable, Sequence

import pandas as pd

log = logging.getLogger(__name__)

NAME = "name-heuristic"

# Strictly below the lowest real source (`openfigi` and `yahoo`, at 40). It never
# competes in the merge -- `apply` writes only into empty cells -- so this number
# is a statement of standing rather than a tie-break, and it is recorded in the
# provenance trail so "why does this fund say equity" has an answer.
TRUST = 10

# The two fields this module is allowed to touch.
FIELDS = ("asset_class", "strategy")


def _rules(*pairs: tuple[str, str]) -> tuple[tuple[re.Pattern[str], str], ...]:
    return tuple((re.compile(pattern, re.IGNORECASE), value) for pattern, value in pairs)


# --------------------------------------------------------------------------- #
# asset_class
# --------------------------------------------------------------------------- #

# Companies that mine, drill or build the thing. Their names read like commodity
# and crypto funds and they hold equities, so they are settled before any of the
# rules those words would otherwise trigger.
_EQUITY_FIRST = _rules(
    (
        r"\bminers?\b|\bmining\b|gold bugs|\bexploration\b|\bproducers?\b|"
        r"\boil\s*&\s*gas\b|natural resources|\bblockchain\b|"
        r"\bmetals?\s*&\s*miners\b|\benergy (companies|equit)",
        "equity",
    ),
    (
        # A fund of the *companies* around an asset rather than the asset:
        # "Bitwise Bitcoin Standard Corporations", "Global X Uranium Companies".
        # The asset word is what its rules would fire on, so it is settled here.
        r"\b(bitcoin|crypto\w*|digital assets?|gold|silver|uranium|lithium|"
        r"copper|oil|gas)\b[\w\s&.'-]{0,25}\b(compan(y|ies)|corp(oration)?s?\b|"
        r"industry|ecosystem|equit\w*|leaders)",
        "equity",
    ),
)

_ASSET_RULES = _rules(
    # "Allocation" carries the multi-asset rule below, and it is the weakest word
    # in this module: an issuer allocating across equity factors or across bond
    # sectors uses it for a single-asset fund. Qualified, it decides the opposite
    # way, so the qualified forms are settled first. Found by auditing all 63
    # funds the multi-asset rule claimed.
    (r"\b(equit\w*|factor|sector|style)\s+allocation\b", "equity"),
    (r"\b(fixed[-\s]?income|bond|municipal|muni|credit)\s+allocation\b", "bond"),
    # Named sleeves: "LifeStrategy 60% Equity" and "Global Allocation" would both
    # be read as equity by the rules further down.
    (
        r"multi[-\s]?asset|\ballocation\b|lifestrategy|life strategy|"
        r"target[-\s]?date|\bbalanced\b|\b\d{2}/\d{2} portfolio\b",
        "multi-asset",
    ),
    (
        r"\bbitcoin\b|\bethereum\b|\bcrypto\w*|digital assets?|\bbtc\b|\bxbt\b|"
        r"\bsolana\b|\bpolkadot\b|\bcardano\b|\blitecoin\b|\bchainlink\b",
        "crypto",
    ),
    (
        r"\breits?\b|real[-\s]?estate|\bproperty\b|\brealty\b|\bepra\b|"
        r"immobili|\bimmo\b",
        "real-estate",
    ),
    # Ahead of bond: an overnight-rate or T-bill fund is money market, and every
    # one of these phrases also contains a word the bond rules claim.
    (
        r"money[-\s]?market|mon[eé]taire|geldmarkt|\bovernight\b|\b€str\b|"
        r"\bestr\b|\bsofr\b|\bsonia\b|\bsaron\b|treasury bills?|\bt[-\s]?bills?\b",
        "money-market",
    ),
    (
        # "corporate" alone is not a debt word: Canadian equity ETFs are sold in
        # "corporate class" share structures, so the debt sense must be spelled.
        r"\bbonds?\b|\bobligac|\bobligation|\brenten\b|\bgilts?\b|\btreasur|"
        r"\bbunds?\b|\bbtps?\b|\bschatz\b|\bbobl\b|aggregate|fixed[-\s]?income|"
        r"high[-\s]yield|investment[-\s]grade|\bsovereign\b|\bgovernment\b|"
        r"\bcorp(orate)?\s+bond|\bcorporates\b|\bmunicipal\b|\bmunis?\b|\btips\b|"
        r"inflation[-\s]?linked|\bduration\b|\bmaturity\b|\bconvertibles?\b|"
        r"\bcoco\b|\bat1\b|\bclo\b|\bloans?\b|\bmortgage\b|\bdebt\b|\bibonds\b|"
        r"\bfloating[-\s]?rate\b|\bcredit\b(?!\s+(suisse|agricole|mutuel|industriel))",
        "bond",
    ),
    (
        # Bare "energy" is missing on purpose: clean energy, energy transition and
        # MSCI Energy are all equity funds.
        r"\bcommodit\w*|\bgold\b|\bsilver\b|\bplatinum\b|\bpalladium\b|"
        r"precious metals?|industrial metals?|base metals?|\bbrent\b|\bwti\b|"
        r"\bcrude\b|natural gas|\bcopper\b|\baluminium\b|\baluminum\b|\bnickel\b|"
        r"\bzinc\b|\bwheat\b|\bcorn\b|\bsoybean|\bsugar\b|\bcocoa\b|\bcoffee\b|"
        r"\blivestock\b|\bagricultur\w*|\bgsci\b|\bbcom\b|\bcmci\b|"
        r"carbon (allowance|emission|credit)",
        "commodity",
    ),
    # Narrow to the point of being nearly closed. See the module docstring: this
    # database is full of "EUR Hedged" share classes that are not currency funds.
    (r"currencyshares|\b(us )?dollar index\b|\bcurrency basket\b", "currency"),
    (
        r"\bequit\w*|\bstocks?\b|\baktie\w*|\bazioni\b|\bacciones\b|"
        r"\bs&p\s?\d|\bs&p\s?/|nasdaq|\bmsci\b|\bstoxx\b|\bftse\b|\bdax\b|"
        r"\bcac\s?40\b|\bsmi\b|\bibex\b|\bmib\b|\baex\b|\bomx\w*|\bnikkei\b|"
        r"\btopix\b|\bjpx\b|\brussell\b|hang seng|\bkospi\b|\bsensex\b|"
        r"\bnifty\b|bovespa|\bwig\d|\bswig\d|\bmwig\d|\bsofix\b|\bathex\b|"
        r"dow jones|industrial average|\bacwi\b|\bs&p/asx\b|\basx\s?\d|"
        r"small[-\s]?cap|\bmid[-\s]?cap\b|large[-\s]?cap|\bdividend\w*|"
        r"aristocrat|\bmomentum\b|minimum volatility|\bmin\.?\s?vol\b|"
        r"low volatility|\bbuyback\b|\bcash cows?\b|"
        r"financials\b|health\s?care|biotech\w*|semiconductor|technolog\w*|"
        r"consumer (staples|discretionary|goods|services)|communication services|"
        r"\butilities\b|\bindustrials\b|\binsurance\b|\bbanks\b|"
        r"clean energy|renewable|\bsolar\b|\bwater\b|\bcybersecur\w*|"
        r"\brobotic\w*|artificial intelligence|\besports\b|video gaming|"
        r"\bcannabis\b|\bmarijuana\b|\binfrastructure\b",
        "equity",
    ),
)


# --------------------------------------------------------------------------- #
# strategy
#
# A much shorter list than the asset rules, and deliberately so. Most of the
# vocabulary in `schema.STRATEGY` -- broad-market, country, thematic -- is a
# judgement about a fund rather than a phrase in its name, and guessing it from a
# name produces a filter that is confidently wrong. What is left is the set of
# words that mean exactly one thing when an issuer uses them.
# --------------------------------------------------------------------------- #

_STRATEGY_RULES = _rules(
    (r"\binverse\b|\bbear\b|(?<![\w.])-\d(\.\d)?x\b|daily short|\bshort daily\b", "inverse"),
    (r"(?<![\w.])\d(\.\d)?x\b|\bleverage\w*|\blev\b|\bbull\b|\bultra\b(?!\s*short)", "leveraged"),
    (r"covered[-\s]?calls?|buy[-\s]?write", "covered-call"),
    (
        r"\besg\b|\bsri\b|sustainab\w*|socially responsib\w*|paris[-\s]?aligned|"
        r"\bclimate\b|\bscreened\b|\bethical\b|\bgreen bond",
        "esg",
    ),
    (r"\bdividend\w*|aristocrat|\bdivid\w*", "dividend"),
    (r"\bfactors?\b|multi[-\s]?factor|minimum volatility|\bmin\.?\s?vol\b|low volatility|\bmomentum\b|equal[-\s]?weight", "factor"),
    (
        r"financials\b|health\s?care|information technology|consumer staples|"
        r"consumer discretionary|communication services|\butilities\b|"
        r"\bindustrials\b|energy sector|\bbanks\b|semiconductor",
        "sector",
    ),
    (r"\bactive(ly)?\b|\bactif\b|\baktiv\b", "active"),
)

# "UltraShort" is ProShares for -2x, and it is also how three issuers write a
# very short bond duration. The word cannot decide alone, so it only counts as
# inverse once the asset rules have said the fund is not a bond fund.
_ULTRASHORT = re.compile(r"\bultra[-\s]?short\b", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# The classifier
# --------------------------------------------------------------------------- #


def classify(name: object, index_name: object = None) -> tuple[str | None, str | None]:
    """Infer `(asset_class, strategy)` from a fund's name and tracked index.

    Either element is None when nothing in the text says so, which is the answer
    for most names -- "Beta ETF TBSP Portfolio Closed" describes nothing a rule
    can stand behind, and a guess there would be a coin flip published as a fact.
    """
    text = " ".join(
        part.strip() for part in (name, index_name) if isinstance(part, str) and part.strip()
    )
    if not text:
        return None, None

    asset_class = _match(_EQUITY_FIRST, text) or _match(_ASSET_RULES, text)
    strategy = _match(_STRATEGY_RULES, text)
    if strategy is None and asset_class != "bond" and _ULTRASHORT.search(text):
        strategy = "inverse"
    return asset_class, strategy


def _match(rules: Sequence[tuple[re.Pattern[str], str]], text: str) -> str | None:
    for pattern, value in rules:
        if pattern.search(text):
            return value
    return None


def is_empty(value: object) -> bool:
    """True when a cell carries no claim: null, blank, or the "unknown" placeholder.

    "unknown" is treated as empty because that is what it means -- a source
    reached for a value and could not establish one -- so replacing it loses
    nothing. Any other value came from a source that did establish something, and
    is out of this module's reach.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return True
    return isinstance(value, str) and value.strip().lower() in ("", "unknown")


def apply(funds: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """Fill empty `asset_class` / `strategy` cells from the name. Never overwrites.

    Returns the new frame and the keys filled per field, so the caller can record
    the provenance rather than leaving a value in the database with no author.
    """
    filled: dict[str, list[str]] = {field: [] for field in FIELDS}
    if funds is None or funds.empty:
        return funds, filled

    frame = funds.copy()
    names = frame["name"] if "name" in frame else pd.Series([None] * len(frame))
    indices = frame["index_name"] if "index_name" in frame else pd.Series([None] * len(frame))

    values = {field: list(frame[field]) if field in frame else [None] * len(frame) for field in FIELDS}
    touched: set[int] = set()

    for position, (key, name, index_name) in enumerate(
        zip(frame["isin"], names, indices)
    ):
        wanted = [field for field in FIELDS if is_empty(values[field][position])]
        if not wanted:
            continue
        inferred = dict(zip(FIELDS, classify(name, index_name)))
        for field in wanted:
            if inferred[field] is None:
                continue
            values[field][position] = inferred[field]
            filled[field].append(str(key))
            touched.add(position)

    for field in FIELDS:
        frame[field] = values[field]
    if touched:
        frame["data_sources"] = [
            _with_source(sources) if position in touched else sources
            for position, sources in enumerate(frame["data_sources"])
        ]

    log.info(
        "name heuristic filled %s over %d fund(s)",
        {field: len(keys) for field, keys in filled.items()},
        len(touched),
    )
    return frame, filled


def _with_source(sources: object) -> list[str]:
    """`data_sources` with this module named, kept sorted and unique.

    The schema has no column for "this value was inferred", and inventing one is
    not this module's call -- but `data_sources` already exists to say who
    contributed to a row, and a heuristic that contributed is a contributor.
    """
    existing: Iterable[object] = () if sources is None else sources
    try:
        names = {str(value) for value in existing}
    except TypeError:  # a scalar null of some dtype
        names = set()
    return sorted(names | {NAME})


__all__ = ["FIELDS", "NAME", "TRUST", "apply", "classify", "is_empty"]
