"""Euronext product directory -- the ticker source for continental Europe.

One keyless GET returns every ETP quoted on Paris, Amsterdam, Brussels, Lisbon,
Oslo *and* Borsa Italiana's ETFplus, because Euronext owns Milan: 2,452 of the
~4,200 rows are Italian. Scraping borsaitaliana.it separately buys nothing.

What it is good for, and what it is not
---------------------------------------
Euronext carries the two fields FIRDS structurally cannot -- the tradeable
`Symbol` and the full marketing name -- which is why it is worth a second
request after the register. It carries no issuer, no TER, no AUM, and its
`filter_jsongateway` facet endpoint (which would have) returns HTTP 500 on every
call, so those stay null here and arrive from elsewhere.

The file shape
--------------
`;`-delimited CSV with a UTF-8 BOM, and a header row followed by *three lines of
preamble* ("European Trackers", the date, a disclaimer) before the data starts.
Reading it naively yields three junk funds named after a disclaimer.

Market names are not MICs
-------------------------
The `Market` column is prose, and two of its values matter more than they look:
"Euronext Amsterdam - Multi-currency Trading" is a distinct ISO 10383 segment
(XAMC, not XAMS) that carries the USD quote line of a fund also listed in EUR on
XAMS. Mapping both to XAMS would collide two genuinely different listings on the
same (ISIN, MIC) key and silently drop one of the currencies. Three-way names
("Euronext Amsterdam, Paris") are one row describing two listings and are split
into two.

An unrecognised market string is dropped and counted, never guessed into a MIC:
inventing a venue is how a fund ends up advertised as tradeable where it is not.

`is_primary`
------------
Venue in the fund's ISIN-prefix country first, then the fixed `MIC_PREFERENCE`
below -- plain order books ahead of the multi-currency segments, which are
secondary quote lines of the same fund -- then alphabetical MIC.
"""

from __future__ import annotations

import io
from datetime import date
from typing import Any

import pandas as pd

from . import SourceResult
from ._http import (
    drop_invalid_isins,
    failed,
    get_cached,
    primary_flags,
    timestamp,
    to_frame,
)

NAME = "euronext"
TRUST = 80

DOWNLOAD_URL = (
    "https://live.euronext.com/en/product_directory/data/etf-all-markets/download"
)

# The venue list the product-directory page itself passes; without it the
# endpoint answers for a default subset only.
MICS = (
    "ALXA,ALXB,ALXL,ALXP,ATFX,BGEM,ENXB,ENXL,ETFP,ETLX,EXGM,MERK,MIVX,MLXB,MOTX,"
    "MTAA,MTAH,MTCH,SEDX,TNLA,TNLB,VPXB,WOMF,XACD,XAMC,XAMS,XATL,XBRU,XDUB,XESM,"
    "XLDN,XLIS,XMLI,XMOT,XMSM,XOAM,XOAS,XOBD,XOSL,XPAR,XPMC"
)

# Lines 2-4 of the download are a title, a date and a disclaimer.
PREAMBLE_ROWS = [1, 2, 3]

REQUIRED_COLUMNS = ("Instrument Fullname", "Name", "ISIN", "Symbol", "Market", "Currency")

# Every `Market` string the endpoint emits, resolved to ISO 10383 segment MICs.
# A comma-separated name is one row standing for several listings.
MARKET_MICS: dict[str, tuple[str, ...]] = {
    "Euronext Paris": ("XPAR",),
    "Euronext Amsterdam": ("XAMS",),
    "Euronext Brussels": ("XBRU",),
    "Euronext Lisbon": ("XLIS",),
    "Euronext Dublin": ("XMSM",),
    "Euronext Paris - Multi-currency Trading": ("XPMC",),
    "Euronext Amsterdam - Multi-currency Trading": ("XAMC",),
    "Euronext Amsterdam, Paris": ("XAMS", "XPAR"),
    "Euronext Paris, Amsterdam": ("XPAR", "XAMS"),
    "Euronext Amsterdam, Brussels": ("XAMS", "XBRU"),
    "Euronext Paris, Brussels": ("XPAR", "XBRU"),
    "Oslo Børs": ("XOSL",),
    "ETF Plus": ("ETFP",),
}

MIC_NAMES = {
    "XPAR": "Euronext Paris",
    "XPMC": "Euronext Paris - Multi-currency Trading",
    "XAMS": "Euronext Amsterdam",
    "XAMC": "Euronext Amsterdam - Multi-currency Trading",
    "XBRU": "Euronext Brussels",
    "XLIS": "Euronext Lisbon",
    "XMSM": "Euronext Dublin",
    "XOSL": "Oslo Børs",
    "ETFP": "Borsa Italiana ETFplus",
}

MIC_COUNTRY = {
    "XPAR": "FR",
    "XPMC": "FR",
    "XAMS": "NL",
    "XAMC": "NL",
    "XBRU": "BE",
    "XLIS": "PT",
    "XMSM": "IE",
    "XOSL": "NO",
    "ETFP": "IT",
}

MIC_PREFERENCE = ("XPAR", "XAMS", "XBRU", "XLIS", "XMSM", "ETFP", "XOSL", "XPMC", "XAMC")

# SFDR classification, as Euronext publishes it. "-" is a statement, not a gap:
# the venue classifies every row, and an unclassified fund is an Article 6 one.
ESG_CLASSES = {"ESG ETF art. 8": True, "ESG ETF art. 9": True, "-": False}


def parse(raw: bytes) -> pd.DataFrame:
    """Read the download into a frame, or raise if it is no longer that file."""
    frame = pd.read_csv(
        io.BytesIO(raw),
        sep=";",
        encoding="utf-8-sig",
        skiprows=PREAMBLE_ROWS,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
    )
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise KeyError(f"Euronext download is missing columns: {missing}")
    return frame


def build(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Turn the parsed download into the two schema frames."""
    stats: dict[str, Any] = {"rows": int(len(frame))}

    frame = frame.copy()
    frame["ISIN"] = frame["ISIN"].astype(str).str.strip()
    frame, rejected = drop_invalid_isins(frame, "ISIN")
    stats["invalid_isin_dropped"] = len(rejected)
    stats["invalid_isin_examples"] = rejected[:5]

    known = frame["Market"].isin(MARKET_MICS)
    stats["unknown_market_dropped"] = int((~known).sum())
    stats["unknown_markets"] = sorted(set(frame.loc[~known, "Market"].astype(str)))[:5]
    frame = frame[known]

    funds = _funds(frame)
    listings, listing_stats = _listings(frame)
    stats.update(listing_stats)
    stats["funds"] = int(len(funds))
    return funds, listings, stats


def _funds(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per ISIN. Multi-listed funds repeat their name identically."""
    unique = frame.sort_values(["ISIN", "Market"], kind="mergesort").drop_duplicates(
        "ISIN"
    )
    return to_frame(
        pd.DataFrame(
            {
                "isin": unique["ISIN"],
                "name": unique["Instrument Fullname"],
                "short_name": unique["Name"],
                "esg": unique["ESG Classification"].map(ESG_CLASSES)
                if "ESG Classification" in unique
                else None,
                "data_sources": [[NAME]] * len(unique),
                "last_updated": date.today(),
            }
        ),
        "funds",
    )


def _listings(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = [
        {
            "isin": record.ISIN,
            "exchange_mic": mic,
            "exchange_name": MIC_NAMES.get(mic),
            "ticker": record.Symbol,
            # Case is preserved exactly as quoted: fx.py distinguishes GBp from
            # GBP by case alone, for a factor of one hundred.
            "trading_currency": record.Currency,
        }
        for record in frame.itertuples(index=False)
        for mic in MARKET_MICS[record.Market]
    ]

    listings = pd.DataFrame(rows, columns=_LISTING_COLUMNS)
    before = len(listings)
    listings = listings.sort_values(
        ["isin", "exchange_mic", "ticker"], kind="mergesort"
    ).drop_duplicates(["isin", "exchange_mic"])

    listings["is_primary"] = primary_flags(
        list(listings["isin"]),
        list(listings["exchange_mic"]),
        mic_country=MIC_COUNTRY,
        preference=MIC_PREFERENCE,
    )

    stats = {
        "listings": int(len(listings)),
        "duplicate_listings_dropped": int(before - len(listings)),
    }
    return to_frame(listings, "listings"), stats


_LISTING_COLUMNS = ["isin", "exchange_mic", "exchange_name", "ticker", "trading_currency"]


def fetch(refresh: bool = False) -> SourceResult:
    """Download the product directory and shape it. Never raises."""
    try:
        raw = get_cached(NAME, DOWNLOAD_URL, refresh=refresh, params={"mics": MICS})
        funds, listings, stats = build(parse(raw))
        return SourceResult(
            name=NAME,
            funds=funds,
            listings=listings,
            fetched_at=timestamp(),
            stats=stats,
        )
    except Exception as exc:  # noqa: BLE001 -- fail-soft is the contract
        return failed(NAME, f"{type(exc).__name__}: {exc}")
