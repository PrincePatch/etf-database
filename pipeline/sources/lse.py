"""London Stock Exchange -- the venue FIRDS cannot see.

The UK left the ESMA regime, so `FULINS_C` covers London barely at all: the
LSE's ~4,500 ETPs are largely additive rather than redundant, which makes this
the highest-value venue to add after the register itself. It is also the venue
that makes the currency rule in `fx.py` load-bearing (see below).

The endpoint
------------
`api.londonstockexchange.com/api/v1/components/refresh` is the JSON behind the
Price Explorer page: no key, no cookie, no referer, `size=1000` honoured, so the
whole ETF category arrives in a handful of POSTs. It returns a page-layout
document with the rows buried inside it, and the exact nesting is undocumented
and unversioned -- so the rows are located by shape (any object carrying both
`isin` and `tidm`) rather than by a path that a Drupal reshuffle would break.
An empty page ends the paging; a first page with no rows at all is treated as a
failed fetch, because "the shape changed" and "there are no ETFs in London" must
not produce the same result.

The monthly `Instrument list_NN.xlsx` workbook is the documented alternative and
is deliberately not used: its filename index increments every month with no
stable "latest" alias, so a loader has to HEAD-walk indices to find the current
one -- more requests, and a guaranteed outage on the day the index rolls.

GBp is not GBP
--------------
London quotes most ETPs in pence and reports the currency as `GBX`/`GBp`. That
differs from `GBP` by one character's case for a factor of one hundred, and
`fx.py` matches `GBp` case-sensitively on purpose. So the quote currency is
written through verbatim -- no `.upper()`, no normalisation. Upper-casing it
here would multiply every London price by a hundred and the resulting chart
would look entirely plausible.

One line per (ISIN, MIC)
------------------------
The same fund appears once per currency line, each with its own TIDM (CBTC in
GBP, CBTU in USD, same ISIN). Those collide on the listings key, so the pence
line is kept -- it is the one a UK retail order actually hits -- and otherwise
the alphabetically first TIDM, which is stable across runs.

`is_primary`
------------
Every row is XLON: as far as this source can see, that is the fund's listing.
The merge stage reconciles against sources that can see more venues.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any, Iterable, Iterator

import pandas as pd

from . import SourceResult
from ._http import drop_invalid_isins, failed, get_cached, primary_flags, timestamp, to_frame

NAME = "lse"
TRUST = 80

API_URL = "https://api.londonstockexchange.com/api/v1/components/refresh"
PAGE_PATH = "live-markets/market-data-dashboard/price-explorer"
COMPONENT_ID = "block_content:9524a5dd-7053-4f7a-ac75-71d12db796b4"

CATEGORY = "ETFS"
PAGE_SIZE = 1000
MAX_PAGES = 20

MIC = "XLON"
EXCHANGE_NAME = "London Stock Exchange"
MIC_COUNTRY = {MIC: "GB"}

# The pence quote lines, in the spellings London and its vendors use. Matched
# exactly, never case-folded: upper-casing "GBp" produces "GBP", which is a
# different currency worth a hundred times as much.
PENCE_CODES = frozenset({"GBX", "GBx", "GBp"})

# The row fields this adapter reads. Everything else the Price Explorer returns
# is delayed market data, which is a different stage's problem.
FIELDS = ("isin", "tidm", "name", "issuername", "currency", "category")


def page_body(page: int) -> dict[str, Any]:
    """The POST body for one page. Parameters are URL-encoded inside JSON strings."""
    parameters = f"categories%3D{CATEGORY}"
    return {
        "path": PAGE_PATH,
        "parameters": parameters,
        "components": [
            {
                "componentId": COMPONENT_ID,
                "parameters": f"{parameters}%26size%3D{PAGE_SIZE}%26page%3D{page}",
            }
        ],
    }


def iter_rows(payload: Any) -> Iterator[dict[str, Any]]:
    """Yield every object in the response that looks like an instrument row.

    Located by shape rather than by path: the response is a page-layout document
    whose nesting is undocumented, and a hardcoded path would break the day the
    LSE reorders its components -- silently, returning zero rows.
    """
    if isinstance(payload, dict):
        if "isin" in payload and "tidm" in payload:
            yield payload
            return
        for value in payload.values():
            yield from iter_rows(value)
    elif isinstance(payload, list):
        for value in payload:
            yield from iter_rows(value)


def parse_page(raw: bytes) -> pd.DataFrame:
    """Extract the instrument rows of one page, keeping only the fields used.

    Narrowed here rather than downstream because the rows also carry a price and
    a market cap: numbers that are stale by fifteen minutes, belong to the
    prices stage, and would make concatenating pages a dtype guessing game.
    """
    rows = list(iter_rows(json.loads(raw)))
    if not rows:
        return pd.DataFrame(columns=list(FIELDS))
    return pd.DataFrame(rows).reindex(columns=list(FIELDS))


def pages(refresh: bool = False) -> Iterable[pd.DataFrame]:
    """Yield pages until one comes back empty."""
    for page in range(MAX_PAGES):
        raw = get_cached(
            NAME,
            API_URL,
            refresh=refresh,
            method="POST",
            json_body=page_body(page),
            headers={"Content-Type": "application/json"},
        )
        frame = parse_page(raw)
        if frame.empty:
            return
        yield frame


def build(frames: Iterable[pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Turn the collected pages into the two schema frames."""
    collected = [frame for frame in frames if not frame.empty]
    if not collected:
        raise ValueError("LSE price explorer returned no instrument rows")

    frame = pd.concat(collected, ignore_index=True)
    stats: dict[str, Any] = {"rows": int(len(frame)), "pages": len(collected)}

    frame["isin"] = frame["isin"].astype(str).str.strip()
    frame, rejected = drop_invalid_isins(frame, "isin")
    stats["invalid_isin_dropped"] = len(rejected)
    stats["invalid_isin_examples"] = rejected[:5]

    frame = frame.drop_duplicates(["isin", "tidm", "currency"])
    unique = _one_line_per_isin(frame)
    stats["duplicate_listings_dropped"] = int(len(frame) - len(unique))

    funds = to_frame(
        pd.DataFrame(
            {
                "isin": unique["isin"],
                "name": unique["name"],
                "issuer": unique["issuername"],
                "data_sources": [[NAME]] * len(unique),
                "last_updated": date.today(),
            }
        ),
        "funds",
    )

    listings = to_frame(
        pd.DataFrame(
            {
                "isin": unique["isin"],
                "exchange_mic": MIC,
                "exchange_name": EXCHANGE_NAME,
                "ticker": unique["tidm"],
                # Verbatim, case included: GBp is pence and GBP is pounds.
                "trading_currency": unique["currency"],
                "is_primary": primary_flags(
                    list(unique["isin"]),
                    [MIC] * len(unique),
                    mic_country=MIC_COUNTRY,
                    preference=(MIC,),
                ),
            }
        ),
        "listings",
    )

    stats["funds"] = int(len(funds))
    stats["listings"] = int(len(listings))
    return funds, listings, stats


def _one_line_per_isin(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep the pence line of each fund, else its alphabetically first TIDM."""
    if frame.empty:
        return frame
    ordered = frame.copy()
    currency = ordered["currency"].fillna("").astype(str)
    ordered["_rank"] = (~currency.isin(PENCE_CODES)).astype(int)
    ordered = ordered.sort_values(["isin", "_rank", "tidm"], kind="mergesort")
    return ordered.drop_duplicates("isin").drop(columns="_rank")


def fetch(refresh: bool = False) -> SourceResult:
    """Page through the ETF category and shape the result. Never raises."""
    try:
        funds, listings, stats = build(pages(refresh=refresh))
        return SourceResult(
            name=NAME,
            funds=funds,
            listings=listings,
            fetched_at=timestamp(),
            stats=stats,
        )
    except Exception as exc:  # noqa: BLE001 -- fail-soft is the contract
        return failed(NAME, f"{type(exc).__name__}: {exc}")
