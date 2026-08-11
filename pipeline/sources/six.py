"""SIX Swiss Exchange -- Swiss listings, and one of the few free sources with a fee.

SIX exposes its full reference dataset through `fqs/ref.csv`, keyless and
without a cookie or referer. It matters here for two reasons: Switzerland is
outside the EU register, so these listings are largely additive to FIRDS; and
`ManagementFee` is published per fund, which almost no free source does. The
legitimate fee sources -- SIX, Borsa Italiana, JPX, Nasdaq Nordic, Yahoo -- exist
precisely so this database never has to touch justETF, whose robots.txt and
terms both forbid the automated query that would hand over TER and AUM in one
request.

Three quirks that cost the recon an afternoon
---------------------------------------------
* `where=ProductLine=ET` is mandatory. Without it the endpoint answers with
  `totalRows: 0` and no error at all.
* `pageSize` is silently capped at 50 and the pagination parameter is `page`;
  `pageNumber` and `offset` are accepted and ignored, returning page one forever.
  So the full pull is ~50 sequential requests, and paging stops on the first
  empty page rather than trusting any advertised total.
* The payload is **ISO-8859-1**. Decoded as UTF-8 the Swiss and German issuer
  names either mojibake or raise.

Fees
----
`ManagementFee` is a percentage (`0.35`), and the schema stores fractions, so it
is divided by 100. It is the management fee rather than a full ongoing-charges
figure -- the closest thing SIX publishes to a TER, and labelled as such here so
a later source with a real KID-sourced OCF can outrank it.

`is_primary`
------------
Every row is XSWX, so the shared rule reduces to: the only venue this source
knows about is the primary one as far as this source can tell. A CH-domiciled
fund matches on country and the rest fall through the preference order to the
same answer. The merge stage, which sees FIRDS and the other venues too,
resolves an ISIN that is primary somewhere better.
"""

from __future__ import annotations

import io
from datetime import date
from typing import Any, Iterable

import pandas as pd

from . import SourceResult
from ._http import drop_invalid_isins, failed, get_cached, primary_flags, timestamp, to_frame

NAME = "six"
TRUST = 80

BASE_URL = "https://www.six-group.com/fqs/ref.csv"

# The 334-column schema is reachable with `select=*`; these are the columns that
# carry information no other source has.
SELECT = (
    "ISIN,ValorSymbol,ShortName,FundLongName,IssuerNameFull,"
    "TradingCurrency,FundCurrency,ManagementFee,MarketCode,ListingSegmentDesc"
)

# ET is the exchange-traded funds product line, EP the exchange-traded products
# (the ETC/ETN side, which FIRDS FULINS_C does not carry).
PRODUCT_LINES = ("ET", "EP")

# Hard cap on paging so a change in the stop condition cannot become an infinite
# loop against someone else's server.
MAX_PAGES = 200

MIC = "XSWX"
EXCHANGE_NAME = "SIX Swiss Exchange"
MIC_COUNTRY = {MIC: "CH"}

REQUIRED_COLUMNS = ("ISIN", "ValorSymbol", "TradingCurrency", "MarketCode")


def parse_page(raw: bytes) -> pd.DataFrame:
    """Read one `;`-delimited, latin-1 page. An empty page ends the pagination."""
    frame = pd.read_csv(
        io.BytesIO(raw),
        sep=";",
        dtype=str,
        encoding="ISO-8859-1",
        keep_default_na=False,
        na_values=[""],
    )
    if frame.empty:
        return frame
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise KeyError(f"SIX fqs response is missing columns: {missing}")
    return frame


def pages(product_line: str, refresh: bool = False) -> Iterable[pd.DataFrame]:
    """Yield every page of one product line, stopping at the first empty one.

    Paging is **1-based**: `page=0` answers HTTP 400 "Invalid page number", and
    a page past the end answers 200 with a header and no rows. So the stop
    condition is an empty page rather than any advertised total, which the
    endpoint does not give in CSV form anyway.
    """
    for page in range(1, MAX_PAGES + 1):
        raw = get_cached(
            NAME,
            BASE_URL,
            refresh=refresh,
            params={
                "select": SELECT,
                "where": f"ProductLine={product_line}",
                "page": str(page),
            },
        )
        frame = parse_page(raw)
        if frame.empty:
            return
        yield frame


def build(frames: Iterable[pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Turn the collected pages into the two schema frames."""
    collected = [frame for frame in frames if not frame.empty]
    frame = (
        pd.concat(collected, ignore_index=True)
        if collected
        else pd.DataFrame(columns=SELECT.split(","))
    )

    stats: dict[str, Any] = {"rows": int(len(frame)), "pages": len(collected)}

    frame["ISIN"] = frame["ISIN"].astype(str).str.strip()
    frame, rejected = drop_invalid_isins(frame, "ISIN")
    stats["invalid_isin_dropped"] = len(rejected)
    stats["invalid_isin_examples"] = rejected[:5]

    frame = frame.drop_duplicates(["ISIN", "ValorSymbol", "TradingCurrency"])
    unique = _one_line_per_isin(frame)
    stats["duplicate_listings_dropped"] = int(len(frame) - len(unique))

    funds = to_frame(
        pd.DataFrame(
            {
                "isin": unique["ISIN"],
                "name": unique["FundLongName"],
                "short_name": unique["ShortName"],
                "issuer": unique["IssuerNameFull"],
                "fund_currency": unique["FundCurrency"]
                if "FundCurrency" in unique
                else None,
                "ter": _fee_fraction(unique),
                "data_sources": [[NAME]] * len(unique),
                "last_updated": date.today(),
            }
        ),
        "funds",
    )

    listings = to_frame(
        pd.DataFrame(
            {
                "isin": unique["ISIN"],
                "exchange_mic": unique["MarketCode"],
                "exchange_name": EXCHANGE_NAME,
                "ticker": unique["ValorSymbol"],
                # Case preserved exactly: fx.py separates GBp from GBP by case.
                "trading_currency": unique["TradingCurrency"],
                "is_primary": primary_flags(
                    list(unique["ISIN"]),
                    list(unique["MarketCode"]),
                    mic_country=MIC_COUNTRY,
                    preference=(MIC,),
                ),
            }
        ),
        "listings",
    )

    stats["funds"] = int(len(funds))
    stats["listings"] = int(len(listings))
    stats["with_fee"] = int(funds["ter"].notna().sum())
    return funds, listings, stats


def _one_line_per_isin(frame: pd.DataFrame) -> pd.DataFrame:
    """Collapse a fund's several currency lines to one (ISIN, MIC) row.

    SIX quotes the same fund under several Valor symbols, one per trading
    currency, all on XSWX -- so the raw rows collide on the listings key. The
    line whose trading currency is the fund's own currency is the reference one;
    failing that the symbols are ordered alphabetically, so the choice is the
    same on every run rather than whatever the pagination happened to yield.
    """
    if frame.empty:
        return frame
    ordered = frame.copy()
    fund_currency = (
        ordered["FundCurrency"] if "FundCurrency" in ordered else pd.Series(dtype=str)
    )
    ordered["_rank"] = (
        (ordered["TradingCurrency"] != fund_currency).astype(int)
        if len(fund_currency)
        else 1
    )
    ordered = ordered.sort_values(
        ["ISIN", "MarketCode", "_rank", "ValorSymbol"], kind="mergesort"
    )
    return ordered.drop_duplicates(["ISIN", "MarketCode"]).drop(columns="_rank")


def _fee_fraction(frame: pd.DataFrame) -> pd.Series:
    """`ManagementFee` is published in percent; the schema stores fractions."""
    if "ManagementFee" not in frame:
        return pd.Series([None] * len(frame), index=frame.index, dtype="float64")
    return pd.to_numeric(frame["ManagementFee"], errors="coerce") / 100.0


def fetch(refresh: bool = False) -> SourceResult:
    """Page through both product lines and shape the result. Never raises."""
    try:
        collected: list[pd.DataFrame] = []
        counts: dict[str, int] = {}
        for product_line in PRODUCT_LINES:
            rows = 0
            for frame in pages(product_line, refresh=refresh):
                collected.append(frame)
                rows += len(frame)
            counts[product_line] = rows

        funds, listings, stats = build(collected)
        stats["by_product_line"] = counts
        return SourceResult(
            name=NAME,
            funds=funds,
            listings=listings,
            fetched_at=timestamp(),
            stats=stats,
        )
    except Exception as exc:  # noqa: BLE001 -- fail-soft is the contract
        return failed(NAME, f"{type(exc).__name__}: {exc}")
