"""US listings, from the Nasdaq Trader symbol directory.

One pipe-delimited file covers the entire US ETF market -- NYSE, NYSE Arca,
Nasdaq and Cboe BZX -- because every US venue reports into the consolidated
symbol directory Nasdaq publishes for the tape. It carried 5,597 ETF-flagged
rows on 2026-08-11, which is more than the Nasdaq screener API returns and with
strictly more fields; and unlike that API this host is not robots-disallowed
(`api.nasdaq.com/robots.txt` is `Disallow: /`, so it is not used here).

The file has two traps. The ETF flag is field 6, not field 8 -- reading the
wrong one silently yields 33 "ETFs". And the last line is not data but
`File Creation Time: 0810202615:42|||||`, which parses into a fund called
"File Creation Time" if the loader is naive about it.

The ISIN problem
----------------
This file has no ISIN, and neither does any other free US source. A US ISIN is
`US` + CUSIP + check digit, and CUSIP is licensed by CUSIP Global Services for
money, so the input to the derivation is exactly the part that cannot be
obtained. Two free routes were measured during the source recon:

* ESMA FIRDS carries the real ISIN of every US ETF that any EU venue reports,
  and OpenFIGI maps those ISINs back to their US ticker. Measured end to end on
  2026-08-11: 2,327 US-domiciled ISINs in FIRDS, 2,222 mapped to a US ticker,
  2,220 of those tickers present here -- 39.7% of the universe, and the large,
  internationally distributed part of it.
* SEC series/class -> N-CEN -> GLEIF resolved 0.8% end to end. Abandoned.

So ~60% of US ETFs will never get an ISIN here, and that is a permanent,
structural fact rather than a bug to be worked around later. The one thing this
module must never do is close the gap by inventing an identifier: a plausible
but wrong ISIN does not fail loudly, it merges two unrelated funds into one row
and corrupts every statistic computed from them. A null is recoverable; a
confident wrong key is not. Every ISIN that does get attached here is checked
against its ISO 6166 check digit first, and an ISIN claimed by two different
tickers is dropped from both rather than guessed between.

Rows with no ISIN are keyed by a surrogate, `US:{MIC}:{ticker}` -- deliberately
not ISIN-shaped, so no downstream reader can mistake one for the real thing.
The surrogate lives in the `isin` column because `schema.FUNDS` declares that
column non-nullable (a null there cannot even be written to Parquet) and the
schema carries no second key column. `is_surrogate()` is the supported way to
ask "does this fund actually have an ISIN?".

The ticker -> ISIN mapping is not fetched here. `sources/openfigi.py` writes it
to data/raw/openfigi/us_ticker_isin.json as a by-product of its own run, and
this module reads it if it is there. The coupling is one file in one direction:
without it every US row simply keeps its surrogate key.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import pandas as pd

from ..config import RAW
from . import SourceResult
from ._http import failed, get_cached, timestamp, to_frame, valid_isin

log = logging.getLogger(__name__)

NAME = "nasdaqtrader"
TRUST = 80

SYMBOL_DIRECTORY_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"

# Written by sources/openfigi.py; read here, never written here.
US_TICKER_ISIN_PATH = RAW / "openfigi" / "us_ticker_isin.json"

# The directory's Listing Exchange column is a single letter from the CTA/UTP
# participant list. Mapping it to a MIC now keeps every listing table in the
# database keyed the same way, whichever continent it came from.
EXCHANGE_MIC = {
    "A": ("XASE", "NYSE American"),
    "N": ("XNYS", "New York Stock Exchange"),
    "P": ("ARCX", "NYSE Arca"),
    "Q": ("XNAS", "Nasdaq"),
    "V": ("IEXG", "Investors Exchange"),
    "Z": ("BATS", "Cboe BZX"),
}

SURROGATE_PREFIX = "US:"

_HEADER_FIELDS = 12
_TRAILER_PREFIX = "File Creation Time"


# --------------------------------------------------------------------------- #
# Identifiers
# --------------------------------------------------------------------------- #


def surrogate_key(exchange_mic: str, ticker: str) -> str:
    """Key for a US listing whose ISIN is unknown: `US:{MIC}:{ticker}`.

    `(ticker, MIC)` is the only natural key the US tape offers -- a bare ticker
    is not unique across venues -- and the colons guarantee the result can never
    be read as an ISIN.
    """
    return f"{SURROGATE_PREFIX}{exchange_mic}:{ticker}"


def is_surrogate(key: object) -> bool:
    """True when a fund key stands in for an ISIN we could not establish."""
    return isinstance(key, str) and key.startswith(SURROGATE_PREFIX)


# --------------------------------------------------------------------------- #
# Fetch and parse
# --------------------------------------------------------------------------- #


def _download(refresh: bool) -> bytes:
    """The symbol directory's bytes, cached under data/raw/nasdaqtrader/."""
    return get_cached(NAME, SYMBOL_DIRECTORY_URL, refresh=refresh)


def parse_symbol_directory(payload: bytes | str) -> pd.DataFrame:
    """Parse nasdaqtraded.txt into one row per traded symbol.

    Returns the file's own columns, minus the trailing creation-time line, and
    minus any row whose field count disagrees with the header -- a truncated
    download should drop rows, not shift every column of the survivors.
    """
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        raise ValueError("nasdaqtraded.txt is empty")

    header = lines[0].split("|")
    if len(header) != _HEADER_FIELDS or header[5] != "ETF":
        raise ValueError(f"unexpected nasdaqtraded.txt header: {header}")

    rows = []
    for line in lines[1:]:
        if line.startswith(_TRAILER_PREFIX):
            continue
        fields = line.split("|")
        if len(fields) != _HEADER_FIELDS:
            continue
        rows.append([field.strip() for field in fields])

    return pd.DataFrame(rows, columns=[column.strip() for column in header])


def _isin_by_ticker(path: Path = US_TICKER_ISIN_PATH) -> dict[str, str]:
    """Load OpenFIGI's ticker -> ISIN artefact, keeping only what is defensible.

    Three filters, all of them about not corrupting the fund key: the check
    digit must hold, the ISIN must be US-prefixed (this file only ever claims
    US listings), and an ISIN claimed by two tickers is discarded from both --
    that is precisely the ambiguity that would fuse two funds into one row.
    """
    if not path.exists():
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.warning("unreadable OpenFIGI ticker map at %s (%s)", path, exc)
        return {}

    mapping = raw.get("tickers", raw) if isinstance(raw, dict) else {}
    valid = {
        str(ticker).strip().upper(): isin
        for ticker, isin in mapping.items()
        if valid_isin(isin) and isin.startswith("US")
    }

    counts = pd.Series(list(valid.values())).value_counts() if valid else pd.Series(dtype=int)
    contested = set(counts[counts > 1].index)
    if contested:
        log.warning("%d ISINs claimed by several tickers; dropped", len(contested))
    return {t: i for t, i in valid.items() if i not in contested}


# --------------------------------------------------------------------------- #
# Frames
# --------------------------------------------------------------------------- #


def build_frames(
    directory: pd.DataFrame, isin_by_ticker: dict[str, str] | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Turn parsed directory rows into the funds and listings frames.

    Split out from `fetch` so the whole transformation -- the ETF filter, the
    exchange mapping, the key choice -- is testable against a fixture without a
    network call.
    """
    isin_by_ticker = isin_by_ticker or {}
    stats: dict[str, int] = {"rows_in_file": int(len(directory))}

    etfs = directory[directory["ETF"] == "Y"].copy()
    stats["etf_flagged"] = int(len(etfs))

    # Test issues are exchange plumbing (ZVZZT and friends), not tradeable funds.
    etfs = etfs[etfs["Test Issue"] != "Y"]
    stats["dropped_test_issues"] = stats["etf_flagged"] - int(len(etfs))

    known = etfs["Listing Exchange"].isin(EXCHANGE_MIC)
    stats["dropped_unknown_exchange"] = int((~known).sum())
    if stats["dropped_unknown_exchange"]:
        log.warning(
            "dropping %d ETFs on unmapped listing exchanges: %s",
            stats["dropped_unknown_exchange"],
            sorted(set(etfs.loc[~known, "Listing Exchange"])),
        )
    etfs = etfs[known]

    etfs["exchange_mic"] = etfs["Listing Exchange"].map(lambda c: EXCHANGE_MIC[c][0])
    etfs["exchange_name"] = etfs["Listing Exchange"].map(lambda c: EXCHANGE_MIC[c][1])
    etfs["ticker"] = etfs["Symbol"].str.strip().str.upper()

    resolved = etfs["ticker"].map(isin_by_ticker)
    etfs["isin"] = resolved.where(
        resolved.notna(),
        [surrogate_key(mic, tic) for mic, tic in zip(etfs["exchange_mic"], etfs["ticker"])],
    )
    stats["isin_resolved"] = int(resolved.notna().sum())
    stats["isin_surrogate"] = int(len(etfs) - stats["isin_resolved"])

    # One row per symbol in, one fund out: the same fund listed twice under two
    # tickers would be two funds here, and only a real ISIN can prove otherwise.
    etfs = etfs.drop_duplicates(subset="isin", keep="first")
    today = date.today()

    funds = to_frame(
        pd.DataFrame({
            "isin": etfs["isin"].to_numpy(),
            "name": etfs["Security Name"].str.strip().to_numpy(),
            # Every ETF on a US tape is a US-registered product -- a '40 Act
            # fund, a grantor trust or a commodity pool. None of them is UCITS,
            # and the CTO classifier needs exactly that fact to explain why a US
            # ETF has no PRIIPs KID.
            "domicile": "US",
            "ucits": False,
            "fund_currency": "USD",
            "data_sources": [[NAME]] * len(etfs),
            "last_updated": today,
        }),
        "funds",
    )

    listings = to_frame(
        pd.DataFrame({
            "isin": etfs["isin"].to_numpy(),
            "exchange_mic": etfs["exchange_mic"].to_numpy(),
            "exchange_name": etfs["exchange_name"].to_numpy(),
            "ticker": etfs["ticker"].to_numpy(),
            # US symbols are Yahoo symbols, except that Yahoo writes share-class
            # dots as hyphens (BRK.B -> BRK-B).
            "yahoo_ticker": etfs["ticker"].str.replace(".", "-", regex=False).to_numpy(),
            "trading_currency": "USD",
            # The directory names one listing venue per symbol, and that venue is
            # by definition where the fund is listed.
            "is_primary": True,
        }),
        "listings",
    )

    stats["funds"] = int(len(funds))
    stats["listings"] = int(len(listings))
    return funds, listings, stats


def fetch(refresh: bool = False) -> SourceResult:
    """Load the US ETF universe. Never raises on an upstream failure."""
    try:
        directory = parse_symbol_directory(_download(refresh))
        # The artefact path is read from the module, not defaulted into the
        # signature, so a caller or a test can redirect it.
        funds, listings, stats = build_frames(directory, _isin_by_ticker(US_TICKER_ISIN_PATH))
    except Exception as exc:  # network, layout change, unreadable artefact
        return failed(NAME, f"{type(exc).__name__}: {exc}")

    stats["source_url"] = SYMBOL_DIRECTORY_URL
    return SourceResult(
        name=NAME,
        funds=funds,
        listings=listings,
        fetched_at=timestamp(),
        stats=stats,
    )
