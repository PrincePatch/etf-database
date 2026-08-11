"""Deutsche Börse Xetra -- the German ticker, and the ETCs and ETNs FIRDS omits.

Xetra publishes its T7 all-tradable-instruments file daily, keyless, with an
`Instrument Type` column that is a clean ETP classifier (ETF / ETC / ETN / CS).
Two things make it worth a request of its own:

* **ETCs and ETNs are debt instruments** (CFI starting `D`), so they are not in
  FIRDS `FULINS_C` at all -- they live in `FULINS_D`, which is seventeen parts
  and mostly corporate bonds. Taking the ~590 German ETC/ETN lines from here
  instead is two orders of magnitude cheaper.
* **`Primary Market MIC Code`** states where each instrument is primary-listed
  even when that venue is not Xetra, which is a genuine multi-listing hint no
  other free file carries.

The URL rotates
---------------
The download is content-addressed: `/resource/blob/1528/<32-hex hash>/data/...`,
and the hash changes when the file does. The filename in the path is decorative
-- swapping `t7-xetr-` for `t7-xfra-` returns byte-identical XETR data -- so
there is no Frankfurt equivalent to fetch here. The current link is scraped from
the Downloads page each run, with the last known URL as a fallback, because a
hardcoded hash is a 404 waiting to happen and a scrape-only path is a build
failure waiting to happen.

`Instrument` is truncated
-------------------------
It is a ~25-character abbreviation ("EXPAT BUL.SOFIX UCITS ETF"), not the legal
name, so it is written to `short_name` and `name` is left null for Euronext, LSE
or the register to fill. Publishing a truncation as the fund's name would let it
win a merge against the real one on a coin toss.

`is_primary`
------------
Xetra answers this itself: a listing is primary when `Primary Market MIC Code`
is XETR. No inference, no preference order -- the venue's own statement, which
is exactly what the generic domicile-then-preference rule is a substitute for
when a source has nothing to say.
"""

from __future__ import annotations

import io
import re
from datetime import date
from typing import Any

import pandas as pd

from . import SourceResult
from ._http import drop_invalid_isins, failed, get, get_cached, timestamp, to_frame

NAME = "xetra"
TRUST = 80

HOST = "https://www.cashmarket.deutsche-boerse.com"
DOWNLOADS_PAGE = f"{HOST}/cash-en/trading/Tradable-Instruments-Xetra/Downloads"
FALLBACK_URL = (
    f"{HOST}/resource/blob/1528/a31c10e3183f4c5dd721f9c7f9eaaaea/data/"
    "t7-xetr-allTradableInstruments.csv"
)
_BLOB_RE = re.compile(
    r"/resource/blob/\d+/[0-9a-f]{16,}/data/t7-xetr-allTradableInstruments\.csv"
)

# Line 1 is `Market:;XETR`, line 2 the update date, line 3 the header.
PREAMBLE_ROWS = 2

MIC = "XETR"
EXCHANGE_NAME = "Xetra"

ETP_TYPES = ("ETF", "ETC", "ETN")

REQUIRED_COLUMNS = (
    "Instrument",
    "ISIN",
    "Mnemonic",
    "MIC Code",
    "Currency",
    "Instrument Type",
    "Primary Market MIC Code",
)

# `Product Assignment Group Description` is Deutsche Börse's own shelf label.
# Only the ones that state an asset are mapped: "PASSIV" says the fund is index
# tracking, which is a strategy claim about nothing in particular, and mapping it
# to "equity" -- as the bond and money-market trackers sitting in it would show
# -- would be an invention.
GROUP_ASSET_CLASS = {
    "EXCHANGE TRADED FUNDS - RENTEN": "bond",
    "ETF RENTEN - FOREIGN CURRENCY": "bond",
    "EXCHANGE TRADED COMMODITIES": "commodity",
}

# ETCs are commodity wrappers by construction. ETNs are not: the segment holds
# crypto, volatility and leveraged notes indiscriminately, so they stay unknown.
TYPE_ASSET_CLASS = {"ETC": "commodity"}

GROUP_STRATEGY = {"EXCHANGE TRADED FUNDS - AKTIV": "active"}


def current_url() -> str:
    """The live blob URL, scraped from the Downloads page.

    Falls back to the last known URL rather than failing: the page is a
    marketing surface that changes shape more often than the blob does.
    """
    try:
        page = get(DOWNLOADS_PAGE, timeout=60).text
        match = _BLOB_RE.search(page)
        if match:
            return HOST + match.group(0)
    except Exception:  # noqa: BLE001 -- discovery is best-effort by design
        pass
    return FALLBACK_URL


def parse(raw: bytes) -> pd.DataFrame:
    """Read the T7 file into a frame, or raise if it is no longer that file."""
    frame = pd.read_csv(
        io.BytesIO(raw),
        sep=";",
        skiprows=PREAMBLE_ROWS,
        dtype=str,
        low_memory=False,
        keep_default_na=False,
        na_values=[""],
        encoding="utf-8",
        encoding_errors="replace",
    )
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise KeyError(f"Xetra T7 file is missing columns: {missing}")
    return frame


def build(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Turn the parsed T7 file into the two schema frames."""
    stats: dict[str, Any] = {"rows": int(len(frame))}

    frame = frame[frame["Instrument Type"].isin(ETP_TYPES)].copy()
    stats["etp_rows"] = int(len(frame))
    stats["by_type"] = frame["Instrument Type"].value_counts().to_dict()

    frame["ISIN"] = frame["ISIN"].astype(str).str.strip()
    frame, rejected = drop_invalid_isins(frame, "ISIN")
    stats["invalid_isin_dropped"] = len(rejected)
    stats["invalid_isin_examples"] = rejected[:5]

    # Reported rather than filtered: an inactive line is still a listing the
    # venue publishes, and suppressing it here would hide a suspension the merge
    # stage is better placed to record.
    if "Instrument Status" in frame:
        stats["inactive_rows"] = int((frame["Instrument Status"] != "Active").sum())

    frame = frame.sort_values(["ISIN", "Mnemonic"], kind="mergesort")
    unique = frame.drop_duplicates("ISIN")
    stats["duplicate_listings_dropped"] = int(len(frame) - len(unique))

    groups = unique["Product Assignment Group Description"]
    funds = to_frame(
        pd.DataFrame(
            {
                "isin": unique["ISIN"],
                "short_name": unique["Instrument"],
                "asset_class": unique["Instrument Type"]
                .map(TYPE_ASSET_CLASS)
                .fillna(groups.map(GROUP_ASSET_CLASS))
                .fillna("unknown"),
                "strategy": groups.map(GROUP_STRATEGY).fillna("unknown"),
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
                "exchange_mic": unique["MIC Code"],
                "exchange_name": EXCHANGE_NAME,
                "ticker": unique["Mnemonic"],
                # Case preserved exactly: fx.py separates GBp from GBP by case.
                "trading_currency": unique["Currency"],
                "is_primary": unique["Primary Market MIC Code"] == MIC,
            }
        ),
        "listings",
    )

    stats["funds"] = int(len(funds))
    stats["listings"] = int(len(listings))
    stats["primary_listings"] = int((unique["Primary Market MIC Code"] == MIC).sum())
    return funds, listings, stats


def fetch(refresh: bool = False) -> SourceResult:
    """Download the current T7 instrument file and shape it. Never raises."""
    try:
        raw = get_cached(NAME, current_url(), refresh=refresh)
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
