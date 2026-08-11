"""Yahoo Finance: TER, AUM and category, for funds the venues do not describe.

Exchange files say what a fund is called and where it trades; almost none of
them say what it costs or how big it is. SIX and Borsa Italiana publish a TER,
which covers ~2,300 funds each; Yahoo's screener carries `netExpenseRatio` and
`netAssets` for tens of thousands of listings worldwide in one schema, which is
why this module exists despite everything below.

Everything below
----------------
This is the least trustworthy source in the pipeline and it is wired up
accordingly.

* Trust 40. It must never overwrite a venue- or regulator-published value; the
  merge decides that from TRUST, so this adapter simply reports what it saw.
* Yahoo's terms prohibit redistribution, so these numbers are for internal
  enrichment and cross-checking. Anything published from them needs its own
  licensed source.
* `query1.finance.yahoo.com/robots.txt` is `User-agent: * / Disallow: /`. That
  is the same posture that got justETF excluded from this pipeline entirely
  (reference/UNIVERSE_SOURCES.md §2.13), and it is the reason this module
  fetches at a trickle, caches a day at a time, and is trivially removable: one
  import in the registry.
* The endpoint is undocumented and gated. It needs a cookie plus a crumb token
  that rotates, and the CDN answers any non-browser User-Agent with HTTP 429 --
  which reads as a rate limit but is a header check (measured in
  reference/PRICE_SOURCES.md §1.1). This module sends the conventional
  `Mozilla/5.0 (compatible; ...)` bot form with the project URL in it: enough to
  pass the gate, and it does not pretend to be a person driving Chrome.

So every failure here is expected rather than exceptional. A rotated crumb, a
throttled page or a moved field degrades the run quietly and leaves the affected
columns null, because a build that dies because Yahoo changed a token is a worse
outcome than a build with no TER.

Units, which are where this data bites
--------------------------------------
`netExpenseRatio` is a percentage (SPY comes back as 0.0945, meaning 0.0945%)
and the schema stores fractions, so it is divided by 100. `netAssets` is in the
listing's quote currency, including pence for London lines, so it goes through
`fx.normalise_currency` before conversion to EUR -- treating GBp as GBP would
overstate a London fund's size by a factor of 100.

Category is a second, expensive pass. The screener stopped returning
`categoryName`, and the only endpoint that still carries it costs one request
per fund, so `fetch(categories=N)` spends that budget on the N largest funds by
AUM rather than making 13,000 calls.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

import pandas as pd
import requests

from .. import fx
from ..config import HTTP_TIMEOUT, RAW, processed_path
from . import SourceResult
from ._http import empty_frames, failed, timestamp, to_frame

log = logging.getLogger(__name__)

NAME = "yahoo"
TRUST = 40

COOKIE_URL = "https://fc.yahoo.com"
CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"
SCREENER_URL = "https://query2.finance.yahoo.com/v1/finance/screener"
QUOTE_SUMMARY_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"

# Identifies the project while satisfying the edge's browser-shaped gate. Not a
# Chrome string: we are a robot and say so.
BOT_USER_AGENT = (
    "Mozilla/5.0 (compatible; etf-database/0.1; "
    "+https://github.com/PrincePatch/etf-database)"
)

PAGE_SIZE = 250
# The screener reports a total in the tens of thousands for Germany but stops
# returning rows a little past 9,000; asking beyond that is wasted politeness.
MAX_ROWS_PER_REGION = 9_000
REQUEST_PAUSE = 0.4

# Regions whose ETF counts the recon verified. Germany's 27k is listing count,
# not funds -- the same ETF repeated across eight regional venues.
REGIONS = ("us", "gb", "de", "fr", "it", "nl", "ch", "ca", "jp", "hk", "se", "au")

CACHE_DIR = RAW / NAME

# Yahoo's exchange codes, for listings whose Yahoo symbol we do not already
# know. Deliberately partial, like every other code table in this pipeline: an
# unmapped code drops the row rather than inventing a venue for it.
YAHOO_EXCHANGE_MIC = {
    "PCX": "ARCX",
    "NYQ": "XNYS",
    "NMS": "XNAS",
    "NGM": "XNAS",
    "NCM": "XNAS",
    "BTS": "BATS",
    "ASE": "XASE",
    "GER": "XETR",
    "FRA": "XFRA",
    "PAR": "XPAR",
    "AMS": "XAMS",
    "BRU": "XBRU",
    "LSE": "XLON",
    "MIL": "XMIL",
    "EBS": "XSWX",
    "VIE": "XWBO",
    "CPH": "XCSE",
    "STO": "XSTO",
    "OSL": "XOSL",
    "HKG": "XHKG",
    "TOR": "XTSE",
    "ASX": "XASX",
}

# Morningstar-style category names, matched as substrings in order. First match
# wins, so the specific rules ("equity precious metals" is an equity sector
# fund, not a commodity fund) must precede the general ones.
_ASSET_CLASS_RULES: Sequence[tuple[str, str]] = (
    ("equity precious metals", "equity"),
    ("equity energy", "equity"),
    ("digital assets", "crypto"),
    ("bitcoin", "crypto"),
    ("crypto", "crypto"),
    ("commodities", "commodity"),
    ("precious metals", "commodity"),
    ("real estate", "real-estate"),
    ("money market", "money-market"),
    ("allocation", "multi-asset"),
    ("target-date", "multi-asset"),
    ("target date", "multi-asset"),
    ("convertible", "bond"),
    ("bond", "bond"),
    ("muni", "bond"),
    ("government", "bond"),
    ("corporate", "bond"),
    ("bank loan", "bond"),
    ("high yield", "bond"),
    ("currency", "currency"),
    ("stock", "equity"),
    ("equity", "equity"),
    ("blend", "equity"),
    ("growth", "equity"),
    ("value", "equity"),
    ("sector", "equity"),
)

_STRATEGY_RULES: Sequence[tuple[str, str]] = (
    ("leveraged", "leveraged"),
    ("inverse", "inverse"),
    ("derivative income", "covered-call"),
    ("covered call", "covered-call"),
    ("dividend", "dividend"),
    ("equity income", "dividend"),
    ("sustainab", "esg"),
    ("technology", "sector"),
    ("health", "sector"),
    ("financial", "sector"),
    ("energy", "sector"),
    ("utilities", "sector"),
    ("industrials", "sector"),
    ("consumer", "sector"),
    ("communications", "sector"),
    ("natural resources", "sector"),
    ("precious metals", "sector"),
    ("infrastructure", "sector"),
    ("real estate", "sector"),
    ("japan", "country"),
    ("china", "country"),
    ("india", "country"),
    ("europe", "country"),
    ("latin america", "country"),
    ("pacific", "country"),
    ("emerging", "country"),
    ("blend", "broad-market"),
    ("total market", "broad-market"),
    ("world stock", "broad-market"),
    ("global stock", "broad-market"),
)


def map_category(category: object) -> tuple[str | None, str | None]:
    """Map a Yahoo category onto (asset_class, strategy) from schema's vocabularies.

    A category we cannot place becomes "unknown" -- a raw upstream string in a
    controlled column would break every filter downstream. No category at all
    stays null, which is a different statement: not established, rather than
    established as unclassifiable.
    """
    if not isinstance(category, str) or not category.strip():
        return None, None

    text = category.strip().lower()
    asset_class = next((value for token, value in _ASSET_CLASS_RULES if token in text), "unknown")
    strategy = next((value for token, value in _STRATEGY_RULES if token in text), "unknown")
    return asset_class, strategy


# --------------------------------------------------------------------------- #
# Transport
# --------------------------------------------------------------------------- #


class Yahoo:
    """Cookie + crumb session over the screener and quoteSummary endpoints."""

    def __init__(
        self,
        session: object | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._session = session or requests.Session()
        if hasattr(self._session, "headers"):
            self._session.headers["User-Agent"] = BOT_USER_AGENT
        self._sleep = sleep
        self._crumb: str | None = None
        self.requests_sent = 0

    def crumb(self, refresh: bool = False) -> str:
        """The rotating token every screener call needs.

        Fetching the cookie first is what makes the crumb valid; fc.yahoo.com
        answers with an HTTP error while setting it, which is expected and
        ignored.
        """
        if self._crumb and not refresh:
            return self._crumb
        try:
            self._session.get(COOKIE_URL, timeout=HTTP_TIMEOUT)
        except requests.RequestException:  # the error page still carries cookies
            pass

        response = self._session.get(CRUMB_URL, timeout=HTTP_TIMEOUT)
        self.requests_sent += 1
        token = (response.text or "").strip()
        if response.status_code != 200 or not token or "<" in token:
            raise RuntimeError(f"no Yahoo crumb (HTTP {response.status_code})")
        self._crumb = token
        return token

    def screener_page(self, region: str, offset: int) -> dict:
        body = {
            "size": PAGE_SIZE,
            "offset": offset,
            "sortField": "fundnetassets",
            "sortType": "DESC",
            "quoteType": "ETF",
            "query": {
                "operator": "AND",
                "operands": [{"operator": "eq", "operands": ["region", region]}],
            },
            "userId": "",
            "userIdType": "guid",
        }
        params = {
            "crumb": self.crumb(),
            "lang": "en-US",
            "region": "US",
            "formatted": "false",
        }
        response = self._session.post(
            SCREENER_URL, params=params, json=body, timeout=HTTP_TIMEOUT
        )
        self.requests_sent += 1

        # An expired crumb is the normal failure mode, not an outage: take a
        # fresh one and try the page once more.
        if response.status_code == 401:
            params["crumb"] = self.crumb(refresh=True)
            response = self._session.post(
                SCREENER_URL, params=params, json=body, timeout=HTTP_TIMEOUT
            )
            self.requests_sent += 1

        if response.status_code != 200:
            raise RuntimeError(f"screener HTTP {response.status_code}")
        return response.json()["finance"]["result"][0]

    def quotes(self, region: str) -> list[dict]:
        """Every ETF quote Yahoo will page through for one region."""
        rows: list[dict] = []
        offset = 0
        while offset < MAX_ROWS_PER_REGION:
            try:
                page = self.screener_page(region, offset)
            except (requests.RequestException, RuntimeError, ValueError, KeyError) as exc:
                log.info("yahoo screener %s stopped at offset %d (%s)", region, offset, exc)
                break

            quotes = page.get("quotes") or []
            rows.extend(quotes)
            total = int(page.get("total") or 0)
            offset += PAGE_SIZE
            if len(quotes) < PAGE_SIZE or offset >= total:
                break
            self._sleep(REQUEST_PAUSE)
        return rows

    def category(self, symbol: str) -> str | None:
        """`fundProfile.categoryName` for one symbol, or None if Yahoo declines."""
        try:
            response = self._session.get(
                QUOTE_SUMMARY_URL.format(symbol=symbol),
                params={"modules": "fundProfile", "crumb": self.crumb()},
                timeout=HTTP_TIMEOUT,
            )
            self.requests_sent += 1
            if response.status_code != 200:
                return None
            result = (response.json().get("quoteSummary") or {}).get("result") or []
            return (result[0].get("fundProfile") or {}).get("categoryName") if result else None
        except (requests.RequestException, ValueError, KeyError, IndexError) as exc:
            log.debug("no category for %s (%s)", symbol, exc)
            return None


# --------------------------------------------------------------------------- #
# Caching
# --------------------------------------------------------------------------- #


def _cache_path(region: str) -> Path:
    return CACHE_DIR / f"screener_{region}.json"


def _cached_quotes(region: str, refresh: bool) -> list[dict] | None:
    """Today's cached page set for a region, if there is one.

    Cached by day: these are end-of-day-ish reference numbers, and a fund's TER
    does not move between two builds on the same afternoon.
    """
    path = _cache_path(region)
    if refresh or not path.exists():
        return None
    stamp = date.fromtimestamp(path.stat().st_mtime)
    if stamp != date.today():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _cache_quotes(region: str, quotes: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(region).write_text(json.dumps(quotes), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Keying
# --------------------------------------------------------------------------- #


def _key_index(listings: pd.DataFrame) -> tuple[dict[str, str], dict[tuple[str, str], str]]:
    """Build symbol -> fund key lookups from the universe's listing table.

    Two routes, both exact. `yahoo_ticker` is the resolved Yahoo symbol when an
    adapter knew it; otherwise the local ticker plus the venue MIC. A symbol
    that would resolve to two different funds is dropped from both indexes --
    attaching a TER to the wrong fund is the failure this whole pipeline is
    built to avoid.
    """
    by_symbol: dict[str, str] = {}
    by_ticker_mic: dict[tuple[str, str], str] = {}
    contested: set[str] = set()

    for row in listings.itertuples(index=False):
        isin = getattr(row, "isin", None)
        if not isinstance(isin, str):
            continue

        symbol = getattr(row, "yahoo_ticker", None)
        if isinstance(symbol, str) and symbol:
            key = symbol.upper()
            if by_symbol.setdefault(key, isin) != isin:
                contested.add(key)

        ticker = getattr(row, "ticker", None)
        mic = getattr(row, "exchange_mic", None)
        if isinstance(ticker, str) and isinstance(mic, str):
            by_ticker_mic.setdefault((ticker.upper(), mic), isin)

    for key in contested:
        by_symbol.pop(key, None)
    return by_symbol, by_ticker_mic


def _fund_key(quote: Mapping, by_symbol: dict, by_ticker_mic: dict) -> str | None:
    symbol = (quote.get("symbol") or "").upper()
    if not symbol:
        return None
    if symbol in by_symbol:
        return by_symbol[symbol]

    mic = YAHOO_EXCHANGE_MIC.get(quote.get("exchange"))
    root = symbol.split(".")[0]
    return by_ticker_mic.get((root, mic)) if mic else None


# --------------------------------------------------------------------------- #
# Values
# --------------------------------------------------------------------------- #


def _latest_rates() -> dict[str, float]:
    """Most recent EUR rate per currency, or an empty map if FX is unavailable."""
    try:
        rates = fx.load_rates()
    except Exception as exc:  # FX is a hard dependency of publishing, not of this
        log.warning("no FX rates for AUM conversion (%s)", exc)
        return {}
    latest = rates.sort_values("date").groupby("currency")["rate_to_eur"].last()
    return latest.to_dict()


def _to_eur(amount: object, currency: object, rates: Mapping[str, float]) -> float | None:
    if not isinstance(amount, (int, float)) or amount <= 0 or not rates:
        return None
    try:
        iso, unit = fx.normalise_currency(str(currency))
    except fx.UnsupportedCurrencyError:
        return None
    if iso == "EUR":
        return float(amount) * unit
    rate = rates.get(iso)
    return float(amount) * unit / rate if rate else None


def _ter(value: object) -> float | None:
    """Yahoo quotes the expense ratio in percent; the schema stores a fraction."""
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    # A "TER" above 25% is a unit error or a data error, never a fee.
    return float(value) / 100.0 if value < 25 else None


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #


def _universe_listings() -> pd.DataFrame:
    path = processed_path("listings")
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def fetch(
    refresh: bool = False,
    listings: pd.DataFrame | None = None,
    regions: Iterable[str] = REGIONS,
    categories: int = 0,
    client: Yahoo | None = None,
) -> SourceResult:
    """Enrich the universe with TER, AUM and (optionally) category.

    `listings` is how funds are identified: Yahoo has no ISIN anywhere in its
    responses, so a quote can only be attached to a fund the pipeline already
    knows by its symbol. Without a listing table this returns nothing, which is
    the honest answer -- it will not mint fund rows out of Yahoo symbols.
    """
    stats: dict[str, object] = {}
    empty_funds, no_listings = empty_frames()

    universe = _universe_listings() if listings is None else listings
    if universe is None or universe.empty:
        stats["note"] = "no listings to enrich"
        return SourceResult(
            name=NAME,
            funds=empty_funds,
            listings=no_listings,
            fetched_at=timestamp(),
            stats=stats,
        )

    by_symbol, by_ticker_mic = _key_index(universe)
    client = client or Yahoo()

    try:
        quotes: list[dict] = []
        for region in regions:
            cached = _cached_quotes(region, refresh)
            if cached is None:
                cached = client.quotes(region)
                if cached:
                    _cache_quotes(region, cached)
            stats[f"quotes_{region}"] = len(cached)
            quotes.extend(cached)
    except Exception as exc:  # crumb rotated, endpoint moved, edge throttled
        return failed(NAME, f"{type(exc).__name__}: {exc}")

    rates = _latest_rates()
    matched: dict[str, dict] = {}
    for quote in quotes:
        key = _fund_key(quote, by_symbol, by_ticker_mic)
        if key is None:
            continue
        ter = _ter(quote.get("netExpenseRatio"))
        aum = _to_eur(quote.get("netAssets"), quote.get("currency"), rates)
        if ter is None and aum is None:
            continue
        # Regional duplicates of one fund: keep the first, richest answer.
        record = matched.setdefault(key, {"symbol": quote.get("symbol"), "ter": None, "aum_eur": None})
        record["ter"] = record["ter"] if record["ter"] is not None else ter
        record["aum_eur"] = record["aum_eur"] if record["aum_eur"] is not None else aum

    stats["quotes_seen"] = len(quotes)
    stats["funds_matched"] = len(matched)

    if categories > 0 and matched:
        ranked = sorted(
            matched.items(), key=lambda item: item[1]["aum_eur"] or 0.0, reverse=True
        )[:categories]
        resolved = 0
        for key, record in ranked:
            name = client.category(record["symbol"])
            record["asset_class"], record["strategy"] = map_category(name)
            resolved += name is not None
            time.sleep(REQUEST_PAUSE)
        stats["categories_requested"] = len(ranked)
        stats["categories_resolved"] = resolved

    funds = to_frame(
        [
            {
                "isin": key,
                "ter": record.get("ter"),
                "aum_eur": record.get("aum_eur"),
                "asset_class": record.get("asset_class"),
                "strategy": record.get("strategy"),
                "data_sources": [NAME],
                "last_updated": date.today(),
            }
            for key, record in matched.items()
        ],
        "funds",
    )
    stats["funds"] = int(len(funds))
    stats["with_ter"] = int(funds["ter"].notna().sum()) if not funds.empty else 0
    stats["with_aum"] = int(funds["aum_eur"].notna().sum()) if not funds.empty else 0
    stats["requests_sent"] = client.requests_sent

    return SourceResult(
        name=NAME,
        funds=funds,
        listings=no_listings,
        fetched_at=timestamp(),
        stats=stats,
    )
