"""OpenFIGI: ISIN -> ticker, for every venue Bloomberg knows the fund on.

Enrichment only. This adapter invents no funds -- it takes the ISINs the
universe already contains and answers "what is this called where it trades",
which no single venue file can say. One ISIN routinely fans out to well over a
hundred FIGI records (SPY returns 175), so this is also the cheapest way to
learn about listings on venues the pipeline never scrapes.

What it cannot do, and why that matters here
--------------------------------------------
OpenFIGI accepts an ISIN as input but never returns one: Bloomberg does not
redistribute ISINs. So it cannot invent an identifier for a US ETF out of thin
air -- and that is exactly what makes it safe for the US gap. What it can do is
confirm that an ISIN we already hold from a regulator (ESMA FIRDS carries 2,333
US-domiciled ETF ISINs, because those funds are reported on EU venues) belongs
to a named US ticker. That direction is sound: the ISIN comes from the
regulator, the ticker comes from Bloomberg, and nothing is derived from nothing.
The resulting ticker -> ISIN map is written to data/raw/openfigi/, where
sources/us.py picks it up.

Rate limits, both tiers
-----------------------
The free API has two very different budgets, and the adapter detects which one
it is on from the presence of `config.OPENFIGI_API_KEY`:

    keyed    100 jobs/request, 25 requests / 6 seconds   ~25,000 ISIN/min
    keyless   10 jobs/request, 25 requests / 60 seconds     ~250 ISIN/min

That is a factor of a hundred, so a keyless run of the whole universe takes
tens of minutes and a keyed one takes seconds. Both paths work; the key is
worth getting. A key the server rejects is dropped and the run continues
keyless rather than failing.

Caching is permanent by design. An ISIN's ticker on a given exchange is a fact
about an instrument, not a quote -- it does not change between builds, and
re-asking for it is pure waste of a budget that is the binding constraint here.
The cache is append-only JSON Lines so a run interrupted after 200 requests
does not throw away the 200 requests. `refresh=True` re-asks everything.
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Sequence

import pandas as pd
import requests

from ..config import HTTP_TIMEOUT, OPENFIGI_API_KEY, RAW, USER_AGENT, processed_path
from . import SourceResult
from ._http import empty_frames, failed, timestamp, to_frame, valid_isin

log = logging.getLogger(__name__)

NAME = "openfigi"
TRUST = 40

MAPPING_URL = "https://api.openfigi.com/v3/mapping"

CACHE_DIR = RAW / NAME
MAPPING_CACHE_PATH = CACHE_DIR / "mapping.jsonl"
# Read by sources/us.py to attach real ISINs to US listings.
US_TICKER_ISIN_PATH = CACHE_DIR / "us_ticker_isin.json"

# The composite exchange code that stands for "the US consolidated tape". It is
# not a venue, so it yields no listing row -- only the ticker <-> ISIN fact.
US_COMPOSITE = "US"

# Bloomberg exchange codes are not MICs, and this table is deliberately
# incomplete: a code whose MIC is not certain produces no listing row at all.
# Every venue below is also covered by its own adapter at trust 80, so the cost
# of omitting a code is a missing cross-check, while the cost of guessing one
# wrong is a listing row pointing at an exchange the fund does not trade on.
EXCH_CODE_MIC = {
    "LN": "XLON",
    "NA": "XAMS",
    "FP": "XPAR",
    "BB": "XBRU",
    "PL": "XLIS",
    "GY": "XETR",
    "SW": "XSWX",
    "SS": "XSTO",
    "DC": "XCSE",
    "NO": "XOSL",
    "ID": "XDUB",
    "IM": "XMIL",
    "SM": "XMAD",
    "AV": "XWBO",
    "HK": "XHKG",
}

# Fields kept from each FIGI record. The rest (compositeFIGI, marketSector,
# securityDescription) duplicate these or are not used downstream, and the cache
# is already tens of megabytes.
_KEPT_FIELDS = ("figi", "ticker", "exchCode", "name", "securityType2", "shareClassFIGI")

_US_TICKER_RE = re.compile(r"[A-Z][A-Z0-9.]{0,5}")

_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 5


class TierDemoted(RuntimeError):
    """The API key was refused mid-run and the client fell back to keyless.

    Raised rather than handled in place because the keyless tier takes ten jobs
    per request instead of a hundred: the batch in flight is now too big, and
    only the caller knows how to cut it up again.
    """


# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Tier:
    """One of the two free-tier budgets, as published by openfigi.com."""

    name: str
    jobs: int
    requests: int
    per_seconds: float


KEYED = Tier("keyed", jobs=100, requests=25, per_seconds=6.0)
KEYLESS = Tier("keyless", jobs=10, requests=25, per_seconds=60.0)


class RateLimiter:
    """Sliding window over the last `tier.requests` request start times.

    A fixed delay between calls would either waste most of the budget or exceed
    it in bursts; the window spends the allowance as fast as it is allowed to be
    spent and blocks only when it is actually exhausted.
    """

    def __init__(self, tier: Tier, sleep: Callable[[float], None] = time.sleep) -> None:
        self.tier = tier
        self._sleep = sleep
        self._sent: deque[float] = deque()

    def acquire(self, now: Callable[[], float] = time.monotonic) -> None:
        while len(self._sent) >= self.tier.requests:
            wait = self.tier.per_seconds - (now() - self._sent[0])
            if wait <= 0:
                self._sent.popleft()
                continue
            self._sleep(wait)
        self._sent.append(now())
        while self._sent and now() - self._sent[0] >= self.tier.per_seconds:
            self._sent.popleft()


class Client:
    """Thin transport over /v3/mapping: budget, retries, and tier fallback."""

    def __init__(
        self,
        api_key: str | None = OPENFIGI_API_KEY,
        session: object | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.api_key = api_key or None
        self.tier = KEYED if self.api_key else KEYLESS
        self._session = session or requests.Session()
        self._sleep = sleep
        self._limiter = RateLimiter(self.tier, sleep)
        self.requests_sent = 0
        self.retries = 0

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT}
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key
        return headers

    def _demote(self) -> None:
        """Continue keyless after the server refuses the key.

        A rejected key is a configuration problem, not a reason to abandon the
        universe: keyless is a hundred times slower but it is not zero.
        """
        log.warning("OpenFIGI rejected the API key; continuing on the keyless tier")
        self.api_key = None
        self.tier = KEYLESS
        self._limiter = RateLimiter(self.tier, self._sleep)

    def map(self, jobs: Sequence[dict]) -> list[list[dict]]:
        """Send one request of at most `tier.jobs` jobs; return per-job records."""
        if len(jobs) > self.tier.jobs:
            raise ValueError(f"{len(jobs)} jobs exceeds the {self.tier.name} limit")

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            self._limiter.acquire()
            self.requests_sent += 1
            response = self._session.post(
                MAPPING_URL,
                json=list(jobs),
                headers=self._headers(),
                timeout=HTTP_TIMEOUT,
            )
            status = getattr(response, "status_code", 200)

            if status in (401, 403) and self.api_key:
                self._demote()
                raise TierDemoted(f"OpenFIGI refused the API key (HTTP {status})")

            if status in _RETRY_STATUSES:
                self.retries += 1
                if attempt == _MAX_ATTEMPTS:
                    raise RuntimeError(f"OpenFIGI returned HTTP {status} {_MAX_ATTEMPTS}x")
                self._sleep(_backoff(response, attempt))
                continue

            if status >= 400:
                raise RuntimeError(f"OpenFIGI returned HTTP {status}")

            payload = response.json()
            return [entry.get("data", []) or [] for entry in payload]

        raise RuntimeError("OpenFIGI request exhausted its attempts")


def _backoff(response: object, attempt: int) -> float:
    """Seconds to wait after a throttled or failed request.

    `Retry-After` is the server telling us exactly how long the window is; it is
    obeyed when present, and doubling from two seconds is the fallback.
    """
    header = getattr(response, "headers", {}) or {}
    retry_after = header.get("Retry-After") if hasattr(header, "get") else None
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            pass
    return min(60.0, 2.0 * 2 ** (attempt - 1))


# --------------------------------------------------------------------------- #
# Cache
# --------------------------------------------------------------------------- #


def _cache_key(isin: str, exch_code: str | None) -> str:
    return f"{isin}|{exch_code or ''}"


def _load_cache(path: Path = MAPPING_CACHE_PATH) -> dict[str, list[dict]]:
    if not path.exists():
        return {}
    cached: dict[str, list[dict]] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except ValueError:  # a run killed mid-write leaves a partial line
                continue
            cached[entry["key"]] = entry["records"]
    return cached


def _append_cache(entries: dict[str, list[dict]], path: Path = MAPPING_CACHE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for key, records in entries.items():
            handle.write(json.dumps({"key": key, "records": records}) + "\n")


def _trim(records: Iterable[dict]) -> list[dict]:
    return [{f: record.get(f) for f in _KEPT_FIELDS if record.get(f)} for record in records]


def cached_mappings(path: Path | None = None) -> dict[str, list[dict]]:
    """Everything ever mapped, keyed by ISIN and merged across exchange filters.

    The artefact sources/us.py reads is rebuilt from this rather than from the
    ISINs of the run that happens to be finishing: a run over a subset of the
    universe would otherwise publish a file naming only that subset, silently
    unresolving every fund it did not ask about.
    """
    merged: dict[str, list[dict]] = {}
    for key, records in _load_cache(path or MAPPING_CACHE_PATH).items():
        merged.setdefault(key.split("|", 1)[0], []).extend(records)
    return merged


# --------------------------------------------------------------------------- #
# Mapping
# --------------------------------------------------------------------------- #


def map_isins(
    isins: Iterable[str],
    exch_code: str | None = None,
    refresh: bool = False,
    client: Client | None = None,
) -> dict[str, list[dict]]:
    """Resolve ISINs to FIGI records, batching at the tier's job limit.

    `exch_code` narrows the answer to one exchange -- passing "US" is the
    recon's recipe for recovering US tickers and cuts the response from ~175
    records to one. Results are cached permanently, including empty ones: an
    ISIN that OpenFIGI does not know is the majority case for some venues, and
    re-asking every build would spend the whole budget on known misses.
    """
    client = client or Client()
    wanted = [i for i in dict.fromkeys(isins) if valid_isin(i)]

    # Paths are read from the module rather than defaulted into the signature so
    # a caller (or a test) can redirect the cache.
    cached = {} if refresh else _load_cache(MAPPING_CACHE_PATH)
    resolved = {
        isin: cached[_cache_key(isin, exch_code)]
        for isin in wanted
        if _cache_key(isin, exch_code) in cached
    }
    pending = [isin for isin in wanted if isin not in resolved]

    # A while loop rather than a fixed range: a rejected key shrinks the batch
    # size by a factor of ten halfway through, and the next batch must respect it.
    index = 0
    while index < len(pending):
        batch = pending[index : index + client.tier.jobs]
        extra = {"exchCode": exch_code} if exch_code else {}
        jobs = [{"idType": "ID_ISIN", "idValue": isin, **extra} for isin in batch]

        try:
            answers = client.map(jobs)
        except TierDemoted as exc:
            log.info("%s; re-chunking the batch", exc)
            continue

        index += len(batch)
        fresh = {isin: _trim(records) for isin, records in zip(batch, answers)}
        _append_cache(
            {_cache_key(i, exch_code): r for i, r in fresh.items()}, MAPPING_CACHE_PATH
        )
        resolved.update(fresh)

    return resolved


def us_ticker_isins(mapped: dict[str, list[dict]]) -> dict[str, str]:
    """Extract the ticker -> ISIN facts sources/us.py needs.

    Only the composite `US` record is used. A venue-level code would give the
    same ticker for a dozen codes, and a foreign listing of the same fund would
    give a foreign ticker -- neither belongs in a map whose only job is to say
    which line of the US tape this ISIN is.

    A ticker that two ISINs both claim is dropped rather than decided. The
    consumer joins on ticker, so an arbitrary pick there would attach one fund's
    identity to another fund's row -- the exact failure this pipeline treats as
    worse than a null.
    """
    claims: dict[str, set[str]] = {}
    for isin, records in mapped.items():
        if not valid_isin(isin):
            continue
        for record in records:
            ticker = (record.get("ticker") or "").strip().upper()
            if record.get("exchCode") == US_COMPOSITE and _US_TICKER_RE.fullmatch(ticker):
                claims.setdefault(ticker, set()).add(isin)
                break

    contested = {ticker for ticker, isins in claims.items() if len(isins) > 1}
    if contested:
        log.warning("%d US tickers claimed by several ISINs; dropped", len(contested))
    return {ticker: next(iter(isins)) for ticker, isins in claims.items() if len(isins) == 1}


def write_us_ticker_isins(tickers: dict[str, str], path: Path = US_TICKER_ISIN_PATH) -> None:
    """Publish the artefact sources/us.py reads, replacing any earlier copy."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "source": NAME,
                "generated_at": timestamp(),
                "tickers": dict(sorted(tickers.items())),
            },
            indent=1,
        ),
        encoding="utf-8",
    )


def _listings(mapped: dict[str, list[dict]]) -> pd.DataFrame:
    """One row per (ISIN, MIC) for the exchange codes that map to a real venue."""
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for isin, records in mapped.items():
        for record in records:
            mic = EXCH_CODE_MIC.get(record.get("exchCode"))
            ticker = (record.get("ticker") or "").strip().upper()
            if not mic or not ticker or (isin, mic) in seen:
                continue
            seen.add((isin, mic))
            rows.append({"isin": isin, "exchange_mic": mic, "ticker": ticker})

    return to_frame(rows, "listings")


def _universe_isins() -> list[str]:
    """ISINs already in the database, which is all this adapter may enrich.

    Returning nothing when the funds table has not been built yet is the
    correct answer, not a failure: an enrichment source that invented rows for
    funds nobody has heard of would be adding noise, not information.
    """
    path = processed_path("funds")
    if not path.exists():
        return []
    funds = pd.read_parquet(path, columns=["isin"])
    return [isin for isin in funds["isin"].dropna().unique() if valid_isin(isin)]


def fetch(
    refresh: bool = False,
    isins: Iterable[str] | None = None,
    exch_code: str | None = None,
    client: Client | None = None,
) -> SourceResult:
    """Map the universe's ISINs to tickers. Never raises on an upstream failure.

    `exch_code="US"` runs the narrow US-recovery pass; the default asks for
    every venue.
    """
    client = client or Client()
    wanted = list(isins) if isins is not None else _universe_isins()
    funds, no_listings = empty_frames()

    stats: dict[str, object] = {"tier": client.tier.name, "isins_requested": len(wanted)}
    if not wanted:
        stats["note"] = "no ISINs to enrich"
        return SourceResult(
            name=NAME,
            funds=funds,
            listings=no_listings,
            fetched_at=timestamp(),
            stats=stats,
        )

    try:
        mapped = map_isins(wanted, exch_code=exch_code, refresh=refresh, client=client)
        tickers = us_ticker_isins(cached_mappings(MAPPING_CACHE_PATH))
        write_us_ticker_isins(tickers, US_TICKER_ISIN_PATH)
        listings = _listings(mapped)
    except Exception as exc:  # network, throttling that outlasted the retries
        return failed(NAME, f"{type(exc).__name__}: {exc}")

    stats.update(
        {
            "tier": client.tier.name,  # may have been demoted mid-run
            "requests_sent": client.requests_sent,
            "retries": client.retries,
            "isins_resolved": sum(1 for records in mapped.values() if records),
            "us_tickers": len(tickers),
            "listings": int(len(listings)),
        }
    )
    # Nothing at fund level: OpenFIGI's `name` is an abbreviated Bloomberg
    # description, worse than every venue file already in the pipeline.
    return SourceResult(
        name=NAME,
        funds=funds,
        listings=listings,
        fetched_at=timestamp(),
        stats=stats,
    )
