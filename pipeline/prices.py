"""Daily OHLCV and corporate actions for the whole universe.

This module is the only thing in the pipeline that talks to a price vendor. It
produces the two append-only tables `adjust.py` needs -- raw bars conforming to
`schema.PRICES` (with `adj_close` still empty) and events conforming to
`schema.CORPORATE_ACTIONS` -- and it owns the on-disk layout they live in.

Why Yahoo, and only Yahoo
-------------------------
Not for lack of looking. `reference/PRICE_SOURCES.md` benchmarks every free
alternative: stooq's bulk CSV endpoint returns "Access denied" for all 24
symbols tested even after solving its proof-of-work; boerse-frankfurt and
Euronext both 403 or return AES-encrypted payloads; and all six commercial free
tiers forbid redistribution, which is fatal for an open database. Four of the
six do not serve European venues at all. There is therefore **no drop-in
fallback vendor**, and resilience has to come from somewhere else: the Parquet
store below is the real safety net, because a vendor outage costs us one day of
new bars rather than the history.

The three findings this file is built around
--------------------------------------------
1. **The 429 is a User-Agent gate, not a rate limit.** Measured back to back on
   one IP: the default `python-requests/2.33.1` UA gets `429 Too Many Requests`
   on the first call, while a browser UA served 4,000+ consecutive requests with
   zero 429s. Anyone reading a 429 here as "we are going too fast" and adding
   sleeps is fixing the wrong thing -- check the UA first. We send a browser UA
   *and* impersonate Chrome's TLS fingerprint via curl_cffi, which measured 40.6
   tickers/s against 21.5 for `requests` with the same header (HTTP/2
   multiplexing over one connection).

2. **`range=max&interval=1d` silently returns MONTHLY bars.** 404 of them,
   against 8,439 for the same symbol and the same `interval=1d` expressed as
   `period1`/`period2`. No error, no warning, 20x less data -- it would quietly
   degrade every long-window statistic in the database by an order of magnitude
   of resolution. `_chart_params` never emits `range`, and
   `tests/test_prices.py` asserts the bar spacing that comes back.

3. **Yahoo's `close` is already split-adjusted, and so are its dividend
   amounts.** Verified on AAPL: the 2020-08-27 close comes back as 125.01, not
   the ~500 the tape printed before the 4:1 split, and the 2020-02-07 dividend
   is 0.1925 rather than the 0.77 actually paid. Two consequences. Splits must
   NOT be re-applied when reconstructing total return from this source -- see
   `adjust.CLOSE_IS_SPLIT_ADJUSTED`. And a split silently *restates the entire
   stored history*, which is the one thing that breaks an append-only table, so
   `refresh` detects new split events and refetches those tickers in full.

We deliberately do not request `includeAdjustedClose`. Yahoo publishes 40
dividend events for ISF.L and applies none of them to its own adjusted close,
understating that fund's 10-year return by 4.03pp/yr while the same fund on
Amsterdam and Milan is adjusted correctly. Not fetching the column is the
cheapest way to guarantee nothing downstream can accidentally trust it.

Concurrency and politeness
--------------------------
Throughput peaks at 24-48 workers (65 tickers/s cold, 172/s incremental) and
*regresses* past that -- 64 workers measured slower than 32. `MAX_WORKERS`
therefore caps concurrency at the measured optimum rather than at whatever the
caller asks for: past the knee we would be adding load upstream to make
ourselves slower, which is the definition of abuse. Yahoo is a free public
service and this pipeline is not entitled to it. The real politeness lever is
that `refresh`, not `fetch_history`, is the daily path: a cold backfill of
13,000 tickers moves ~4.8 GB, an incremental one moves ~22 MB.

Storage layout
--------------
    data/processed/prices/bucket=00/part.parquet   (64 buckets)
    data/processed/prices/_index.parquet           (one row per fund)
    data/processed/corporate_actions.parquet

13,000 funds x 15 years is 49.1M bars. Measured on a 831k-bar sample of this
exact schema, zstd Parquet costs 22.1 B/row, so the full panel is ~1.1 GB.
**GitHub hard-blocks any file over 100 MiB**, so that cannot be committed as one
file, which is why `data/processed/prices/` is in `.gitignore` while every other
processed table is versioned. Bucketing on a stable hash of the fund key holds
each part to ~17 MB at full scale, comfortably under the limit, and makes a
per-fund read touch exactly one file instead of all 64 -- the reason for hashing
on the ISIN rather than partitioning by year, which is smaller in total but
forces every per-fund query to open every part.

The store is deliberately *raw*: `adj_close` is null on disk. Bars and events
are append-only, while the adjustment is a derived view that one new dividend
restates back to inception, so persisting it would be persisting something that
goes stale the moment it is written. `export_single_file()` applies it on the
way out, bucket by bucket -- which is exact, because a fund's whole history is
always inside one bucket.

The intended escape hatch for publishing is a **release asset**: up to 2 GiB per
file, up to 1,000 files, no bandwidth limit::

    python -c "from pipeline import prices; print(prices.export_single_file())"
    gh release create prices-$(date +%F) data/processed/prices.parquet

Shipping the 64 buckets as 64 separate assets works too and lets a consumer pull
one fund's shard without downloading the other gigabyte.
"""

from __future__ import annotations

import gzip
import json
import logging
import random
import threading
import time
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from hashlib import blake2b
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
from tqdm import tqdm

from . import schema
from .config import HTTP_RETRIES, HTTP_TIMEOUT, PROCESSED, RAW, processed_path

log = logging.getLogger(__name__)

CHART_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"

# The single header that decides whether we get data or a 429. config.USER_AGENT
# is the polite, self-identifying string every other source in this pipeline
# sends, and it is exactly what Yahoo's edge rejects -- so this one endpoint gets
# a browser UA. That is a deliberate exception, not an oversight: the alternative
# is not "be polite and slower", it is "get zero rows".
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
IMPERSONATE = "chrome"

# Measured optimum is 24-48; 64 workers is slower than 32. A hard ceiling rather
# than a default so that `workers=256` cannot turn a config typo into a flood.
DEFAULT_WORKERS = 32
MAX_WORKERS = 48

# Retry only what can plausibly succeed on a second try. A 404 is Yahoo
# correctly saying the symbol does not exist and retrying it is pure noise.
RETRY_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

# Days of overlap re-fetched by `refresh`. The last stored bar is provisional
# when it was fetched during the session, and exchanges restate prior days after
# the close, so an append that starts strictly after the stored maximum date
# freezes those errors in permanently. Five calendar days costs 1.7 KB/ticker --
# the whole 13,000-ticker daily delta is 22 MB -- and buys self-healing.
REFRESH_OVERLAP_DAYS = 5

PRICES_DIR = PROCESSED / "prices"
INDEX_NAME = "_index.parquet"
BUCKETS = 64

# Raw payload cache, for re-running the pipeline locally without re-hitting
# Yahoo. Off by default in bulk: a cold 13,000-ticker backfill would write ~1 GB
# here and a CI runner is fresh every time, so it buys nothing there. The cache
# that actually matters is the Parquet store -- that is what makes `refresh` the
# daily path and keeps us off the wire for 15 years of unchanged history.
CACHE_DIR = RAW / "yahoo_chart"
CACHE_TTL = timedelta(hours=12)

PRICE_COLUMNS = ["isin", "ticker", "date", "open", "high", "low", "close", "volume", "currency"]
ACTION_COLUMNS = ["isin", "ticker", "date", "kind", "amount", "ratio", "currency"]


class FetchError(RuntimeError):
    """An upstream failure attributable to one ticker.

    Carries the HTTP status when there was one, so the retry policy can tell a
    delisted symbol (404, permanent) from a throttled or broken edge (429/5xx,
    worth another try).
    """

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


# --------------------------------------------------------------------------- #
# Transport
#
# Injectable so the whole module is testable offline. Everything above this line
# is pure parsing and scheduling; this is the only part that needs a network.
# --------------------------------------------------------------------------- #

Transport = Callable[[str, Mapping[str, Any]], bytes]

_local = threading.local()


def _session() -> Any:
    """One curl_cffi session per thread -- they are not thread-safe, and reusing
    one per thread is what keeps the HTTP/2 connection alive across requests."""
    session = getattr(_local, "session", None)
    if session is None:
        from curl_cffi import requests as curl_requests

        session = curl_requests.Session(impersonate=IMPERSONATE)
        session.headers.update({"User-Agent": BROWSER_UA, "Accept": "application/json"})
        _local.session = session
    return session


def http_transport(symbol: str, params: Mapping[str, Any]) -> bytes:
    """Default transport: curl_cffi with Chrome's TLS fingerprint."""
    try:
        response = _session().get(
            CHART_URL.format(symbol=symbol), params=dict(params), timeout=HTTP_TIMEOUT
        )
    except Exception as exc:  # curl_cffi raises its own hierarchy; all are retryable
        raise FetchError(f"transport error: {type(exc).__name__}: {exc}") from exc

    if response.status_code != 200:
        # Yahoo returns a JSON error body on 404 with a usable description.
        raise FetchError(
            f"HTTP {response.status_code}", status=response.status_code
        )
    return response.content


# --------------------------------------------------------------------------- #
# Request construction
# --------------------------------------------------------------------------- #


def _epoch(day: date) -> int:
    """Midnight UTC of `day`. Yahoo filters on the bar's session-open timestamp,
    which for every venue we cover is later in the day than midnight UTC, so this
    includes `day` itself."""
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


def _chart_params(start: date | None = None, end: date | None = None) -> dict[str, Any]:
    """Query string for one full-resolution daily history.

    `range` is never emitted, at any value. `interval=1d` is *ignored* when
    `range` is present and Yahoo silently downgrades the response to monthly
    bars -- 404 instead of 8,439 for SPY, with a 200 status and no warning.
    """
    end_epoch = _epoch(end + timedelta(days=1)) if end else int(time.time())
    return {
        "period1": _epoch(start) if start else 0,
        "period2": end_epoch,
        "interval": "1d",
        # capitalGains is requested so we can *notice* it (see `_parse_events`),
        # not because we store it: schema.CORPORATE_ACTIONS.kind has no slot for
        # it, and it is a US-mutual-fund artefact rather than an ETF one.
        "events": "div,splits,capitalGains",
    }


# --------------------------------------------------------------------------- #
# Payload parsing
#
# Everything here treats the payload as hostile. A malformed or partial response
# must cost one ticker, never the batch, so each structural assumption is
# checked and turned into a FetchError rather than an IndexError 40 frames down.
# --------------------------------------------------------------------------- #


def _bar_dates(timestamps: Sequence[int], meta: Mapping[str, Any]) -> pd.DatetimeIndex:
    """Session timestamps -> the exchange's local calendar date.

    Yahoo stamps a daily bar at the session *open* in UTC, so a naive UTC
    conversion is correct for European venues and for the US only by luck. The
    exchange timezone is applied per-timestamp so historical DST transitions
    resolve correctly; `gmtoffset` is the current offset and would misdate bars
    from the other half of the year on venues that open near midnight UTC.
    """
    index = pd.to_datetime(pd.Series(timestamps, dtype="int64"), unit="s", utc=True)
    zone = meta.get("exchangeTimezoneName")
    if zone:
        try:
            index = index.dt.tz_convert(zone)
        except Exception:  # tz database missing for an exotic venue
            index = index + pd.Timedelta(seconds=int(meta.get("gmtoffset", 0)))
    else:
        index = index + pd.Timedelta(seconds=int(meta.get("gmtoffset", 0)))
    return pd.DatetimeIndex(index.dt.tz_localize(None).dt.normalize())


def _numeric(values: Any, length: int) -> np.ndarray:
    """Yahoo pads halted sessions with nulls and, rarely, truncates an array."""
    if not isinstance(values, list):
        return np.full(length, np.nan)
    out = np.array(values + [None] * (length - len(values)), dtype=object)[:length]
    return pd.to_numeric(pd.Series(out), errors="coerce").to_numpy(dtype="float64")


def _parse_bars(result: Mapping[str, Any]) -> pd.DataFrame:
    meta = result.get("meta") or {}
    timestamps = result.get("timestamp") or []
    if not timestamps:
        raise FetchError("payload carries no timestamps")

    indicators = result.get("indicators") or {}
    quotes = indicators.get("quote") or []
    if not quotes or not isinstance(quotes[0], dict):
        raise FetchError("payload carries no quote indicators")
    quote = quotes[0]

    n = len(timestamps)
    frame = pd.DataFrame(
        {
            "date": _bar_dates(timestamps, meta),
            "open": _numeric(quote.get("open"), n),
            "high": _numeric(quote.get("high"), n),
            "low": _numeric(quote.get("low"), n),
            "close": _numeric(quote.get("close"), n),
            "volume": _numeric(quote.get("volume"), n),
        }
    )

    # A null close is a session the venue did not print (halt, holiday Yahoo
    # kept in the grid). Keeping it would inject a gap into every return series.
    frame = frame[frame["close"].notna()]
    if frame.empty:
        raise FetchError("payload carries no priced sessions")

    # Yahoo occasionally repeats the last session; keep the later observation.
    frame = frame.drop_duplicates(subset="date", keep="last")
    return frame.sort_values("date", kind="mergesort").reset_index(drop=True)


def _parse_events(result: Mapping[str, Any]) -> tuple[pd.DataFrame, int]:
    """Dividend and split events, plus a count of capital-gain events skipped."""
    meta = result.get("meta") or {}
    events = result.get("events") or {}
    rows: list[dict[str, Any]] = []

    for entry in (events.get("dividends") or {}).values():
        if not isinstance(entry, dict) or entry.get("amount") is None:
            continue
        rows.append(
            {"stamp": entry.get("date"), "kind": "dividend",
             "amount": float(entry["amount"]), "ratio": np.nan}
        )

    for entry in (events.get("splits") or {}).values():
        if not isinstance(entry, dict):
            continue
        numerator, denominator = entry.get("numerator"), entry.get("denominator")
        if not numerator or not denominator:
            continue
        rows.append(
            {"stamp": entry.get("date"), "kind": "split",
             "amount": np.nan, "ratio": float(numerator) / float(denominator)}
        )

    capital_gains = len(events.get("capitalGains") or {})

    if not rows:
        return pd.DataFrame(columns=["date", "kind", "amount", "ratio"]), capital_gains

    frame = pd.DataFrame(rows)
    frame = frame[frame["stamp"].notna()]
    frame["date"] = _bar_dates(frame["stamp"].astype("int64").tolist(), meta)
    frame = frame.drop(columns="stamp")
    return (
        frame.sort_values(["date", "kind"], kind="mergesort").reset_index(drop=True),
        capital_gains,
    )


def parse_chart(payload: bytes | str | Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, str | None, int]:
    """Decode one chart response into (bars, events, currency, capital_gains).

    Raises FetchError on anything that is not a usable chart -- truncated JSON,
    an error envelope, a null result, a payload with no priced session.
    """
    if isinstance(payload, (bytes, str)):
        try:
            payload = json.loads(payload)
        except (ValueError, UnicodeDecodeError) as exc:
            raise FetchError(f"malformed JSON: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise FetchError(f"unexpected payload type {type(payload).__name__}")

    chart = payload.get("chart")
    if not isinstance(chart, Mapping):
        raise FetchError("payload has no chart envelope")

    error = chart.get("error")
    if error:
        description = error.get("description") if isinstance(error, Mapping) else error
        raise FetchError(f"upstream error: {description}")

    results = chart.get("result")
    if not results or not isinstance(results[0], Mapping):
        raise FetchError("payload has an empty result set")

    result = results[0]
    bars = _parse_bars(result)
    events, capital_gains = _parse_events(result)
    currency = (result.get("meta") or {}).get("currency")
    return bars, events, currency, capital_gains


# --------------------------------------------------------------------------- #
# Single-ticker fetch
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class FetchResult:
    """Outcome for exactly one ticker. `ok=False` is a normal outcome, not an
    exception: over 13,000 symbols there are always some delisted ones, and the
    batch must complete regardless."""

    ticker: str
    ok: bool
    bars: pd.DataFrame | None = None
    events: pd.DataFrame | None = None
    currency: str | None = None
    error: str | None = None
    status: int | None = None
    bytes_received: int = 0
    attempts: int = 1
    from_cache: bool = False
    capital_gains: int = 0


def _cache_path(symbol: str, params: Mapping[str, Any]) -> Path:
    # The window is part of the key: a cached full history must not satisfy a
    # request for a five-day delta, nor the reverse.
    stamp = blake2b(
        f"{symbol}|{params.get('period1')}|{params.get('period2', 0) // 86400}".encode(),
        digest_size=6,
    ).hexdigest()
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in symbol)
    return CACHE_DIR / f"{safe}.{stamp}.json.gz"


def fetch_one(
    ticker: str,
    start: date | None = None,
    end: date | None = None,
    *,
    transport: Transport | None = None,
    retries: int = HTTP_RETRIES,
    cache: bool = False,
) -> FetchResult:
    """Fetch one ticker's bars and events, never raising for that ticker's sake.

    Every failure mode -- 404, 429, timeout, truncated body, a chart with no
    priced session -- comes back as `FetchResult(ok=False)` with a reason. The
    only exceptions that escape are programming errors.
    """
    transport = transport or http_transport
    params = _chart_params(start, end)
    path = _cache_path(ticker, params) if cache else None

    if path is not None and path.exists():
        age = time.time() - path.stat().st_mtime
        if age < CACHE_TTL.total_seconds():
            try:
                payload = gzip.decompress(path.read_bytes())
                bars, events, currency, gains = parse_chart(payload)
                return FetchResult(ticker, True, bars, events, currency,
                                   bytes_received=len(payload), from_cache=True,
                                   capital_gains=gains)
            except (FetchError, OSError) as exc:
                log.debug("discarding unusable cache for %s: %s", ticker, exc)

    last: FetchError | None = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            payload = transport(ticker, params)
            bars, events, currency, gains = parse_chart(payload)
        except FetchError as exc:
            last = exc
            retryable = exc.status is None or exc.status in RETRY_STATUS
            # A malformed body is not worth a second round trip: Yahoo does not
            # serve a truncated chart intermittently, it serves a refusal page.
            if exc.status is None and "JSON" in str(exc):
                retryable = False
            if not retryable or attempt >= retries:
                break
            # Jittered backoff. Per the recon there is no volume-based limit, so
            # this exists for transient edge failures, not for throttling.
            time.sleep(min(2 ** (attempt - 1), 8) * (0.5 + random.random()))
            continue
        except Exception as exc:  # a bug in our parser must still not kill the batch
            last = FetchError(f"{type(exc).__name__}: {exc}")
            break
        else:
            if path is not None:
                try:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                    path.write_bytes(gzip.compress(raw, 1))
                except OSError as exc:
                    log.debug("could not cache %s: %s", ticker, exc)
            size = len(payload) if isinstance(payload, (bytes, str)) else 0
            return FetchResult(ticker, True, bars, events, currency,
                               bytes_received=size, attempts=attempt,
                               capital_gains=gains)

    assert last is not None
    return FetchResult(ticker, False, error=str(last), status=last.status,
                       attempts=attempt)


# --------------------------------------------------------------------------- #
# Batch fetch
# --------------------------------------------------------------------------- #


@dataclass
class FetchReport:
    """Attached to both returned frames as `.attrs["report"]`.

    Kept out of the return signature so callers who only want the data are not
    forced to unpack it, but present by default because a silently short batch
    is the failure mode that matters here.
    """

    requested: int = 0
    succeeded: int = 0
    failed: dict[str, str] = field(default_factory=dict)
    status_counts: dict[int, int] = field(default_factory=dict)
    bars: int = 0
    events: int = 0
    bytes_received: int = 0
    seconds: float = 0.0
    from_cache: int = 0
    capital_gains: int = 0

    @property
    def success_rate(self) -> float:
        return self.succeeded / self.requested if self.requested else 0.0

    @property
    def tickers_per_second(self) -> float:
        return self.requested / self.seconds if self.seconds else 0.0

    def summary(self) -> str:
        return (
            f"{self.succeeded}/{self.requested} tickers ({self.success_rate:.1%}) "
            f"in {self.seconds:.1f}s = {self.tickers_per_second:.1f} tk/s, "
            f"{self.bars:,} bars, {self.events:,} events, "
            f"{self.bytes_received / 1e6:.1f} MB"
        )


def _pairs(tickers: Mapping[str, str] | Iterable[str]) -> list[tuple[str, str]]:
    """Normalise the `tickers` argument into (key, yahoo_symbol) pairs.

    A mapping is read as {isin: yahoo_ticker}, matching `schema.LISTINGS`. A bare
    iterable of symbols keys the output on the symbol itself, which is what makes
    this module usable on its own; the pipeline always passes the mapping.
    """
    if isinstance(tickers, Mapping):
        pairs = [(str(k), str(v)) for k, v in tickers.items() if v and pd.notna(v)]
    else:
        pairs = [(str(t), str(t)) for t in tickers if t and pd.notna(t)]
    seen: set[tuple[str, str]] = set()
    return [p for p in pairs if not (p in seen or seen.add(p))]


def _empty_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    prices = pd.DataFrame(columns=PRICE_COLUMNS)
    actions = pd.DataFrame(columns=ACTION_COLUMNS)
    for column in ("open", "high", "low", "close", "volume"):
        prices[column] = prices[column].astype("float64")
    prices["date"] = pd.to_datetime(prices["date"])
    actions["date"] = pd.to_datetime(actions["date"])
    for column in ("amount", "ratio"):
        actions[column] = actions[column].astype("float64")
    return prices, actions


def fetch_history(
    tickers: Mapping[str, str] | Iterable[str],
    start: date | Mapping[str, date] | None = None,
    workers: int = DEFAULT_WORKERS,
    *,
    end: date | None = None,
    transport: Transport | None = None,
    cache: bool = False,
    progress: bool = True,
    retries: int = HTTP_RETRIES,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch daily bars and corporate actions for many tickers concurrently.

    `tickers` is either {isin: yahoo_ticker} or a bare iterable of symbols.
    `start` is a single date applied to every ticker, or a per-*ticker* mapping
    (what `refresh` passes); `None` means full history.

    Returns `(prices, actions)`. `prices` carries the `schema.PRICES` columns
    except `adj_close`, which `adjust.apply_adjustment` fills, plus a `ticker`
    column that `schema.conform` drops on the way to disk. Both frames carry a
    `FetchReport` in `.attrs["report"]`.

    One failing ticker never affects another: failures are collected, not raised.
    """
    pairs = _pairs(tickers)
    symbols = list(dict.fromkeys(symbol for _, symbol in pairs))
    keys_by_symbol: dict[str, list[str]] = {}
    for key, symbol in pairs:
        keys_by_symbol.setdefault(symbol, []).append(key)

    report = FetchReport(requested=len(symbols))
    if not symbols:
        prices, actions = _empty_frames()
        prices.attrs["report"] = actions.attrs["report"] = report
        return prices, actions

    def start_for(symbol: str) -> date | None:
        return start.get(symbol) if isinstance(start, Mapping) else start

    # Past the measured knee more workers make us slower *and* push more load
    # upstream, so the ceiling is enforced rather than advisory.
    workers = max(1, min(int(workers), MAX_WORKERS, len(symbols)))

    price_parts: list[pd.DataFrame] = []
    action_parts: list[pd.DataFrame] = []
    started = time.perf_counter()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                fetch_one, symbol, start_for(symbol), end,
                transport=transport, retries=retries, cache=cache,
            ): symbol
            for symbol in symbols
        }
        bar = tqdm(
            total=len(futures), desc="prices", unit="tk",
            disable=not progress, leave=False,
        )
        with bar:
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # belt and braces: fetch_one should not raise
                    result = FetchResult(symbol, False, error=f"{type(exc).__name__}: {exc}")

                bar.update(1)
                if not result.ok:
                    report.failed[symbol] = result.error or "unknown"
                    if result.status:
                        report.status_counts[result.status] = (
                            report.status_counts.get(result.status, 0) + 1
                        )
                    continue

                report.succeeded += 1
                report.bytes_received += result.bytes_received
                report.from_cache += int(result.from_cache)
                report.capital_gains += result.capital_gains

                for key in keys_by_symbol[symbol]:
                    if result.bars is not None and not result.bars.empty:
                        block = result.bars.copy()
                        block.insert(0, "ticker", symbol)
                        block.insert(0, "isin", key)
                        block["currency"] = result.currency
                        price_parts.append(block)
                        report.bars += len(block)
                    if result.events is not None and not result.events.empty:
                        block = result.events.copy()
                        block.insert(0, "ticker", symbol)
                        block.insert(0, "isin", key)
                        block["currency"] = result.currency
                        action_parts.append(block)
                        report.events += len(block)

    report.seconds = time.perf_counter() - started

    empty_prices, empty_actions = _empty_frames()
    prices = (
        pd.concat(price_parts, ignore_index=True)[PRICE_COLUMNS]
        if price_parts else empty_prices
    )
    actions = (
        pd.concat(action_parts, ignore_index=True)[ACTION_COLUMNS]
        if action_parts else empty_actions
    )
    prices.attrs["report"] = actions.attrs["report"] = report

    if report.status_counts.get(429):
        # Per the recon this should be structurally impossible with a browser UA
        # over any volume, so a 429 means the gate moved, not that we sped up.
        log.warning(
            "%d ticker(s) got HTTP 429 -- the User-Agent gate has changed, "
            "check BROWSER_UA before adding sleeps", report.status_counts[429]
        )
    if report.capital_gains:
        log.warning(
            "%d capital-gain event(s) seen and dropped: schema.CORPORATE_ACTIONS "
            "has no kind for them, so their total return is understated",
            report.capital_gains,
        )
    if report.failed:
        log.info("%d/%d tickers failed, e.g. %s", len(report.failed), report.requested,
                 dict(list(report.failed.items())[:3]))
    log.info("fetch_history: %s", report.summary())
    return prices, actions


# --------------------------------------------------------------------------- #
# On-disk layout
# --------------------------------------------------------------------------- #


def bucket_of(key: str) -> int:
    """Stable shard for a fund key.

    Deliberately not `hash()`: Python salts string hashing per process, so a
    store written today would be unreadable by tomorrow's run. blake2b is stable
    across processes, versions and platforms.
    """
    digest = blake2b(str(key).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % BUCKETS


def _buckets(keys: pd.Series) -> pd.Series:
    mapping = {key: bucket_of(key) for key in keys.unique()}
    return keys.map(mapping).astype("int16")


def _bucket_file(root: Path, bucket: int) -> Path:
    return root / f"bucket={bucket:02d}" / "part.parquet"


def _to_arrow(frame: pd.DataFrame, table: str) -> pa.Table:
    prepared = frame.copy()
    # A FetchReport in .attrs is not JSON-serialisable and pyarrow would try to
    # embed it in the file's pandas metadata.
    prepared.attrs = {}
    if "date" in prepared.columns:
        prepared["date"] = pd.to_datetime(prepared["date"]).dt.date
    return schema.conform(pa.Table.from_pandas(prepared, preserve_index=False), table)


INDEX_COLUMNS = ["isin", "ticker", "first_date", "last_date", "bars", "currency", "bucket"]


def _index_frame(prices: pd.DataFrame, tickers: Mapping[str, str] | None = None) -> pd.DataFrame:
    """One row per fund: the manifest `refresh` reads instead of the whole panel.

    Scanning 49M rows to answer "what is each fund's last bar?" costs seconds on
    every run; this costs milliseconds and doubles as the export manifest.

    `ticker` lives here and nowhere else on disk, because `schema.PRICES` has no
    such column -- the ISIN is the key and the listing table owns the symbol. The
    manifest still needs it so a refresh can find its way back to the venue.
    """
    grouped = prices.groupby("isin", sort=True)
    index = grouped.agg(
        first_date=("date", "min"),
        last_date=("date", "max"),
        bars=("date", "size"),
    ).reset_index()
    if "currency" in prices.columns:
        index["currency"] = index["isin"].map(grouped["currency"].last())
    else:
        index["currency"] = None

    known = dict(tickers or {})
    if "ticker" in prices.columns:
        known.update(
            {str(k): str(v) for k, v in grouped["ticker"].last().items() if pd.notna(v)}
        )
    index["ticker"] = index["isin"].map(known)
    index["bucket"] = _buckets(index["isin"])
    return index[INDEX_COLUMNS]


def write_prices(
    prices: pd.DataFrame,
    root: Path | str = PRICES_DIR,
    *,
    mode: str = "append",
    compression: str = "zstd",
) -> dict[str, Any]:
    """Write bars into the bucketed store, idempotently.

    `mode="append"` merges into whatever is already there; a row already present
    for the same (isin, date) is replaced by the incoming one, never duplicated.
    Newest-wins rather than first-wins on purpose: the last stored bar of a
    session fetched intraday is provisional, and the point of re-fetching the
    overlap window is to let the settled figure overwrite it. Re-running with
    identical input is therefore a no-op, which is what makes the daily job safe
    to retry.
    """
    if mode not in ("append", "overwrite"):
        raise ValueError(f"unknown write mode {mode!r}")

    root = Path(root)
    prices = prices.copy()
    if prices.empty:
        return {"rows_written": 0, "buckets": 0, "bytes": 0}

    missing = prices["isin"].isna() | (prices["isin"].astype(str).str.len() == 0)
    if missing.any():
        raise ValueError(f"{int(missing.sum())} rows have no fund key to shard on")
    suspicious = prices.loc[~missing, "isin"].astype(str)
    if not suspicious.str.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]").all():
        log.warning("store keys do not all look like ISINs -- check the ticker mapping")

    prices["date"] = pd.to_datetime(prices["date"])
    prices["bucket"] = _buckets(prices["isin"])

    written = 0
    total_bytes = 0
    for bucket, block in prices.groupby("bucket", sort=True):
        path = _bucket_file(root, int(bucket))
        block = block.drop(columns="bucket")
        if mode == "append" and path.exists():
            stored = pd.read_parquet(path)
            stored["date"] = pd.to_datetime(stored["date"])
            block = pd.concat([stored, block], ignore_index=True)
        block = block.drop_duplicates(subset=["isin", "date"], keep="last")
        block = block.sort_values(["isin", "date"], kind="mergesort").reset_index(drop=True)

        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            _to_arrow(block, "prices"), path, compression=compression,
            # Row groups aligned to whole funds where possible, so a per-fund
            # read decompresses a few groups instead of the whole part.
            row_group_size=200_000,
        )
        written += len(block)
        total_bytes += path.stat().st_size

    tickers = None
    if "ticker" in prices.columns:
        pairs = prices.dropna(subset=["ticker"]).drop_duplicates("isin")
        tickers = dict(zip(pairs["isin"].astype(str), pairs["ticker"].astype(str)))
    _rebuild_index(root, tickers)
    return {"rows_written": written, "buckets": prices["bucket"].nunique(), "bytes": total_bytes}


def _rebuild_index(root: Path, tickers: Mapping[str, str] | None = None) -> pd.DataFrame:
    """Recompute the manifest from the buckets, carrying tickers forward.

    Spans come from the files, so the manifest can never disagree with them; the
    ticker column is remembered from the previous manifest because the stored
    bars do not carry it.
    """
    path = root / INDEX_NAME
    remembered: dict[str, str] = {}
    if path.exists():
        previous = pd.read_parquet(path)
        remembered = {
            str(k): str(v)
            for k, v in zip(previous["isin"], previous.get("ticker", []))
            if pd.notna(v)
        }
    remembered.update({str(k): str(v) for k, v in (tickers or {}).items() if pd.notna(v)})

    frames = [
        pd.read_parquet(part, columns=["isin", "date", "currency"])
        for part in sorted(root.glob("bucket=*/part.parquet"))
    ]
    if not frames:
        return pd.DataFrame(columns=INDEX_COLUMNS)

    index = _index_frame(pd.concat(frames, ignore_index=True), remembered)
    index.to_parquet(path, index=False)
    return index


def store_index(root: Path | str = PRICES_DIR) -> pd.DataFrame:
    """Per-fund manifest of the store: first/last bar, bar count, ticker, bucket."""
    root = Path(root)
    if root.is_file():  # a caller pointed us at a single coalesced Parquet
        return _index_frame(pd.read_parquet(root, columns=["isin", "date", "currency"]))
    path = root / INDEX_NAME
    if path.exists():
        return pd.read_parquet(path)
    if root.is_dir():
        return _rebuild_index(root)
    return pd.DataFrame(columns=INDEX_COLUMNS)


def read_prices(
    root: Path | str = PRICES_DIR,
    isins: Iterable[str] | None = None,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Read the store, optionally for a few funds only.

    Restricting to `isins` narrows the scan to the buckets those funds hash into
    -- one file out of 64 for a single fund -- which is the entire reason the
    layout hashes on the fund key.
    """
    root = Path(root)
    if root.is_file():
        frame = pd.read_parquet(root, columns=list(columns) if columns else None)
        if isins is not None:
            frame = frame[frame["isin"].isin(set(isins))]
        return frame.reset_index(drop=True)

    if not root.is_dir():
        return pd.DataFrame(columns=[f.name for f in schema.PRICES])

    if isins is None:
        paths = sorted(root.glob("bucket=*/part.parquet"))
    else:
        wanted = set(isins)
        paths = sorted(
            {_bucket_file(root, bucket_of(isin)) for isin in wanted}
        )
        paths = [p for p in paths if p.exists()]
    if not paths:
        return pd.DataFrame(columns=[f.name for f in schema.PRICES])

    dataset = pa_ds.dataset(paths, format="parquet")
    expression = (
        pa_ds.field("isin").isin(list(set(isins))) if isins is not None else None
    )
    table = dataset.to_table(columns=list(columns) if columns else None, filter=expression)
    frame = table.to_pandas()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
    order = [c for c in ("isin", "date") if c in frame.columns]
    if order:
        frame = frame.sort_values(order, kind="mergesort")
    return frame.reset_index(drop=True)


def export_single_file(
    root: Path | str = PRICES_DIR,
    dest: Path | str | None = None,
    *,
    compression: str = "zstd",
    actions: Path | str | pd.DataFrame | None = None,
    total_return: bool = True,
) -> Path:
    """Coalesce the bucketed store into one publishable Parquet.

    The bucketed form exists to stay under GitHub's 100 MiB per-file
    *repository* limit; release assets have no such limit (2 GiB per file, no
    bandwidth cap), so the published artefact is one file a consumer opens with
    a single `pd.read_parquet`. Run this after the daily job and upload with
    `gh release create prices-$(date +%F) <dest>`.

    The store on disk is raw: `adj_close` is null there, because the bars are an
    append-only record of what the venue printed and the adjustment is a derived
    view that a single new dividend restates all the way back to inception.
    Publishing it that way would ship a total-return column full of nulls, so the
    adjustment is applied here, on the way out.

    It is applied one bucket at a time, which is exact rather than approximate:
    buckets are sharded on the fund key, so a fund's entire history lives in
    exactly one of them and no adjustment ever needs to span two. That is what
    keeps peak memory at one bucket -- some tens of MB -- instead of the whole
    0.9 GB panel.
    """
    from . import adjust  # local: adjust is a consumer of this module's output

    root = Path(root)
    dest = Path(dest) if dest else processed_path("prices")
    paths = sorted(root.glob("bucket=*/part.parquet"))
    if not paths:
        raise FileNotFoundError(f"no price buckets under {root}")

    events: pd.DataFrame | None = None
    if total_return:
        if isinstance(actions, pd.DataFrame):
            events = actions
        else:
            source = Path(actions) if actions else processed_path("corporate_actions")
            if not source.exists():
                # Publishing a price-return series under the name `adj_close`
                # is the exact error this whole module exists to prevent.
                raise FileNotFoundError(
                    f"no corporate actions at {source}; pass total_return=False to "
                    "publish raw bars with a null adj_close"
                )
            events = pd.read_parquet(source)
        events["date"] = pd.to_datetime(events["date"])

    dest.parent.mkdir(parents=True, exist_ok=True)
    writer: pq.ParquetWriter | None = None
    try:
        for part in paths:
            block = pd.read_parquet(part)
            if block.empty:
                continue
            block["date"] = pd.to_datetime(block["date"])
            if events is not None:
                block = adjust.apply_adjustment(
                    block, events[events["isin"].isin(set(block["isin"]))]
                )
            table = _to_arrow(block, "prices")
            if writer is None:
                writer = pq.ParquetWriter(dest, table.schema, compression=compression)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    return dest


def write_actions(
    actions: pd.DataFrame,
    path: Path | str | None = None,
    *,
    mode: str = "append",
) -> dict[str, Any]:
    """Write corporate actions to their single (small) Parquet, idempotently.

    One file, not bucketed: 13,000 funds carry on the order of half a million
    events in total, a few MB, so this one *is* committable.
    """
    path = Path(path) if path else processed_path("corporate_actions")
    actions = actions.copy()
    if actions.empty and not path.exists():
        return {"rows_written": 0}

    if not actions.empty:
        actions["date"] = pd.to_datetime(actions["date"])
    if mode == "append" and path.exists():
        stored = pd.read_parquet(path)
        stored["date"] = pd.to_datetime(stored["date"])
        # Concatenating an all-NA empty frame would coerce the stored dtypes.
        actions = pd.concat([f for f in (stored, actions) if not f.empty], ignore_index=True)

    # (isin, date, kind) is the natural key: a fund can pay a dividend and split
    # on one day, but not pay two distinct dividends on one day.
    actions = actions.drop_duplicates(subset=["isin", "date", "kind"], keep="last")
    actions = actions.sort_values(["isin", "date", "kind"], kind="mergesort").reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(_to_arrow(actions, "corporate_actions"), path, compression="zstd")
    return {"rows_written": len(actions)}


# --------------------------------------------------------------------------- #
# Incremental refresh -- the default daily path
# --------------------------------------------------------------------------- #


def last_dates(existing: Path | str) -> dict[str, date]:
    """Last stored bar per fund, from the manifest when there is one."""
    index = store_index(existing)
    if index.empty:
        return {}
    stamps = pd.to_datetime(index["last_date"])
    return {str(k): v.date() for k, v in zip(index["isin"], stamps) if pd.notna(v)}


def refresh(
    existing_parquet: Path | str = PRICES_DIR,
    tickers: Mapping[str, str] | Iterable[str] | None = None,
    *,
    workers: int = DEFAULT_WORKERS,
    transport: Transport | None = None,
    cache: bool = False,
    progress: bool = True,
    overlap_days: int = REFRESH_OVERLAP_DAYS,
    actions_path: Path | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch only what the store is missing.

    For a fund already in the store the request starts `overlap_days` before its
    last bar; for a new fund it starts at inception. That is the difference
    between 22 MB and 4.8 GB a day, and it is why this -- not `fetch_history` --
    is what the scheduled job calls.

    Returns the newly fetched `(prices, actions)`, which the caller merges with
    `write_prices(..., mode="append")` / `write_actions`. Running it twice
    against an unchanged upstream produces an unchanged store: the overlap
    re-fetches identical bars and the merge replaces them with themselves.

    One special case forces a full re-fetch. Yahoo restates its whole price
    history the day a split takes effect, so an appended tail would be in
    post-split units while everything before it stayed in pre-split units --
    a fake 4x overnight return. Splits appearing in the overlap window are
    detected here and those tickers are re-fetched from inception.
    """
    root = Path(existing_parquet)
    pairs = _pairs(tickers) if tickers is not None else []
    if not pairs:
        index = store_index(root)
        pairs = [
            (str(k), str(t))
            for k, t in zip(index.get("isin", []), index.get("ticker", []))
            if t and pd.notna(t)
        ]
    if not pairs:
        prices, actions = _empty_frames()
        prices.attrs["report"] = actions.attrs["report"] = FetchReport()
        return prices, actions

    stored = last_dates(root)
    starts: dict[str, date] = {}
    for key, symbol in pairs:
        stamp = stored.get(key)
        if stamp is not None:
            # Earliest wins when several funds share a symbol, so nobody's tail
            # is silently skipped.
            candidate = stamp - timedelta(days=max(0, overlap_days))
            starts[symbol] = min(starts.get(symbol, candidate), candidate)

    prices, actions = fetch_history(
        dict(pairs),
        start=starts or None,
        workers=workers,
        transport=transport,
        cache=cache,
        progress=progress,
    )

    resplit = _tickers_needing_full_refetch(actions, root, actions_path)
    if resplit:
        report = prices.attrs.get("report")
        log.warning(
            "%d ticker(s) split inside the refresh window; re-fetching their full "
            "history because the vendor restates it: %s",
            len(resplit), sorted(resplit)[:5],
        )
        keys = {key: symbol for key, symbol in pairs if symbol in resplit}
        full_prices, full_actions = fetch_history(
            keys, start=None, workers=workers, transport=transport,
            cache=False, progress=progress,
        )
        if not full_prices.empty:
            prices = pd.concat(
                [prices[~prices["ticker"].isin(resplit)], full_prices], ignore_index=True
            )
        if not full_actions.empty:
            actions = pd.concat(
                [actions[~actions["ticker"].isin(resplit)], full_actions], ignore_index=True
            )
        # concat drops attrs; the incremental pass is still what the run did, so
        # its report is what gets reattached rather than the re-fetch's.
        if report is not None:
            report.bars = len(prices)
            report.events = len(actions)
            prices.attrs["report"] = actions.attrs["report"] = report

    return prices, actions


def _tickers_needing_full_refetch(
    actions: pd.DataFrame, root: Path, actions_path: Path | str | None
) -> set[str]:
    """Symbols with a split event we had not already recorded."""
    if actions.empty or "kind" not in actions.columns:
        return set()
    splits = actions[actions["kind"] == "split"]
    if splits.empty:
        return set()

    path = Path(actions_path) if actions_path else processed_path("corporate_actions")
    known: set[tuple[str, Any]] = set()
    if path.exists():
        stored = pd.read_parquet(path)
        stored = stored[stored["kind"] == "split"]
        known = {
            (str(i), pd.Timestamp(d).normalize())
            for i, d in zip(stored["isin"], pd.to_datetime(stored["date"]))
        }

    fresh = splits[
        [
            (str(i), pd.Timestamp(d).normalize()) not in known
            for i, d in zip(splits["isin"], pd.to_datetime(splits["date"]))
        ]
    ]
    return set(fresh["ticker"].astype(str))
