"""Shared plumbing every source adapter sits on.

Three things live here, and they are here because the package contract forbids
the obvious alternative: adapters must not import each other, so that a broken
module cannot take its neighbours down with it. Anything two adapters need
therefore has to live in a module that is not itself a source.

1. **HTTP.** One pooled session, one User-Agent, one retry policy, one throttle.
   Every endpoint in this package is a free public service; hammering them with
   uncoordinated requests is how an open pipeline gets a whole IP range blocked.

2. **An on-disk cache** under ``data/raw/<source>/``, keyed by the full request
   (method, URL, query, body). Re-running the build should not re-download 3.5 MB
   of ESMA zip or 46 pages of SIX; ``refresh=True`` is the deliberate opt-out.

3. **The two conversions every adapter performs on the way out**: validating
   ISINs against the ISO 6166 check digit, and conforming a pile of dicts to the
   declared PyArrow schema. Plus the `is_primary` rule, which is shared
   deliberately -- five adapters agreeing on a rule matters more than five
   adapters each having a copy of it that can drift.

Retry policy
------------
Backoff is exponential and applies only to failures that plausibly heal: network
errors, timeouts, 429 and 5xx. A 404 is *not* retried. Half of these URLs carry
a rotating content hash or an incrementing month index, so a 404 means "this URL
is gone, go rediscover it" -- retrying it four times only delays the truthful
failure by twenty seconds.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

import pandas as pd
import pyarrow as pa
import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from .. import schema
from ..config import HTTP_RETRIES, HTTP_TIMEOUT, RAW, USER_AGENT
from . import SourceResult

log = logging.getLogger(__name__)

# Statuses worth retrying: transient server trouble and explicit rate limiting.
# Everything else -- 401, 403, 404, 410 -- is a decision the server made about
# this request, and it will make the same one again.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

# Minimum gap between two requests to the same host. SIX needs ~50 sequential
# pages and the LSE ~6; at this spacing that is a rounding error to them and
# visibly polite in their logs.
MIN_INTERVAL = 0.5

# Extensions kept as-is in cache filenames, purely so a human poking around
# data/raw/ can open the file with the right tool.
_KNOWN_SUFFIXES = frozenset({".zip", ".csv", ".json", ".xml", ".xlsx", ".xls", ".txt"})

_SESSION: requests.Session | None = None
_LAST_CALL: dict[str, float] = {}
_LOCK = threading.Lock()


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #


def session() -> requests.Session:
    """The one connection-pooled session the whole package shares."""
    global _SESSION
    with _LOCK:
        if _SESSION is None:
            _SESSION = requests.Session()
            _SESSION.headers["User-Agent"] = USER_AGENT
        return _SESSION


def _throttle(host: str, min_interval: float) -> None:
    """Space out requests to one host.

    The sleep happens while holding the lock on purpose: the point is that the
    pipeline as a whole never exceeds the rate, not that each thread does.
    """
    if min_interval <= 0:
        return
    with _LOCK:
        previous = _LAST_CALL.get(host)
        now = time.monotonic()
        if previous is not None:
            pause = min_interval - (now - previous)
            if pause > 0:
                time.sleep(pause)
                now = time.monotonic()
        _LAST_CALL[host] = now


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in RETRYABLE_STATUS
    return False


@retry(
    stop=stop_after_attempt(HTTP_RETRIES),
    wait=wait_exponential(multiplier=1, max=30),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
def get(
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    method: str = "GET",
    json_body: Any | None = None,
    data: Any | None = None,
    timeout: int = HTTP_TIMEOUT,
    min_interval: float = MIN_INTERVAL,
    stream: bool = False,
) -> requests.Response:
    """Perform one throttled, retried request and raise on an error status."""
    _throttle(urlsplit(url).netloc, min_interval)
    response = session().request(
        method,
        url,
        params=params,
        headers=dict(headers or {}),
        json=json_body,
        data=data,
        timeout=timeout,
        stream=stream,
    )
    response.raise_for_status()
    return response


# --------------------------------------------------------------------------- #
# Cache
# --------------------------------------------------------------------------- #


def _cache_key(
    url: str,
    params: Mapping[str, Any] | None,
    method: str,
    json_body: Any | None,
    data: Any | None,
) -> str:
    payload = json.dumps(
        [
            method.upper(),
            url,
            sorted((str(k), str(v)) for k, v in (params or {}).items()),
            json_body,
            data if isinstance(data, (str, type(None))) else repr(data),
        ],
        sort_keys=True,
        default=str,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def cache_path(
    source: str,
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    method: str = "GET",
    json_body: Any | None = None,
    data: Any | None = None,
) -> Path:
    """Where a given request's bytes live on disk.

    The name carries a readable slug *and* a hash: the slug so the directory is
    browsable, the hash so two different query strings against the same path
    (SIX pages, LSE pages) never collide.
    """
    suffix = Path(urlsplit(url).path).suffix.lower()
    if suffix not in _KNOWN_SUFFIXES:
        suffix = ".bin"
    slug = re.sub(r"[^A-Za-z0-9]+", "-", urlsplit(url).path).strip("-")[-60:] or "root"
    key = _cache_key(url, params, method, json_body, data)
    return RAW / source / f"{slug}-{key}{suffix}"


def cached_file(
    source: str,
    url: str,
    *,
    refresh: bool = False,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    method: str = "GET",
    json_body: Any | None = None,
    data: Any | None = None,
    timeout: int = HTTP_TIMEOUT,
    min_interval: float = MIN_INTERVAL,
) -> Path:
    """Return the cached path for a request, downloading it if need be.

    Streams to a `.part` file and renames on completion, so an interrupted run
    leaves no truncated file that the next run would happily parse as complete.
    """
    path = cache_path(
        source, url, params=params, method=method, json_body=json_body, data=data
    )
    if path.exists() and not refresh:
        return path

    response = get(
        url,
        params=params,
        headers=headers,
        method=method,
        json_body=json_body,
        data=data,
        timeout=timeout,
        min_interval=min_interval,
        stream=True,
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    with partial.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1 << 16):
            handle.write(chunk)
    partial.replace(path)

    path.with_name(path.name + ".meta.json").write_text(
        json.dumps(
            {
                "url": response.url,
                "method": method.upper(),
                "status": response.status_code,
                "bytes": path.stat().st_size,
                "fetched_at": timestamp(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def get_cached(source: str, url: str, **kwargs: Any) -> bytes:
    """The cached bytes of a request. See `cached_file` for the arguments."""
    return cached_file(source, url, **kwargs).read_bytes()


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Identifiers
# --------------------------------------------------------------------------- #

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def valid_isin(value: object) -> bool:
    """True when `value` is a structurally valid ISIN with a correct check digit.

    ISO 6166: two letters, nine alphanumerics, one check digit, the whole thing
    expanded letter-by-letter into digits (A=10 ... Z=35) and verified with Luhn.
    Exchange files do carry malformed identifiers -- placeholder rows, internal
    codes, the occasional transposed character -- and a bad ISIN is worse than a
    missing one, because it silently becomes a fund nobody can ever match.
    """
    if not isinstance(value, str):
        return False
    code = value.strip()
    if not _ISIN_RE.match(code):
        return False

    digits = "".join(str(int(char, 36)) for char in code)
    total = 0
    for position, char in enumerate(reversed(digits)):
        digit = ord(char) - 48
        if position % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


def drop_invalid_isins(
    frame: pd.DataFrame, column: str = "isin"
) -> tuple[pd.DataFrame, list[str]]:
    """Split a frame into rows with a usable ISIN and the identifiers rejected.

    Returned rather than logged so the adapter can report a count: a source that
    suddenly rejects 30% of its rows has changed shape, and that must be visible
    in the build log rather than showing up as a quietly smaller universe.
    """
    if frame.empty:
        return frame, []
    keep = frame[column].map(valid_isin)
    return frame[keep].copy(), sorted(set(frame.loc[~keep, column].astype(str)))


# --------------------------------------------------------------------------- #
# The is_primary rule
# --------------------------------------------------------------------------- #


def primary_flags(
    isins: Sequence[str],
    mics: Sequence[str],
    *,
    mic_country: Mapping[str, str],
    preference: Sequence[str],
) -> list[bool]:
    """Flag exactly one listing per ISIN as primary, deterministically.

    The rule, in order:

    1. a venue in the fund's own country wins -- an Irish-domiciled fund's home
       listing is the one whose price series the issuer itself references;
    2. otherwise the first venue in `preference`, a fixed list per adapter;
    3. otherwise the alphabetically first MIC, so an unlisted-in-preference
       venue still yields a stable answer.

    "The fund's country" is read off the ISIN prefix, which is the issuing CSD's
    country -- for the UCITS universe (IE, LU, FR, DE) that is the domicile.

    Selection is a minimum over the ISIN's rows, never a scan that stops at the
    first hit, so the answer does not depend on the order the source happened to
    return its rows in. Callers must de-duplicate (ISIN, MIC) first: two rows
    with the identical key would both be flagged.
    """
    rank = {mic: index for index, mic in enumerate(preference)}
    unranked = len(rank)

    keys = [
        (
            0 if mic_country.get(mic) == isin[:2] else 1,
            rank.get(mic, unranked),
            mic,
        )
        for isin, mic in zip(isins, mics)
    ]

    best: dict[str, tuple[int, int, str]] = {}
    for isin, key in zip(isins, keys):
        if isin not in best or key < best[isin]:
            best[isin] = key
    return [best[isin] == key for isin, key in zip(isins, keys)]


# --------------------------------------------------------------------------- #
# Frames
# --------------------------------------------------------------------------- #


def to_frame(rows: Sequence[Mapping[str, Any]] | pd.DataFrame, table: str) -> pd.DataFrame:
    """Conform an adapter's rows to a declared schema, filling absentees with null.

    Adapters assemble whatever columns their upstream actually carries; this is
    the single place that turns that into the exact column set, order and dtypes
    of `schema.TABLES[table]`, so the merge stage can concatenate frames from
    five sources without inspecting any of them.

    An undeclared column is a programming error and raises, rather than being
    silently dropped -- a typo'd column name would otherwise look like an
    upstream that stopped carrying the field.
    """
    declared = [field.name for field in schema.TABLES[table]]
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))

    unknown = sorted(set(map(str, frame.columns)) - set(declared))
    if unknown:
        raise KeyError(f"columns not declared in schema.{table.upper()}: {unknown}")

    arrow = pa.Table.from_pandas(frame.reset_index(drop=True), preserve_index=False)
    return schema.conform(arrow, table).to_pandas()


def empty_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    """The (funds, listings) pair a failed fetch returns."""
    return to_frame([], "funds"), to_frame([], "listings")


def failed(name: str, error: str) -> SourceResult:
    """Build the fail-soft result. Every adapter's `except` clause ends here.

    A dead upstream degrades the dataset; it does not abort the build. The error
    string travels with the result so the build log can say which source went
    missing and why, instead of the universe silently shrinking.
    """
    log.warning("source %s failed: %s", name, error)
    funds, listings = empty_frames()
    return SourceResult(
        name=name,
        funds=funds,
        listings=listings,
        ok=False,
        error=error,
        fetched_at=timestamp(),
    )
