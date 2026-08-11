"""GLEIF: who actually runs the fund, resolved offline from the golden copy.

An ETF's regulator-published issuer identifier is the sub-fund's own LEI, not
its manager's -- ESMA FIRDS gives 5,505 distinct LEIs for 7,810 ETFs, and those
LEIs name things like "iShares Core MSCI World UCITS ETF", which is the fund
again, not BlackRock. The fact worth having is the `IS_FUND-MANAGED_BY`
relationship, and GLEIF publishes every one of them in a single 23 MB file.

Why bulk files rather than the REST API
---------------------------------------
api.gleif.org answers `/lei-records/{lei}/fund-manager` correctly, but the
universe has thousands of fund LEIs, and thousands of round trips to a free
public service to learn something that ships as one download is neither fast
nor polite. Two files answer it entirely offline:

    rr    484k relationships    23 MB   fund LEI -> manager LEI
    lei2  3.4M legal entities  475 MB   manager LEI -> legal name

Measured during the source recon: 94.1% of ETFs resolve to a fund manager this
way. Trust is 100 because this is the issuer's own filed, verified identity --
GLEIF is the global authority on which legal entity is which, and its data is
CC0, the most permissive licence of any source in this pipeline.

Only `IS_FUND-MANAGED_BY` is followed. `IS_SUBFUND_OF` would raise coverage by
pointing at the umbrella (iShares III plc), but an umbrella and a manager are
different facts and mixing them into one column would make `issuer` mean two
things at once. A fund with no managed-by relationship keeps a null issuer.

The half-gigabyte lei2 file is streamed to disk and read in chunks -- never
held in memory -- and is not re-downloaded unless `refresh=True`. Legal names
change on the order of years, so a copy a few days old misnames nobody, and
asking a free service for 475 MB on every build would not be reasonable.
"""

from __future__ import annotations

import json
import logging
import zipfile
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from ..config import RAW, processed_path
from . import SourceResult
from ._http import empty_frames, failed, get, timestamp, to_frame

log = logging.getLogger(__name__)

NAME = "gleif"
TRUST = 100

GOLDEN_COPY_PUBLISHES_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"
ISIN_LEI_INDEX_URL = "https://mapping.gleif.org/api/v2/isin-lei"

CACHE_DIR = RAW / NAME

FUND_MANAGER = "IS_FUND-MANAGED_BY"
_ACTIVE = "ACTIVE"

_RR_COLUMNS = {
    "Relationship.StartNode.NodeID": "fund_lei",
    "Relationship.EndNode.NodeID": "manager_lei",
    "Relationship.RelationshipType": "relationship",
    "Relationship.RelationshipStatus": "status",
}

_LEI2_COLUMNS = {"LEI": "lei", "Entity.LegalName": "legal_name"}

# 3.4M entities do not fit comfortably in one frame alongside everything else a
# build holds, and only a few hundred of them are ever wanted.
_CHUNK_ROWS = 500_000


# --------------------------------------------------------------------------- #
# Bulk file retrieval
# --------------------------------------------------------------------------- #


def _get_json(url: str, params: dict | None = None) -> dict:
    """GET a small JSON index.

    `Accept: */*` is deliberate: mapping.gleif.org answers `Accept:
    application/json` with HTTP 406.
    """
    return get(url, params=params, headers={"Accept": "*/*"}).json()


def _stream_to(url: str, destination: Path) -> Path:
    """Download `url` to `destination` without holding it in memory.

    `_http.cached_file` is not used for these: it keys the cache on the URL, and
    GLEIF stamps the publication date into every golden-copy URL, so a build
    would re-download half a gigabyte daily to obtain a file whose contents
    change on the order of years. The stable name below is the cache key
    instead, and `refresh=True` is what asks for a newer copy.

    Writes to a sibling `.part` and renames on success, so an interrupted
    download can never be mistaken for a complete cache entry.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")

    with get(url, stream=True) as response:
        with partial.open("wb") as handle:
            for block in response.iter_content(chunk_size=1 << 20):
                handle.write(block)

    partial.replace(destination)
    # The stable filename hides which publication this is; the sidecar answers
    # "how old is the copy I am joining against?" without a re-download.
    destination.with_name(destination.name + ".meta.json").write_text(
        json.dumps(
            {"url": url, "bytes": destination.stat().st_size, "fetched_at": timestamp()},
            indent=2,
        ),
        encoding="utf-8",
    )
    return destination


def golden_copy(kind: str, refresh: bool = False) -> Path:
    """Path to the cached `lei2` or `rr` golden copy, downloading if needed."""
    destination = CACHE_DIR / f"{kind}-golden-copy.csv.zip"
    if destination.exists() and not refresh:
        return destination

    index = _get_json(GOLDEN_COPY_PUBLISHES_URL, {"format": "json", "page[size]": 1})
    publish = index["data"][0]
    url = publish[kind]["full_file"]["csv"]["url"]
    log.info("downloading GLEIF %s golden copy (%s)", kind, publish.get("publish_date"))
    return _stream_to(url, destination)


def isin_lei_file(refresh: bool = False) -> Path:
    """Path to the cached GLEIF ISIN->LEI mapping, downloading if needed."""
    destination = CACHE_DIR / "isin-lei.csv.zip"
    if destination.exists() and not refresh:
        return destination

    index = _get_json(ISIN_LEI_INDEX_URL, {"page[size]": 1})
    url = index["data"][0]["attributes"]["downloadLink"]
    return _stream_to(url, destination)


def _read_zip_csv(path: Path, columns: Mapping[str, str], **kwargs) -> Iterable[pd.DataFrame]:
    """Iterate a single-member zipped CSV in chunks, keeping `columns` only."""
    with zipfile.ZipFile(path) as archive:
        member = next(n for n in archive.namelist() if n.lower().endswith(".csv"))
        with archive.open(member) as handle:
            reader = pd.read_csv(
                handle,
                usecols=list(columns),
                dtype=str,
                chunksize=_CHUNK_ROWS,
                **kwargs,
            )
            for chunk in reader:
                yield chunk.rename(columns=dict(columns))


# --------------------------------------------------------------------------- #
# Relationships and names
# --------------------------------------------------------------------------- #


def fund_manager_leis(fund_leis: Iterable[str], refresh: bool = False) -> dict[str, str]:
    """Map fund LEI -> managing entity LEI for the LEIs asked about.

    Only ACTIVE relationships count: a lapsed one records that a manager used to
    run the fund, which is not what the database means by `issuer`.
    """
    wanted = {lei for lei in fund_leis if isinstance(lei, str) and lei}
    if not wanted:
        return {}

    path = golden_copy("rr", refresh)
    managers: dict[str, str] = {}
    for chunk in _read_zip_csv(path, _RR_COLUMNS):
        hit = chunk[
            (chunk["relationship"] == FUND_MANAGER)
            & (chunk["status"] == _ACTIVE)
            & chunk["fund_lei"].isin(wanted)
        ]
        for fund_lei, manager_lei in zip(hit["fund_lei"], hit["manager_lei"]):
            managers.setdefault(fund_lei, manager_lei)
    return managers


def legal_names(leis: Iterable[str], refresh: bool = False) -> dict[str, str]:
    """Map LEI -> registered legal name from the lei2 golden copy."""
    wanted = {lei for lei in leis if isinstance(lei, str) and lei}
    if not wanted:
        return {}

    path = golden_copy("lei2", refresh)
    names: dict[str, str] = {}
    for chunk in _read_zip_csv(path, _LEI2_COLUMNS):
        hit = chunk[chunk["lei"].isin(wanted)]
        names.update(dict(zip(hit["lei"], hit["legal_name"])))
        if len(names) == len(wanted):
            break
    return names


def isin_leis(isins: Iterable[str], refresh: bool = False) -> dict[str, str]:
    """Map ISIN -> LEI from GLEIF's own mapping file.

    The fallback for callers that have ISINs but no LEI. Coverage is
    issuer-dependent and thinner than a regulator's register -- SPY and QQQ are
    in it, IVV and VOO are not -- so a source that already knows the LEI (ESMA
    FIRDS does) should pass it rather than rely on this.
    """
    wanted = {isin for isin in isins if isinstance(isin, str) and isin}
    if not wanted:
        return {}

    path = isin_lei_file(refresh)
    found: dict[str, str] = {}
    for chunk in _read_zip_csv(path, {"LEI": "lei", "ISIN": "isin"}):
        hit = chunk[chunk["isin"].isin(wanted)]
        for isin, lei in zip(hit["isin"], hit["lei"]):
            found.setdefault(isin, lei)
    return found


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #


def _universe_isins() -> list[str]:
    """ISINs already in the database; an enrichment source may add nothing else."""
    path = processed_path("funds")
    if not path.exists():
        return []
    funds = pd.read_parquet(path, columns=["isin"])
    return [isin for isin in funds["isin"].dropna().unique() if not isin.startswith("US:")]


def fetch(
    refresh: bool = False,
    isin_leis_map: Mapping[str, str] | None = None,
    isins: Iterable[str] | None = None,
) -> SourceResult:
    """Resolve issuer names for the universe. Never raises on an upstream failure.

    `isin_leis_map` is the good path -- ISIN -> the fund's own LEI, straight out
    of a regulator's register. Without it the ISINs are looked up in GLEIF's
    ISIN->LEI file first, at lower coverage.
    """
    stats: dict[str, object] = {}
    empty_funds, no_listings = empty_frames()

    try:
        mapping = dict(isin_leis_map) if isin_leis_map else {}
        if not mapping:
            wanted = list(isins) if isins is not None else _universe_isins()
            stats["isins_requested"] = len(wanted)
            if not wanted:
                stats["note"] = "no ISINs to enrich"
                return SourceResult(
                    name=NAME,
                    funds=empty_funds,
                    listings=no_listings,
                    fetched_at=timestamp(),
                    stats=stats,
                )
            mapping = isin_leis(wanted, refresh)

        stats.setdefault("isins_requested", len(mapping))
        stats["fund_leis"] = len(set(mapping.values()))

        managers = fund_manager_leis(set(mapping.values()), refresh)
        stats["funds_with_manager"] = sum(1 for lei in mapping.values() if lei in managers)

        names = legal_names(set(managers.values()), refresh)
        stats["managers"] = len(set(managers.values()))
        stats["managers_named"] = len(names)

        issuer_by_isin = {
            isin: names[managers[lei]]
            for isin, lei in mapping.items()
            if lei in managers and managers[lei] in names
        }
    except Exception as exc:  # network, layout change, unreadable archive
        return failed(NAME, f"{type(exc).__name__}: {exc}")

    funds = to_frame(
        pd.DataFrame(
            {
                "isin": list(issuer_by_isin),
                "issuer": list(issuer_by_isin.values()),
                "data_sources": [[NAME]] * len(issuer_by_isin),
                "last_updated": date.today(),
            }
        ),
        "funds",
    )
    stats["funds"] = int(len(funds))
    stats["resolution_rate"] = round(len(funds) / max(1, len(mapping)), 4)

    return SourceResult(
        name=NAME,
        funds=funds,
        listings=no_listings,
        fetched_at=timestamp(),
        stats=stats,
    )
