"""ESMA FIRDS `FULINS_C` -- the European backbone of the universe.

Why this source is first
------------------------
Every EU trading venue is legally obliged under MiFIR Art. 27 to report its
instrument reference data to ESMA, and ESMA republishes it weekly, free, without
a key, under a licence that explicitly permits reuse. That makes FIRDS the only
source here that is neither a vendor's opinion nor a single venue's slice: it is
the register. Hence TRUST 100, and hence the identity fields (ISIN, name, CFI)
of any fund FIRDS knows win over every exchange file.

`FULINS` is split by CFI category letter and `C` is Collective Investment
Vehicles, so one 3.5 MB zip contains the entire EU fund universe -- 7.8k ETF
ISINs. The other ~38 weekly parts (debt, equity, futures...) are irrelevant here.

Three design decisions worth defending
--------------------------------------
**Discovery, not a hardcoded filename.** The file is named after its publication
date (`FULINS_C_20260808_01of01.zip`). Hardcoding one means the pipeline silently
serves week-old data until someone notices, so the current file is discovered
through ESMA's public Solr index every run. The Solr call deliberately bypasses
the cache -- caching a discovery request would defeat its only purpose.

**Streaming, not a DOM.** The zip expands to ~87 MB of XML across ~144k records.
`iterparse` with the element cleared (and unlinked from its parent) after each
record keeps memory flat; `ElementTree.parse` would build the whole tree.

**The RMKT filter is not optional.** FIRDS yields ~102k (ISIN, MIC) pairs, but
~82% of them are MTF and OTC reporting venues -- Bloomberg's BTFE, Tradeweb's
TWEM -- not places a retail investor can send an order. Joining the ISO 10383
register and keeping only `MARKET CATEGORY CODE = RMKT` cuts that to ~18k
genuine regulated-market listings. Without the join the listings table is ten
times too large and mostly fiction. If the MIC register cannot be fetched the
adapter fails rather than publishing the unfiltered pairs: 102k wrong rows are
worse than none.

`is_primary`
------------
Venue in the fund's ISIN-prefix country first, then the fixed `MIC_PREFERENCE`
order below, then alphabetical MIC. See `_http.primary_flags`.

What is deliberately left null
------------------------------
* **ticker** -- FIRDS has none. `ShrtNm` is an ISO 18774 FISN abbreviation, not a
  tradeable symbol; treating it as one would poison every ticker join downstream.
* **fund_currency** -- `NtnlCcy` is reported per venue, so it is the *quote*
  currency of that listing, not the fund's base currency: the same ISIN comes
  back USD from one venue and EUR from another. It goes in `listings`, and
  `funds.fund_currency` stays null for a source better placed to answer.
* **issuer** -- FIRDS gives the sub-fund's own LEI, not a name, and the schema
  has no LEI column. The map is written next to the cache as
  `issuer_lei.json` for the GLEIF stage, which resolves LEI to manager name.
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from datetime import date
from typing import Any, IO, Iterator

import pandas as pd
from lxml import etree

from ..config import RAW
from . import SourceResult
from ._http import (
    cached_file,
    drop_invalid_isins,
    failed,
    get,
    get_cached,
    primary_flags,
    timestamp,
    to_frame,
)

log = logging.getLogger(__name__)

NAME = "firds"
TRUST = 100

SOLR_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
MIC_REGISTER_URL = (
    "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"
)

# CFI category C, group E = exchange traded funds. This one prefix is the whole
# ETF filter; the rest of FULINS_C is CI (standard funds), CM, CB (REITs)...
ETF_CFI_PREFIX = "CE"

# ISO 10383 market category for a regulated market. MLTF (MTFs), SINT
# (systematic internalisers) and OTFS are venues in the legal sense and noise in
# the retail sense.
REGULATED_CATEGORY = "RMKT"

# Fixed tie-break for is_primary once the domicile rule has not decided. Order is
# "where would a European retail holder most plausibly look up this fund's
# price": Paris first for a French audience, then the other Euronext books, the
# Irish and Italian regulated markets, then Germany's several Xetra/Frankfurt
# segment MICs, then the Nordics.
MIC_PREFERENCE = (
    "XPAR",
    "XAMS",
    "XBRU",
    "XLIS",
    "XMSM",
    "ETFP",
    "MTAA",
    "XETA",
    "XETU",
    "FRAA",
    "FRAU",
    "XLUX",
    "XCSE",
    "XSTO",
    "XPMC",
    "XAMC",
)

# ISIN prefixes that are not countries: XS is the international CSDs (Euroclear
# and Clearstream), EU is the European institutions. Everything else in an ISIN
# prefix position is an ISO 3166 alpha-2 assigned to a national numbering agency.
NON_COUNTRY_ISIN_PREFIXES = frozenset({"XS", "EU", "QS", "QT", "QZ", "XA", "XF"})

# ISO 10962:2019, group CE. Character 4 is the distribution policy and character
# 5 the asset the fund holds. Issuers self-report these and a visible minority
# report "M -- other" for a plain equity tracker, which is exactly why anything
# not in these maps becomes "unknown" instead of a guess.
CFI_DISTRIBUTION = {"I": "distributing", "G": "accumulating"}  # J = mixed
CFI_ASSET_CLASS = {
    "E": "equity",
    "B": "bond",
    "V": "bond",  # convertibles: a debt instrument with an equity option
    "C": "commodity",
    "R": "real-estate",
    "F": "currency",
    "L": "multi-asset",
}

# The fields `iter_records` pulls out of one <RefData> element, in order.
RECORD_COLUMNS = [
    "isin",
    "name",
    "short_name",
    "cfi",
    "currency",
    "issuer_lei",
    "mic",
    "first_trade_date",
    "termination_date",
]

_ISSUER_LEI_PATH = RAW / NAME / "issuer_lei.json"


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #


def latest_file(rows: int = 200) -> dict[str, Any]:
    """The most recently published `FULINS_C` document, from ESMA's Solr index.

    Not cached: the single question this call exists to answer is "has a newer
    file been published?", and a cached answer is always "no".
    """
    response = get(
        SOLR_URL,
        params={
            "q": "*",
            "fq": "file_type:FULINS",
            "wt": "json",
            "rows": str(rows),
            "sort": "publication_date desc",
        },
        timeout=120,
    )
    documents = response.json()["response"]["docs"]
    candidates = [
        document
        for document in documents
        if str(document.get("file_name", "")).startswith("FULINS_C")
    ]
    if not candidates:
        raise LookupError(
            f"no FULINS_C file among {len(documents)} FULINS documents returned by Solr"
        )
    return max(candidates, key=lambda document: document["publication_date"])


def regulated_markets(refresh: bool = False) -> pd.DataFrame:
    """The ISO 10383 register, reduced to the regulated markets."""
    return parse_register(get_cached("mic", MIC_REGISTER_URL, refresh=refresh))


def parse_register(raw: bytes) -> pd.DataFrame:
    """Reduce the ISO 10383 CSV to `mic`, `venue_name`, `venue_country`.

    Segment MICs are kept alongside their operating MIC because FIRDS reports at
    segment level -- `XETA` and `XETU`, never `XETR` -- so collapsing to
    operating MICs would drop the join for Germany entirely.
    """
    register = pd.read_csv(
        io.BytesIO(raw),
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
        encoding_errors="replace",  # a handful of market names carry cp1252 dashes
    )

    required = {"MIC", "MARKET CATEGORY CODE", "ISO COUNTRY CODE (ISO 3166)"}
    missing = required - set(register.columns)
    if missing:
        raise KeyError(f"ISO 10383 register is missing columns: {sorted(missing)}")

    regulated = register[register["MARKET CATEGORY CODE"] == REGULATED_CATEGORY]
    return pd.DataFrame(
        {
            "mic": regulated["MIC"].str.strip(),
            "venue_name": regulated[
                "MARKET NAME-INSTITUTION DESCRIPTION"
            ].str.strip(),
            "venue_country": regulated["ISO COUNTRY CODE (ISO 3166)"].str.strip(),
        }
    ).drop_duplicates("mic")


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #


def iter_records(stream: IO[bytes]) -> Iterator[dict[str, str | None]]:
    """Yield one dict per `<RefData>` element, holding no more than one in memory.

    The namespace is read off the matched element rather than hardcoded, so an
    ISO 20022 version bump (`auth.017.001.02` -> `.03`) changes nothing here.
    """
    context = etree.iterparse(stream, events=("end",), tag="{*}RefData")
    for _event, element in context:
        namespace = element.tag[: element.tag.index("}") + 1]
        general = element.find(namespace + "FinInstrmGnlAttrbts")
        venue = element.find(namespace + "TradgVnRltdAttrbts")

        yield {
            "isin": _text(general, namespace + "Id"),
            "name": _text(general, namespace + "FullNm"),
            "short_name": _text(general, namespace + "ShrtNm"),
            "cfi": _text(general, namespace + "ClssfctnTp"),
            "currency": _text(general, namespace + "NtnlCcy"),
            "issuer_lei": _text(element, namespace + "Issr"),
            "mic": _text(venue, namespace + "Id"),
            "first_trade_date": _text(venue, namespace + "FrstTradDt"),
            "termination_date": _text(venue, namespace + "TermntnDt"),
        }

        # Free the record, then unlink the emptied shells the parser has already
        # appended to the parent: clearing alone leaves 144k husks behind.
        element.clear()
        while element.getprevious() is not None:
            del element.getparent()[0]


def _text(parent: Any, path: str) -> str | None:
    if parent is None:
        return None
    node = parent.find(path)
    if node is None or node.text is None:
        return None
    return node.text.strip() or None


def _domicile(isin: str) -> str | None:
    prefix = isin[:2]
    return None if prefix in NON_COUNTRY_ISIN_PREFIXES else prefix


def build(
    records: Iterator[dict[str, str | None]], markets: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Turn a stream of FIRDS records into the two schema frames.

    Kept separate from `fetch` so the whole transformation -- the CFI filter, the
    RMKT join, the vocabulary mapping, the primary rule -- is testable against a
    small XML fixture without touching the network.
    """
    stats: dict[str, Any] = {"records": 0, "etf_records": 0}
    rows: list[dict[str, str | None]] = []

    for record in records:
        stats["records"] += 1
        cfi = record["cfi"] or ""
        if not cfi.startswith(ETF_CFI_PREFIX) or not record["isin"]:
            continue
        stats["etf_records"] += 1
        rows.append(record)

    frame = pd.DataFrame(rows, columns=RECORD_COLUMNS)
    frame, rejected = drop_invalid_isins(frame)
    stats["invalid_isin_dropped"] = len(rejected)
    stats["invalid_isin_examples"] = rejected[:5]

    # One record per (ISIN, MIC); the sort makes every downstream pick -- which
    # name wins, which venue is primary -- independent of ESMA's file order.
    frame = frame.sort_values(["isin", "mic"], kind="mergesort").drop_duplicates(
        ["isin", "mic"]
    )
    stats["etf_isins"] = int(frame["isin"].nunique())

    funds = _funds(frame)
    listings, listing_stats = _listings(frame, markets)
    stats.update(listing_stats)
    stats["isins_with_issuer_lei"] = int(
        frame.loc[frame["issuer_lei"].notna(), "isin"].nunique()
    )

    return funds, listings, stats



def _funds(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per ISIN, each field taking its first non-null value in MIC order.

    Venues occasionally disagree about a fund's name and even its CFI. Reading in
    MIC order is arbitrary but stable, which is what matters: the alternative --
    whichever record the file happened to list first -- makes the published name
    flap between runs for no reason a reader could ever explain.
    """
    first = frame.groupby("isin", sort=True).first().reset_index()
    cfi = first["cfi"].fillna("")

    return to_frame(
        pd.DataFrame(
            {
                "isin": first["isin"],
                "name": first["name"],
                "short_name": first["short_name"],
                "domicile": first["isin"].map(_domicile),
                "distribution_policy": cfi.str[3]
                .map(CFI_DISTRIBUTION)
                .fillna("unknown"),
                "asset_class": cfi.str[4].map(CFI_ASSET_CLASS).fillna("unknown"),
                "data_sources": [[NAME]] * len(first),
                "last_updated": date.today(),
            }
        ),
        "funds",
    )


def _listings(
    frame: pd.DataFrame, markets: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    joined = frame.merge(markets, on="mic", how="inner")

    stats = {
        "pairs_total": int(len(frame)),
        "pairs_dropped_not_regulated": int(len(frame) - len(joined)),
        "listings": int(len(joined)),
        "listing_isins": int(joined["isin"].nunique()) if len(joined) else 0,
        # Reported, not filtered: a terminated venue line is still what the
        # register says, and dropping it here would hide a delisting the merge
        # stage is better placed to record.
        "listings_terminated": int(joined["termination_date"].notna().sum())
        if len(joined)
        else 0,
    }

    listings = to_frame(
        pd.DataFrame(
            {
                "isin": joined["isin"],
                "exchange_mic": joined["mic"],
                "exchange_name": joined["venue_name"],
                "trading_currency": joined["currency"],
                "is_primary": primary_flags(
                    list(joined["isin"]),
                    list(joined["mic"]),
                    mic_country=dict(zip(markets["mic"], markets["venue_country"])),
                    preference=MIC_PREFERENCE,
                ),
            }
        ),
        "listings",
    )
    return listings, stats


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #


def fetch(refresh: bool = False) -> SourceResult:
    """Download, filter and shape the current FULINS_C. Never raises."""
    try:
        document = latest_file()
        markets = regulated_markets(refresh=refresh)

        # The download URL carries the publication date, so a new week is a new
        # cache key: the cache expires itself without any TTL bookkeeping.
        archive = cached_file(
            NAME, document["download_link"], refresh=refresh, timeout=600
        )

        issuer_leis: dict[str, str] = {}
        with zipfile.ZipFile(archive) as bundle:
            member = next(
                name for name in bundle.namelist() if name.lower().endswith(".xml")
            )
            with bundle.open(member) as stream:
                funds, listings, stats = build(
                    _capture_issuer_leis(iter_records(stream), issuer_leis), markets
                )

        stats["file_name"] = document["file_name"]
        stats["published"] = document["publication_date"]
        stats["regulated_mics"] = int(len(markets))
        _write_issuer_leis(issuer_leis)

        return SourceResult(
            name=NAME,
            funds=funds,
            listings=listings,
            fetched_at=timestamp(),
            stats=stats,
        )
    except Exception as exc:  # noqa: BLE001 -- fail-soft is the contract
        return failed(NAME, f"{type(exc).__name__}: {exc}")


def _capture_issuer_leis(
    records: Iterator[dict[str, str | None]], sink: dict[str, str]
) -> Iterator[dict[str, str | None]]:
    """Tee ISIN -> issuer LEI out of the stream as it passes.

    A second pass over 87 MB of XML to collect one field would double the
    parse time for no reason.
    """
    for record in records:
        isin, lei, cfi = record["isin"], record["issuer_lei"], record["cfi"] or ""
        if isin and lei and cfi.startswith(ETF_CFI_PREFIX):
            sink.setdefault(isin, lei)
        yield record


def _write_issuer_leis(leis: dict[str, str]) -> None:
    """Persist ISIN -> issuer LEI beside the cache, for the GLEIF stage.

    The schema has no LEI column, so this regulator-grade identifier would
    otherwise be parsed and discarded every run -- and GLEIF's own ISIN->LEI file
    is 9.1M rows to cover the 7.8k we already hold right here.
    """
    try:
        _ISSUER_LEI_PATH.parent.mkdir(parents=True, exist_ok=True)
        _ISSUER_LEI_PATH.write_text(
            json.dumps(leis, indent=0, sort_keys=True), encoding="utf-8"
        )
    except OSError as exc:  # a by-product, never a reason to fail the source
        log.warning("could not write %s: %s", _ISSUER_LEI_PATH, exc)
