"""The merge: eight independent adapters become one universe, once and for all.

Every other stage of this pipeline can be re-run. This one cannot be undone,
because it is where *identity* is decided -- which rows are the same fund, which
venue two adapters are both describing, and which of two disagreeing values gets
published. A price fetched wrong is refetched tomorrow; two funds fused into one
row corrupt every statistic derived from them and leave no trace of the fusion.

So the merge is built around four rules, in descending order of how expensive
getting them wrong is.

**1. A key is never invented.** The fund key is the ISIN where one exists and the
`US:{MIC}:{ticker}` surrogate where it does not (~3,377 US rows). A 12-character
ISIN cannot contain a colon, so the two shapes cannot collide, and
`sources.us.is_surrogate()` is the supported test. Every non-surrogate key is put
through `isin.is_valid` -- the ISO 6166 check digit -- before it enters the
merged table, and the rejects are counted and reported rather than dropped in
silence: a source that suddenly rejects 30% of its rows has changed shape, and
that must be visible in the build log.

**2. One venue is one MIC.** The adapters' exchange-code tables are hand-built
and partial, and they disagree: OpenFIGI calls Borsa Italiana's ETF market
`XMIL`, Euronext and FIRDS call it `ETFP`; FIRDS reports Xetra as its segment
MICs `XETA`/`XETU` while everyone else says `XETR`. Left alone, one venue becomes
two and the same listing is stored twice. `MicRegistry` reconciles them through
the ISO 10383 register the FIRDS adapter already caches -- see `MIC_ALIASES` for
the policy and why a register cannot decide it alone.

**3. Trust decides, and it decides the same way every time.** Sources are folded
highest-TRUST-first, so the first non-null value of a field wins and a lower-trust
source can never overwrite a higher-trust one. Ties break on the source name,
never on execution order: adapters run in dependency order, one of them may fail,
and the published value must not depend on either. `MergeResult.provenance`
records the winner of every populated field, so "where did this TER come from" is
one query rather than an archaeology exercise.

**4. A dead source degrades the result; it never aborts it.** Half these
endpoints are undocumented and rotate their URLs. `run()` treats `ok=False` as a
normal outcome, reports it, and merges what is left -- and wraps every `fetch`
call, because an adapter that raises despite the contract must still not take the
build down.

Order of execution
------------------
Three adapters cannot run first: OpenFIGI maps the ISINs the register supplies,
`nasdaqtrader` reads the ticker->ISIN artefact OpenFIGI writes, GLEIF resolves the
issuer LEIs FIRDS tees out, and Yahoo needs a listing table to attach its quotes
to. `DEPENDS_ON` states that, `plan()` topologically sorts it, and `run()`
performs one intermediate merge in the middle so the enrichment wave has a
universe to enrich. Running OpenFIGI's US pass *before* `nasdaqtrader` is what
lets a single build attach real ISINs to US rows; without it the resolution
always lags one build behind.

The Yahoo AUM bound
-------------------
Yahoo's `netAssets` is sometimes whole-fund assets across every share class
rather than the ETF's: VTI comes back around EUR 2.0e12, which is not a plausible
share class. TRUST 40 already stops it displacing a venue-published figure, but
where it is the only source there is nothing to displace it, so a value above
`MAX_PLAUSIBLE_AUM_EUR` from an aggregator is nulled and counted. The bound is
deliberately loose: it is there to catch an order-of-magnitude error, not to
second-guess a large fund.
"""

from __future__ import annotations

import importlib
import inspect
import io
import json
import logging
import time
from dataclasses import dataclass, field
from hashlib import blake2b
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
import pyarrow as pa

from . import isin as isin_module
from . import schema
from .config import BENCHMARK_ISIN, RAW
from .sources import REGISTRY, SourceResult
from .sources._http import primary_flags, to_frame

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# The registry
#
# Adapters are discovered by name and imported here, at import time, exactly as
# `sources/__init__.py` specifies. Import failures are caught: a module that
# cannot even be imported (a missing optional dependency, a syntax error in a
# module nobody has touched today) must degrade the universe like any other dead
# source, not stop the build before it starts.
# --------------------------------------------------------------------------- #

ADAPTER_MODULES: tuple[str, ...] = (
    "firds",
    "euronext",
    "xetra",
    "lse",
    "six",
    "us",
    "openfigi",
    "gleif",
    "yahoo_meta",
)

# The pseudo-dependency "the universe has been merged once". An adapter that
# declares it runs in the second wave, after `run()` has folded everything else
# together, because it needs the merged table rather than any single source.
MERGE_NODE = "merge"

# Keyed by the adapter's NAME (not its module name), because that is what the
# rest of the pipeline knows it as.
DEPENDS_ON: dict[str, tuple[str, ...]] = {
    # Maps ISINs the register supplies; it invents none of its own.
    "openfigi": ("firds",),
    # Reads data/raw/openfigi/us_ticker_isin.json, which the pass above writes.
    # Running it in the other order costs a build's delay on every US ISIN.
    "nasdaqtrader": ("openfigi",),
    # Resolves the ISIN -> issuer-LEI map FIRDS writes beside its cache.
    "gleif": ("firds",),
    # Has no ISIN anywhere in its responses, so it can only attach quotes to
    # funds the universe already knows by symbol.
    "yahoo": (MERGE_NODE,),
}

# OpenFIGI's composite-US pass: one exchange filter cuts the response from ~175
# records to one, and it is the only pass that recovers US ISINs. The full
# every-venue pass is 52 minutes keyless for the whole universe and buys
# cross-checks rather than identity, so it is opt-in via `openfigi_exch_code`.
OPENFIGI_US_PASS = "US"


@dataclass(frozen=True)
class Adapter:
    """One registered source module, with the two constants the merge needs."""

    name: str
    trust: int
    module: ModuleType

    @property
    def fetch(self) -> Any:
        return self.module.fetch


def register_adapters(modules: Sequence[str] = ADAPTER_MODULES) -> dict[str, Adapter]:
    """Import each adapter and populate `sources.REGISTRY`.

    Returns the adapters keyed by NAME. A module that fails to import is logged
    and skipped -- the same fail-soft posture the fetch contract demands, applied
    one level earlier.
    """
    adapters: dict[str, Adapter] = {}
    for module_name in modules:
        try:
            module = importlib.import_module(f".sources.{module_name}", __package__)
            adapter = Adapter(module.NAME, int(module.TRUST), module)
        except Exception as exc:  # noqa: BLE001 -- a broken module is a dead source
            log.warning("adapter %s could not be imported: %s", module_name, exc)
            continue
        adapters[adapter.name] = adapter
        REGISTRY[adapter.name] = module
        # Two names reach the same adapter: `sources/us.py` publishes itself as
        # "nasdaqtrader" and `yahoo_meta.py` as "yahoo". A caller naming the file
        # is not making a mistake, and silently running nothing would be a nasty
        # way to find that out.
        MODULE_ALIASES[module_name] = adapter.name
    return adapters


# Module name -> adapter NAME, for callers who name the file.
MODULE_ALIASES: dict[str, str] = {}

ADAPTERS: dict[str, Adapter] = register_adapters()


def select_adapters(names: Iterable[str]) -> list[Adapter]:
    """Resolve adapter names -- or module names -- to registered adapters.

    Raises on an unknown name rather than quietly running a smaller universe:
    `--sources firds,euronxt` should fail immediately, not produce a plausible
    dataset missing a source nobody notices for a week.
    """
    chosen: list[Adapter] = []
    unknown: list[str] = []
    for name in names:
        key = MODULE_ALIASES.get(name, name)
        adapter = ADAPTERS.get(key)
        if adapter is None:
            unknown.append(name)
        elif adapter not in chosen:
            chosen.append(adapter)
    if unknown:
        raise KeyError(
            f"no such adapter(s): {unknown}; known: {sorted(set(ADAPTERS) | set(MODULE_ALIASES))}"
        )
    return chosen


def trust_levels() -> dict[str, int]:
    """TRUST per source name, as declared by the adapter modules themselves."""
    return {name: adapter.trust for name, adapter in ADAPTERS.items()}


# --------------------------------------------------------------------------- #
# MICs
#
# The one canonical venue vocabulary, built from the ISO 10383 register that
# `sources/firds.py` already downloads and caches. Nothing here re-derives the
# register: it reads more of its columns.
#
# What the register cannot decide, and why the alias list is explicit
# -------------------------------------------------------------------
# The obvious rule -- "collapse every MIC onto its OPERATING MIC" -- is wrong,
# and the register proves it: NYSE Arca (ARCX) and NYSE American (XASE) both
# report XNYS as their operating MIC and are genuinely different listing venues,
# as are Euronext's multi-currency books (XAMC under XAMS, XPMC under XPAR),
# which carry the USD quote line of a fund also listed in EUR. Collapsing those
# would fuse two real listings and silently drop one currency.
#
# So aliasing is opt-in, one line per venue the adapters actually disagree about,
# and each line is validated against the register: both codes must share an
# OPERATING MIC, or the map is wrong and `MicRegistry.alias_problems` says so.
# The direction is always segment -> operating MIC, i.e. towards "the venue",
# because that is the level the rest of the pipeline is keyed on -- Yahoo's
# exchange table (`yahoo_meta.YAHOO_EXCHANGE_MIC`) and the price-symbol suffixes
# below both name XMIL and XETR, never ETFP or XETA.
# --------------------------------------------------------------------------- #

MIC_ALIASES: dict[str, str] = {
    # Borsa Italiana. FIRDS and Euronext report the segment a fund trades on;
    # OpenFIGI and Yahoo report the exchange. Same venue, four names.
    "ETFP": "XMIL",  # ETFplus, the ETF segment
    "MTAA": "XMIL",  # Euronext Milan, the equity segment
    "MOTX": "XMIL",  # electronic bond market
    "SEDX": "XMIL",  # securitised derivatives
    # Xetra. FIRDS reports the on-book and off-book regulated segments; the Xetra
    # adapter, OpenFIGI and Yahoo all say XETR.
    "XETA": "XETR",
    "XETU": "XETR",
    # Frankfurt's floor segments, same story.
    "FRAA": "XFRA",
    "FRAU": "XFRA",
    # Euronext Dublin: FIRDS and Euronext say XMSM (the main securities market),
    # OpenFIGI says XDUB.
    "XMSM": "XDUB",
}

# Order used to pick each fund's primary listing once every adapter's own
# preference has been reconciled onto canonical codes. "Where would a European
# retail holder look this fund's price up": Paris first for a French audience,
# then the rest of Euronext, the large continental venues, London, the Nordics,
# then the US tape. Multi-currency books rank last -- they are secondary quote
# lines of a fund already listed on the venue above them.
UNIVERSE_MIC_PREFERENCE: tuple[str, ...] = (
    "XPAR",
    "XAMS",
    "XBRU",
    "XLIS",
    "XDUB",
    "XETR",
    "XMIL",
    "XSWX",
    "XLON",
    "XMAD",
    "XWBO",
    "XSTO",
    "XCSE",
    "XOSL",
    "XFRA",
    "XPMC",
    "XAMC",
    "ARCX",
    "XNAS",
    "XNYS",
    "BATS",
    "XASE",
    "IEXG",
)

# Yahoo's venue suffixes. Hand-built because Yahoo publishes no mapping, and
# deliberately partial: a MIC that is not here yields no candidate symbol rather
# than a guessed one, and `sources/prices` would only 404 on it anyway. US
# venues quote bare symbols, which is why they map to the empty string -- a
# distinct statement from "absent".
YAHOO_SUFFIX: dict[str, str] = {
    "XPAR": ".PA",
    "XPMC": ".PA",
    "XAMS": ".AS",
    "XAMC": ".AS",
    "XBRU": ".BR",
    "XLIS": ".LS",
    "XDUB": ".IR",
    "XETR": ".DE",
    "XFRA": ".F",
    "XMIL": ".MI",
    "XSWX": ".SW",
    "XLON": ".L",
    "XMAD": ".MC",
    "XWBO": ".VI",
    "XSTO": ".ST",
    "XCSE": ".CO",
    "XOSL": ".OL",
    "XHKG": ".HK",
    "XTSE": ".TO",
    "XASX": ".AX",
    "XNAS": "",
    "XNYS": "",
    "ARCX": "",
    "XASE": "",
    "BATS": "",
    "IEXG": "",
}

_REGISTER_COLUMNS = {
    "MIC": "mic",
    "OPERATING MIC": "operating_mic",
    "MARKET NAME-INSTITUTION DESCRIPTION": "venue_name",
    "MARKET CATEGORY CODE": "category",
    "ISO COUNTRY CODE (ISO 3166)": "country",
    "STATUS": "status",
}


class MicRegistry:
    """ISO 10383, reduced to what the merge needs, plus the alias policy.

    Deliberately tolerant of an unknown MIC: the register is the authority on
    what a code means, not on what an adapter is allowed to emit, and dropping a
    listing because ISO has not catalogued its venue would lose real rows. An
    unknown code passes through unchanged and is counted.
    """

    def __init__(self, frame: pd.DataFrame, aliases: Mapping[str, str] | None = None) -> None:
        self.frame = frame
        self.aliases = dict(MIC_ALIASES if aliases is None else aliases)
        self._operating = dict(zip(frame["mic"], frame["operating_mic"]))
        self._name = dict(zip(frame["mic"], frame["venue_name"]))
        self._country = dict(zip(frame["mic"], frame["country"]))
        self._category = dict(zip(frame["mic"], frame["category"]))

    # -- construction ------------------------------------------------------- #

    @classmethod
    def from_csv(cls, raw: bytes | str, aliases: Mapping[str, str] | None = None) -> "MicRegistry":
        """Parse the register's CSV. Same file `firds.parse_register` reads."""
        buffer = io.BytesIO(raw.encode("utf-8") if isinstance(raw, str) else raw)
        register = pd.read_csv(
            buffer,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
            encoding_errors="replace",  # a few market names carry cp1252 dashes
        )
        missing = set(_REGISTER_COLUMNS) - set(register.columns)
        if missing:
            raise KeyError(f"ISO 10383 register is missing columns: {sorted(missing)}")

        frame = register[list(_REGISTER_COLUMNS)].rename(columns=_REGISTER_COLUMNS)
        for column in frame.columns:
            frame[column] = frame[column].str.strip()
        # Expired codes are kept: a fund can still hold a listing on a venue ISO
        # retired last month, and rewriting it to nothing would lose the row.
        return cls(frame.drop_duplicates("mic"), aliases)

    @classmethod
    def load(cls, refresh: bool = False) -> "MicRegistry":
        """Read the register through the FIRDS adapter's own cache.

        Imported lazily so that a broken FIRDS module degrades the MIC lookup
        rather than making this module unimportable.
        """
        from .sources import firds
        from .sources._http import get_cached

        return cls.from_csv(get_cached("mic", firds.MIC_REGISTER_URL, refresh=refresh))

    # -- lookups ------------------------------------------------------------ #

    def knows(self, mic: object) -> bool:
        return isinstance(mic, str) and mic in self._operating

    def operating(self, mic: str) -> str | None:
        return self._operating.get(mic)

    def name(self, mic: str) -> str | None:
        return self._name.get(mic) or None

    def country(self, mic: str) -> str | None:
        return self._country.get(mic) or None

    def category(self, mic: str) -> str | None:
        return self._category.get(mic) or None

    def canonical(self, mic: object) -> object:
        """The one code this pipeline stores for the venue `mic` denotes."""
        if not isinstance(mic, str):
            return mic
        code = mic.strip().upper()
        return self.aliases.get(code, code)

    def countries(self) -> dict[str, str]:
        """Canonical MIC -> ISO country, for the `is_primary` rule."""
        mapping: dict[str, str] = {}
        for mic, country in self._country.items():
            if country:
                mapping.setdefault(self.canonical(mic), country)
        return mapping

    # -- the guard ---------------------------------------------------------- #

    def alias_problems(self, surrogate_mics: Iterable[str] = ()) -> list[str]:
        """Ways the alias map is *wrong*, as human-readable lines. Build-fatal.

        Three checks, and the second is the important one. An alias whose source
        code appears inside a `US:{MIC}:{ticker}` surrogate would rewrite the
        *key* of a fund, orphaning every price bar already stored against it, so
        that is refused outright rather than handled.

        A code this register has never heard of is deliberately not reported
        here. That says the register we hold is partial or absent, not that the
        map is wrong, and aborting the build over it would trade one unreconciled
        venue for no database at all. `unknown_aliases` reports those instead.
        """
        problems: list[str] = []
        reserved = {str(mic) for mic in surrogate_mics}

        for source, target in sorted(self.aliases.items()):
            if source in reserved:
                problems.append(
                    f"{source} -> {target}: {source} appears in surrogate fund keys; "
                    "aliasing it would rewrite the key of every fund keyed on it"
                )
            if self.canonical(target) != target:
                problems.append(f"{source} -> {target}: {target} is itself aliased")
            if not self.knows(source):
                continue
            operating = self.operating(source)
            if operating != target:
                problems.append(
                    f"{source} -> {target}: the register says {source} belongs to "
                    f"{operating!r}, so the two are not the same venue"
                )
        return problems

    def unknown_aliases(self) -> list[str]:
        """Aliased codes this register does not mention -- a coverage gap, not an error."""
        return sorted(source for source in self.aliases if not self.knows(source))


# --------------------------------------------------------------------------- #
# Reports
# --------------------------------------------------------------------------- #

# Anything at or below this trust is an aggregator's opinion rather than a
# venue's or a regulator's filing. See the AUM bound in the module docstring.
AGGREGATOR_TRUST = 40

# One trillion euros, and the gap it sits in was measured rather than guessed.
# Over the 5,047 AUM values Yahoo's US screener supplied on 2026-08-11, exactly
# two are above it -- EUR 1.46e12 and 1.98e12, both Vanguard total-market figures
# that count every share class of the fund rather than the ETF line -- while the
# largest credible one is EUR 7.5e11. Loose on purpose: this catches a units or
# share-class error, not a fund that is merely very large.
MAX_PLAUSIBLE_AUM_EUR = 1.0e12


@dataclass
class SourceStatus:
    """What one adapter contributed, whether or not it worked."""

    name: str
    trust: int
    ok: bool
    error: str | None = None
    funds_in: int = 0
    listings_in: int = 0
    funds_kept: int = 0
    fields_won: int = 0
    rejected_isins: tuple[str, ...] = ()
    seconds: float = 0.0
    stats: dict = field(default_factory=dict)


@dataclass
class MergeReport:
    """Everything a reviewer needs to challenge the merged table."""

    sources: list[SourceStatus] = field(default_factory=list)
    funds: int = 0
    listings: int = 0
    surrogate_keys: int = 0
    rejected_isins: dict[str, tuple[str, ...]] = field(default_factory=dict)
    mic_rewrites: dict[str, int] = field(default_factory=dict)
    unknown_mics: dict[str, int] = field(default_factory=dict)
    duplicate_listings: int = 0
    orphan_listings: int = 0
    aum_nulled: int = 0
    aum_examples: tuple[tuple[str, float], ...] = ()
    contested_fields: dict[str, int] = field(default_factory=dict)

    @property
    def failed_sources(self) -> list[SourceStatus]:
        return [status for status in self.sources if not status.ok]

    @property
    def rejected_isin_count(self) -> int:
        return sum(len(codes) for codes in self.rejected_isins.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "funds": self.funds,
            "listings": self.listings,
            "surrogate_keys": self.surrogate_keys,
            "rejected_isins": {k: list(v) for k, v in self.rejected_isins.items()},
            "mic_rewrites": self.mic_rewrites,
            "unknown_mics": self.unknown_mics,
            "duplicate_listings": self.duplicate_listings,
            "orphan_listings": self.orphan_listings,
            "aum_nulled": self.aum_nulled,
            "contested_fields": self.contested_fields,
            "sources": [
                {
                    "name": s.name,
                    "trust": s.trust,
                    "ok": s.ok,
                    "error": s.error,
                    "funds_in": s.funds_in,
                    "listings_in": s.listings_in,
                    "funds_kept": s.funds_kept,
                    "fields_won": s.fields_won,
                    "rejected_isins": len(s.rejected_isins),
                    "seconds": round(s.seconds, 2),
                }
                for s in self.sources
            ],
        }


@dataclass
class MergeResult:
    """The merged universe plus the audit trail that explains it."""

    funds: pd.DataFrame
    listings: pd.DataFrame
    provenance: pd.DataFrame
    report: MergeReport

    def source_of(self, key: str, field_name: str) -> str | None:
        """Which source supplied `funds.{field_name}` for this fund key."""
        hit = self.provenance[
            (self.provenance["isin"] == key) & (self.provenance["field"] == field_name)
        ]
        return None if hit.empty else str(hit.iloc[0]["source"])


# --------------------------------------------------------------------------- #
# Keys
# --------------------------------------------------------------------------- #


def is_surrogate(key: object) -> bool:
    """True when a key stands in for an ISIN that could not be established.

    Delegates to the US adapter, which owns the shape, and falls back to the
    documented prefix if that module could not be imported -- the merge must
    still be able to tell the two key shapes apart.
    """
    module = REGISTRY.get("nasdaqtrader")
    if module is not None:
        return bool(module.is_surrogate(key))
    return isinstance(key, str) and key.startswith("US:")


def normalise_key(raw: object) -> str | None:
    """Return the canonical fund key, or None when the value cannot be one.

    A surrogate is taken verbatim -- tickers carry dots and hyphens that ISIN
    normalisation would strip -- and everything else must survive the ISO 6166
    check digit. There is no third case: a key that is neither is refused, never
    repaired.
    """
    if is_surrogate(raw):
        return str(raw)
    code = isin_module.normalise(raw)
    if code is None or not isin_module.is_valid(code):
        return None
    return code


# --------------------------------------------------------------------------- #
# The merge itself
# --------------------------------------------------------------------------- #

# Filled by the merge rather than taken from a source: `isin` is the key,
# `data_sources` is the union of contributors, `last_updated` the most recent
# contribution, and `is_primary` is recomputed for the whole universe because
# each adapter could only flag a primary among the venues it happened to see.
_FUND_DERIVED = ("isin", "data_sources", "last_updated")
_LISTING_DERIVED = ("isin", "exchange_mic", "is_primary")

_FUND_FIELDS = tuple(
    f.name for f in schema.FUNDS if f.name not in _FUND_DERIVED
)
_LISTING_FIELDS = tuple(
    f.name for f in schema.LISTINGS if f.name not in _LISTING_DERIVED
)


def _ordered(results: Sequence[SourceResult], trust: Mapping[str, int]) -> list[SourceResult]:
    """Sources highest-trust first, ties broken by name.

    The tie-break is the whole point: adapters run in dependency order, that
    order changes when one of them fails or when a new source is added, and the
    published value must not move because of it.
    """
    return sorted(results, key=lambda r: (-int(trust.get(r.name, 0)), r.name))


def _stack(
    frames: Sequence[tuple[SourceResult, pd.DataFrame]], trust: Mapping[str, int]
) -> pd.DataFrame:
    """One frame carrying every source's rows, tagged with source and trust."""
    parts: list[pd.DataFrame] = []
    for result, frame in frames:
        if frame is None or frame.empty:
            continue
        tagged = frame.copy()
        tagged["_source"] = result.name
        tagged["_trust"] = int(trust.get(result.name, 0))
        parts.append(tagged)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _first_by_trust(
    stacked: pd.DataFrame, keys: Sequence[str], fields: Sequence[str]
) -> tuple[dict[str, pd.Series], pd.DataFrame]:
    """Resolve each field to its highest-trust non-null value, with provenance.

    `stacked` must already be ordered highest-trust-first, so "the first non-null
    row of this group" *is* "the most trustworthy source that answered". That
    equivalence is what makes the rule "a lower-trust source never overwrites a
    non-null higher-trust value" true by construction rather than by a check.
    """
    values: dict[str, pd.Series] = {}
    provenance: list[pd.DataFrame] = []

    for name in fields:
        if name not in stacked.columns:
            continue
        present = stacked.loc[stacked[name].notna(), [*keys, name, "_source", "_trust"]]
        if present.empty:
            continue

        contenders = present.groupby(list(keys), sort=False)[name].transform("size")
        winners = present.drop_duplicates(subset=list(keys), keep="first")
        values[name] = pd.Series(
            winners[name].to_numpy(),
            index=pd.MultiIndex.from_frame(winners[list(keys)]),
        )

        trail = winners[[*keys, "_source", "_trust"]].copy()
        trail["field"] = name
        trail["contenders"] = contenders.loc[winners.index].to_numpy()
        provenance.append(trail)

    if provenance:
        trail = pd.concat(provenance, ignore_index=True)
        trail = trail.rename(columns={"_source": "source", "_trust": "trust"})
    else:
        trail = pd.DataFrame(
            columns=[*keys, "source", "trust", "field", "contenders"]
        )
    return values, trail


def _union_sources(stacked: pd.DataFrame, keys: Sequence[str]) -> pd.Series:
    """Every source that supplied a row for a key, sorted for a stable output."""
    grouped = stacked.groupby(list(keys), sort=False)["_source"]
    return grouped.apply(lambda names: sorted(set(names)))


def _canonicalise_mics(
    listings: pd.DataFrame, registry: MicRegistry, report: MergeReport
) -> pd.DataFrame:
    """Rewrite every MIC onto the canonical code for its venue.

    `exchange_name` is dropped from a rewritten row on purpose: it described the
    segment ("Borsa Italiana ETFplus") and the row now describes the venue, so
    the name is left to be filled by a source that used the canonical code, or by
    the register.
    """
    frame = listings.copy()
    raw = frame["exchange_mic"].astype("string")
    canonical = raw.map(lambda mic: registry.canonical(mic) if pd.notna(mic) else mic)

    rewritten = raw.notna() & (canonical != raw)
    for source, target in zip(raw[rewritten], canonical[rewritten]):
        report.mic_rewrites[f"{source}->{target}"] = (
            report.mic_rewrites.get(f"{source}->{target}", 0) + 1
        )

    unknown = canonical.notna() & ~canonical.map(registry.knows)
    for mic in canonical[unknown]:
        report.unknown_mics[str(mic)] = report.unknown_mics.get(str(mic), 0) + 1

    frame["exchange_mic"] = canonical
    if "exchange_name" in frame.columns:
        frame.loc[rewritten.to_numpy(), "exchange_name"] = None
    return frame


def _apply_aum_bound(
    funds: pd.DataFrame, provenance: pd.DataFrame, report: MergeReport
) -> pd.DataFrame:
    """Null an aggregator-sourced AUM that cannot be a share class's assets.

    Applied only where the winning source is an aggregator: a venue- or
    regulator-published figure is accountable and stays, and by the time this
    runs the trust rule has already ensured the winner is the best source there
    was. See the module docstring for the bound.
    """
    if "aum_eur" not in funds.columns or funds.empty:
        return funds

    winners = provenance[provenance["field"] == "aum_eur"]
    weak = set(winners.loc[winners["trust"] <= AGGREGATOR_TRUST, "isin"])
    if not weak:
        return funds

    frame = funds.copy()
    values = pd.to_numeric(frame["aum_eur"], errors="coerce")
    suspect = (
        frame["isin"].isin(weak)
        & values.notna()
        & ((values > MAX_PLAUSIBLE_AUM_EUR) | (values <= 0))
    )
    if not suspect.any():
        return frame

    report.aum_nulled = int(suspect.sum())
    report.aum_examples = tuple(
        (str(key), float(value))
        for key, value in zip(frame.loc[suspect, "isin"], values[suspect])
    )[:5]
    log.warning(
        "nulled %d aggregator AUM value(s) outside the plausible range, e.g. %s",
        report.aum_nulled,
        report.aum_examples[:3],
    )
    frame.loc[suspect, "aum_eur"] = None
    return frame


def merge(
    results: Sequence[SourceResult],
    *,
    registry: MicRegistry | None = None,
    trust: Mapping[str, int] | None = None,
) -> MergeResult:
    """Fold every source into one funds table and one listings table.

    Pure: no network, no disk beyond the MIC register the caller may supply.
    `trust` defaults to the TRUST constants of the registered adapters; a source
    the registry does not know is trusted at 0 and reported, rather than being
    silently ranked above or below anything.

    Failed sources are folded in like any other -- they carry empty frames -- so
    the result degrades by exactly what they would have contributed.
    """
    trust = dict(trust_levels() if trust is None else trust)
    unknown = sorted({r.name for r in results} - set(trust))
    if unknown:
        log.warning("source(s) with no declared TRUST, ranked lowest: %s", unknown)

    report = MergeReport()
    ordered = _ordered(results, trust)
    statuses = {
        r.name: SourceStatus(
            name=r.name,
            trust=int(trust.get(r.name, 0)),
            ok=r.ok,
            error=r.error,
            funds_in=int(len(r.funds)) if r.funds is not None else 0,
            listings_in=int(len(r.listings)) if r.listings is not None else 0,
            stats=dict(r.stats),
        )
        for r in ordered
    }

    funds_stack, listings_stack = _validated_stacks(ordered, trust, statuses, report)

    funds, provenance = _merge_funds(funds_stack, statuses, report)
    listings = _merge_listings(
        listings_stack, funds, registry or MicRegistry.from_csv(_EMPTY_REGISTER), report
    )
    funds = _apply_aum_bound(funds, provenance, report)

    for name, status in statuses.items():
        status.fields_won = int((provenance["source"] == name).sum()) if len(provenance) else 0

    report.sources = [statuses[r.name] for r in ordered]
    report.funds = int(len(funds))
    report.listings = int(len(listings))
    report.surrogate_keys = int(funds["isin"].map(is_surrogate).sum()) if len(funds) else 0
    return MergeResult(funds=funds, listings=listings, provenance=provenance, report=report)


# A register with the right columns and no rows, so `merge` works without one.
# Every MIC then passes through unchanged and is reported as unknown, which is
# the honest degradation: we cannot reconcile venues we cannot look up.
_EMPTY_REGISTER = ",".join(_REGISTER_COLUMNS) + "\n"


def _validated_stacks(
    ordered: Sequence[SourceResult],
    trust: Mapping[str, int],
    statuses: dict[str, SourceStatus],
    report: MergeReport,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Stack both tables, refusing every row whose key cannot be a fund key."""
    fund_frames: list[tuple[SourceResult, pd.DataFrame]] = []
    listing_frames: list[tuple[SourceResult, pd.DataFrame]] = []

    for result in ordered:
        rejected: set[str] = set()
        for frames, incoming in (
            (fund_frames, result.funds),
            (listing_frames, result.listings),
        ):
            if incoming is None or incoming.empty:
                continue
            frame = incoming.copy()
            keys = frame["isin"].map(normalise_key)
            bad = keys.isna()
            if bad.any():
                rejected.update(str(value) for value in frame.loc[bad, "isin"])
            frame["isin"] = keys
            frames.append((result, frame[~bad]))

        if rejected:
            codes = tuple(sorted(rejected))
            statuses[result.name].rejected_isins = codes
            report.rejected_isins[result.name] = codes
            log.warning(
                "%s: %d identifier(s) refused (not an ISIN and not a surrogate): %s",
                result.name,
                len(codes),
                codes[:5],
            )

    return _stack(fund_frames, trust), _stack(listing_frames, trust)


def _merge_funds(
    stacked: pd.DataFrame, statuses: dict[str, SourceStatus], report: MergeReport
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if stacked.empty:
        empty = to_frame([], "funds")
        return empty, pd.DataFrame(
            columns=["isin", "field", "source", "trust", "contenders"]
        )

    values, provenance = _first_by_trust(stacked, ["isin"], _FUND_FIELDS)
    keys = pd.Index(sorted(stacked["isin"].unique()), name="isin")

    columns: dict[str, Any] = {"isin": keys.to_numpy()}
    for name, series in values.items():
        series.index = series.index.get_level_values("isin")
        columns[name] = series.reindex(keys).to_numpy()

    contributors = _union_sources(stacked, ["isin"]).reindex(keys)
    columns["data_sources"] = contributors.to_numpy()
    if "last_updated" in stacked.columns:
        # Nulls are filtered before the max rather than skipped by it: the column
        # holds `datetime.date` objects, and a NaN in the same object column
        # makes the comparison itself a TypeError rather than a skipped value.
        dated = stacked[stacked["last_updated"].notna()]
        stamps = dated.groupby("isin")["last_updated"].max() if len(dated) else pd.Series(dtype=object)
        columns["last_updated"] = stamps.reindex(keys).to_numpy()

    for name, status in statuses.items():
        status.funds_kept = int((stacked["_source"] == name).sum())

    for name, count in provenance.loc[provenance["contenders"] > 1, "field"].value_counts().items():
        report.contested_fields[str(name)] = int(count)

    return to_frame(pd.DataFrame(columns), "funds"), provenance.reset_index(drop=True)


def _merge_listings(
    stacked: pd.DataFrame,
    funds: pd.DataFrame,
    registry: MicRegistry,
    report: MergeReport,
) -> pd.DataFrame:
    if stacked.empty:
        return to_frame([], "listings")

    stacked = _canonicalise_mics(stacked, registry, report)
    stacked = stacked[stacked["exchange_mic"].notna()]

    keys = ["isin", "exchange_mic"]
    before = int(len(stacked))
    values, _ = _first_by_trust(stacked, keys, _LISTING_FIELDS)

    pairs = (
        stacked[keys]
        .drop_duplicates()
        .sort_values(keys, kind="mergesort")
        .reset_index(drop=True)
    )
    report.duplicate_listings = before - int(len(pairs))

    # A listing whose fund is not in the merged table cannot be joined to
    # anything, and publishing it would advertise a venue for a fund we do not
    # have. Counted, not silently dropped.
    known = set(funds["isin"]) if len(funds) else set()
    orphan = ~pairs["isin"].isin(known)
    report.orphan_listings = int(orphan.sum())
    pairs = pairs[~orphan].reset_index(drop=True)
    if pairs.empty:
        return to_frame([], "listings")

    index = pd.MultiIndex.from_frame(pairs)
    columns: dict[str, Any] = {
        "isin": pairs["isin"].to_numpy(),
        "exchange_mic": pairs["exchange_mic"].to_numpy(),
    }
    for name, series in values.items():
        columns[name] = series.reindex(index).to_numpy()

    # The venue's registered name, only where no source supplied one.
    supplied = columns.get("exchange_name")
    names = pd.Series(
        supplied if supplied is not None else [None] * len(pairs),
        index=pairs.index,
        dtype="object",
    )
    fallback = pairs["exchange_mic"].map(registry.name)
    columns["exchange_name"] = names.where(names.notna(), fallback).to_numpy()

    # One rule for the whole database, applied once every adapter's MICs have
    # been reconciled: an adapter could only ever pick a primary among the venues
    # it happened to see, so its flag is recomputed rather than merged.
    columns["is_primary"] = primary_flags(
        list(pairs["isin"]),
        list(pairs["exchange_mic"]),
        mic_country=registry.countries(),
        preference=UNIVERSE_MIC_PREFERENCE,
    )
    return to_frame(pd.DataFrame(columns), "listings")


# --------------------------------------------------------------------------- #
# Running the adapters
# --------------------------------------------------------------------------- #


@dataclass
class Context:
    """What an enrichment adapter is allowed to see of the run so far."""

    isins: tuple[str, ...] = ()
    listings: pd.DataFrame | None = None

    def issuer_leis(self) -> dict[str, str]:
        """ISIN -> issuer LEI, as FIRDS tees it out beside its own cache.

        Absent until FIRDS has run at least once; an empty map sends GLEIF down
        its slower ISIN->LEI path rather than failing it.
        """
        path = RAW / "firds" / "issuer_lei.json"
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            log.warning("unreadable issuer LEI artefact at %s (%s)", path, exc)
            return {}
        return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}


def plan(adapters: Sequence[Adapter]) -> list[list[Adapter]]:
    """Two waves of adapters, with one merge between them.

    Within a wave the order is a topological sort of `DEPENDS_ON`, broken by
    (-trust, name) so that adding or losing a source cannot reshuffle the rest.
    An unsatisfiable dependency does not deadlock the run: the adapter is
    appended at the end of its wave and left to fail on its own terms, which is
    the fail-soft outcome rather than a build that never starts.
    """
    waves: list[list[Adapter]] = []
    first = [a for a in adapters if MERGE_NODE not in DEPENDS_ON.get(a.name, ())]
    second = [a for a in adapters if MERGE_NODE in DEPENDS_ON.get(a.name, ())]

    for wave in (first, second):
        remaining = sorted(wave, key=lambda a: (-a.trust, a.name))
        done: set[str] = {a.name for w in waves for a in w}
        ordered: list[Adapter] = []
        while remaining:
            ready = [
                a
                for a in remaining
                if not (set(DEPENDS_ON.get(a.name, ())) - done - {MERGE_NODE})
            ]
            if not ready:  # a cycle or a dependency on a source not being run
                ready = remaining[:1]
            for adapter in ready:
                ordered.append(adapter)
                done.add(adapter.name)
                remaining.remove(adapter)
        waves.append(ordered)
    return waves


def _kwargs_for(adapter: Adapter, context: Context, refresh: bool, exch_code: str | None) -> dict:
    """Bind the run context to whatever the adapter's `fetch` actually accepts.

    Read off the signature rather than from a per-adapter table so that a new
    enrichment source is served by declaring a parameter, not by editing the
    orchestrator.
    """
    try:
        parameters = inspect.signature(adapter.fetch).parameters
    except (TypeError, ValueError):
        return {}

    kwargs: dict[str, Any] = {}
    if "refresh" in parameters:
        kwargs["refresh"] = refresh
    if "listings" in parameters:
        kwargs["listings"] = context.listings
    if "isin_leis_map" in parameters:
        kwargs["isin_leis_map"] = context.issuer_leis()
    if "exch_code" in parameters:
        kwargs["exch_code"] = exch_code
    if "isins" in parameters:
        wanted = [key for key in context.isins if not is_surrogate(key)]
        # The US pass exists to recover US ISINs; sending it the European
        # universe as well would spend a budget of 250 ISIN/min on rows it
        # cannot answer.
        if exch_code == OPENFIGI_US_PASS and "exch_code" in parameters:
            wanted = [key for key in wanted if key.startswith("US")]
        kwargs["isins"] = wanted
    return kwargs


def _run_one(adapter: Adapter, context: Context, refresh: bool, exch_code: str | None) -> SourceResult:
    """Call one adapter, converting even a contract violation into a dead source."""
    from .sources._http import failed

    started = time.perf_counter()
    try:
        result = adapter.fetch(**_kwargs_for(adapter, context, refresh, exch_code))
    except Exception as exc:  # noqa: BLE001 -- `fetch` promises not to; if it does, absorb it
        log.exception("adapter %s raised despite the fail-soft contract", adapter.name)
        result = failed(adapter.name, f"{type(exc).__name__}: {exc}")
    elapsed = time.perf_counter() - started
    result.stats = {**result.stats, "seconds": round(elapsed, 2)}
    log.info(
        "%s: %s, %d funds, %d listings in %.1fs",
        adapter.name,
        "ok" if result.ok else f"FAILED ({result.error})",
        len(result.funds),
        len(result.listings),
        elapsed,
    )
    return result


def run(
    *,
    refresh: bool = False,
    adapters: Sequence[Adapter] | None = None,
    registry: MicRegistry | None = None,
    openfigi_exch_code: str | None = OPENFIGI_US_PASS,
) -> MergeResult:
    """Fetch every registered adapter and merge the results.

    Two waves with one merge between them: the enrichment sources need a
    universe to enrich, and building it twice is cheaper than making them guess.
    Nothing raised by an adapter escapes, so the result is always a universe --
    possibly a smaller one, with the casualties named in `report.failed_sources`.
    """
    chosen = list(adapters if adapters is not None else ADAPTERS.values())
    if registry is None:
        try:
            registry = MicRegistry.load(refresh=refresh)
        except Exception as exc:  # noqa: BLE001 -- degrade rather than abort
            # The alias map still reconciles the venues it names -- it is this
            # pipeline's own knowledge, not the register's. What is lost is the
            # validation of that map, the venue names, and the country rule that
            # picks a fund's primary listing.
            log.warning(
                "ISO 10383 register unavailable (%s); venues are still reconciled "
                "through the alias map, but unvalidated and unnamed",
                exc,
            )
            registry = MicRegistry.from_csv(_EMPTY_REGISTER)

    # The one thing in this module that is allowed to stop a build. A map that
    # contradicts the register would fuse two venues or rewrite a fund key, and
    # both are unrecoverable once written -- unlike a dead source, which merely
    # leaves a column empty.
    problems = registry.alias_problems(_surrogate_mics())
    for problem in problems:
        log.error("MIC alias: %s", problem)
    if problems:
        raise ValueError(
            "the canonical MIC map disagrees with the ISO 10383 register: "
            + "; ".join(problems)
        )
    unmentioned = registry.unknown_aliases()
    if unmentioned:
        log.warning(
            "the MIC register in hand does not mention %s, so those venues are "
            "reconciled on the map's word alone",
            unmentioned,
        )

    waves = plan(chosen)
    context = Context()
    results: list[SourceResult] = []

    for index, wave in enumerate(waves):
        if index and results:
            # The intermediate merge exists only to give the enrichment wave a
            # keyed listing table; it is thrown away and recomputed below.
            interim = merge(results, registry=registry)
            context = Context(
                isins=tuple(interim.funds["isin"]), listings=interim.listings
            )
        for adapter in wave:
            results.append(_run_one(adapter, context, refresh, openfigi_exch_code))
            if index == 0:
                context = Context(
                    isins=tuple(
                        dict.fromkeys(
                            list(context.isins)
                            + [str(key) for key in results[-1].funds["isin"]]
                        )
                    ),
                    listings=context.listings,
                )

    return merge(results, registry=registry)


def _surrogate_mics() -> set[str]:
    """MICs that appear inside `US:{MIC}:{ticker}` keys, which may not be aliased."""
    module = REGISTRY.get("nasdaqtrader")
    if module is None:
        return set()
    return {mic for mic, _name in module.EXCHANGE_MIC.values()}


# --------------------------------------------------------------------------- #
# Downstream helpers
# --------------------------------------------------------------------------- #


def yahoo_candidates(listings: pd.DataFrame) -> pd.DataFrame:
    """Ordered Yahoo symbols to try for each fund, best venue first.

    Yahoo publishes no ISIN and no venue mapping, so a symbol is assembled from
    the local ticker and the venue suffix. It is a *candidate list* rather than
    an answer because the recon measured every fund to be quoted on two to four
    Yahoo venues with near-identical history, and an ordered retry resolves 100%
    of them where any single venue leaves gaps (reference/PRICE_SOURCES.md §3.3).

    A resolved `yahoo_ticker` (the US adapter sets one) always outranks a
    derived symbol, and a venue with no suffix in `YAHOO_SUFFIX` produces no
    candidate at all rather than a guess.
    """
    columns = ["isin", "rank", "yahoo_ticker", "exchange_mic", "trading_currency"]
    if listings is None or listings.empty:
        return pd.DataFrame(columns=columns)

    frame = listings.copy()
    frame["_preference"] = frame["exchange_mic"].map(
        {mic: rank for rank, mic in enumerate(UNIVERSE_MIC_PREFERENCE)}
    )
    frame["_preference"] = frame["_preference"].fillna(len(UNIVERSE_MIC_PREFERENCE))
    frame["_primary"] = (~frame["is_primary"].fillna(False).astype(bool)).astype(int)
    frame = frame.sort_values(
        ["isin", "_primary", "_preference", "exchange_mic"], kind="mergesort"
    )

    rows: list[dict[str, Any]] = []
    for key, group in frame.groupby("isin", sort=True):
        seen: set[str] = set()
        for row in group.itertuples(index=False):
            symbol = _yahoo_symbol(row)
            if symbol is None or symbol in seen:
                continue
            seen.add(symbol)
            rows.append(
                {
                    "isin": key,
                    "rank": len(seen) - 1,
                    "yahoo_ticker": symbol,
                    "exchange_mic": row.exchange_mic,
                    "trading_currency": getattr(row, "trading_currency", None),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _yahoo_symbol(listing: Any) -> str | None:
    resolved = getattr(listing, "yahoo_ticker", None)
    if isinstance(resolved, str) and resolved.strip():
        return resolved.strip().upper()

    ticker = getattr(listing, "ticker", None)
    mic = getattr(listing, "exchange_mic", None)
    if not isinstance(ticker, str) or not ticker.strip() or not isinstance(mic, str):
        return None
    suffix = YAHOO_SUFFIX.get(mic)
    if suffix is None:
        return None
    # Yahoo writes share-class dots as hyphens (BRK.B -> BRK-B).
    return f"{ticker.strip().upper().replace('.', '-')}{suffix}"


def sample(
    funds: pd.DataFrame,
    listings: pd.DataFrame,
    limit: int | None,
    *,
    keep: Iterable[str] = (BENCHMARK_ISIN,),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cut the universe down to `limit` funds, deterministically and fairly.

    Hash order rather than the alphabetically first N: sorting by ISIN would
    hand back an all-Austrian sample quoted on one venue, which exercises almost
    none of the pipeline. `keep` is forced in regardless, because the benchmark
    fund has to be present for beta and correlation to be computed at all.
    """
    if limit is None or funds.empty or limit >= len(funds):
        return funds, listings

    forced = [key for key in keep if key in set(funds["isin"])]
    pool = funds.loc[~funds["isin"].isin(forced), "isin"]
    ranked = sorted(pool, key=lambda key: blake2b(str(key).encode(), digest_size=8).digest())
    chosen = set(forced) | set(ranked[: max(0, limit - len(forced))])

    kept_funds = funds[funds["isin"].isin(chosen)].reset_index(drop=True)
    kept_listings = (
        listings[listings["isin"].isin(chosen)].reset_index(drop=True)
        if listings is not None and not listings.empty
        else listings
    )
    return kept_funds, kept_listings


def write_provenance(result: MergeResult, root: Path | str | None = None) -> dict[str, Path]:
    """Persist the audit trail next to the raw cache, not in the published tables.

    `schema.py` is the contract for what the database contains, and a provenance
    table is not in it -- but a reviewer asking "where did this TER come from"
    needs an answer that outlives the process, so it is written where build
    artefacts live rather than being invented as an eighth published table.
    """
    directory = Path(root) if root is not None else RAW / "universe"
    directory.mkdir(parents=True, exist_ok=True)

    provenance_path = directory / "field_provenance.parquet"
    report_path = directory / "merge_report.json"
    result.provenance.to_parquet(provenance_path, index=False)
    report_path.write_text(
        json.dumps(result.report.to_dict(), indent=2, default=str), encoding="utf-8"
    )
    return {"provenance": provenance_path, "report": report_path}


def conform(frame: pd.DataFrame, table: str) -> pa.Table:
    """A merged frame as an Arrow table matching its declared schema."""
    return schema.conform(pa.Table.from_pandas(frame, preserve_index=False), table)


__all__ = [
    "ADAPTERS",
    "Adapter",
    "Context",
    "MicRegistry",
    "MergeReport",
    "MergeResult",
    "SourceStatus",
    "MIC_ALIASES",
    "UNIVERSE_MIC_PREFERENCE",
    "YAHOO_SUFFIX",
    "MAX_PLAUSIBLE_AUM_EUR",
    "conform",
    "is_surrogate",
    "merge",
    "normalise_key",
    "plan",
    "register_adapters",
    "run",
    "sample",
    "trust_levels",
    "write_provenance",
    "yahoo_candidates",
]


if __name__ == "__main__":  # pragma: no cover - a convenience, not the entry point
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    outcome = run()
    print(json.dumps(outcome.report.to_dict(), indent=2, default=str))
