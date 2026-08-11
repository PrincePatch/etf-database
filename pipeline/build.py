"""The orchestrator: `python -m pipeline.build` turns eight upstreams into seven tables.

Every stage in this file already exists and is tested somewhere else. What this
module owns is the *order*, the hand-offs between stages, and the one thing no
individual module can do: tell you at the end what the run actually produced and
what it lost on the way.

    universe -> eligibility -> resolve -> prices -> fx -> adjust -> stats -> write

Four decisions worth defending
------------------------------
**Stages are skippable and resumable, not because a build is long but because it
is fragile at the edges.** Yahoo rotates a token, ESMA republishes on Fridays,
the LSE workbook filename increments monthly. A developer fixing the statistics
engine must not have to re-run the universe merge to see the effect, so every
stage that is skipped loads its predecessor's output from `data/processed/`
instead: `--only fx,adjust,stats,write` recomputes every published statistic
from the stored bars in seconds, touching no upstream at all.

**`--limit` samples the merged universe, not the downloads.** The adapters read
whole files -- one 3.5 MB ESMA zip covers the entire EU -- so there is nothing to
limit there, and their caches make a re-run free. What costs real time and real
bandwidth is one HTTP request per fund for prices, which is exactly what the
sample cuts. The sample is hash-ordered rather than alphabetical, because the
first N ISINs are all Austrian and Belgian and would exercise one venue.

**FX runs before the total-return adjustment, and it converts the dividends
too.** The adjustment rebuilds `adj_close` by subtracting each distribution from
the price it was paid out of, so the two must be in the same currency: converting
the bars and leaving the dividends in the venue's currency would overstate the
adjustment by the exchange rate. Converting both here also happens to be the more
faithful model for this database's reader -- a French holder receives the
distribution in euros at the rate of the day it is paid, and reinvests it at that
day's euro price.

**The price store stays raw.** `prices.py` deliberately writes `adj_close` as
null on disk: bars and events are append-only, while the adjustment is a derived
view that one new dividend restates back to inception. So the fx and adjust
stages build an in-memory panel for the statistics engine and never write it
back. `prices.export_single_file()` applies the adjustment on the way out.

Cost of a full run
------------------
Measured throughput from `reference/PRICE_SOURCES.md` §1.4: 40 tickers/s cold and
120 tickers/s incremental, conservatively. For ~15,000 funds that is ~6 minutes
and ~4.8 GB for a cold backfill, ~2 minutes and ~22 MB for the daily incremental
path -- which is why `refresh` is the default and `--full-prices` is opt-in. The
statistics engine adds ~47 s over the full 49.1M-bar panel at ~3.4 GB peak. Run
`--limit` first; the summary prints the per-fund numbers this run measured so the
extrapolation is yours rather than this docstring's.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import adjust, config, eligibility, fx, prices, schema, stats, universe

log = logging.getLogger("pipeline.build")

# In dependency order. `write` is a stage of its own so that a developer can run
# everything and inspect the frames without touching data/processed/.
STAGES: tuple[str, ...] = (
    "universe",
    "eligibility",
    "resolve",
    "prices",
    "fx",
    "adjust",
    "stats",
    "write",
)

# Tables this module writes as single Parquet files. `prices` is excluded on
# purpose: it is bucketed under data/processed/prices/ by `prices.write_prices`
# because a 1.1 GB single file cannot be committed, and coalescing it is a
# publishing step rather than a build step.
WRITTEN_TABLES: tuple[str, ...] = (
    "funds",
    "listings",
    "broker_availability",
    "performance",
    "returns_yearly",
    "returns_monthly",
)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


@dataclass
class BuildConfig:
    """Everything the CLI can set, with `pipeline.config` supplying the defaults.

    `out` is overridable so a test can build into a temporary directory without
    touching a developer's real dataset; everything else defaults to the paths
    the rest of the pipeline already agrees on.
    """

    out: Path = config.PROCESSED
    prices_root: Path | None = None  # defaults to <out>/prices
    stages: tuple[str, ...] = STAGES
    limit: int | None = None
    refresh: bool = False
    full_prices: bool = False
    workers: int = prices.DEFAULT_WORKERS
    attempts: int = 2
    progress: bool = True
    sources: tuple[str, ...] | None = None  # None = every registered adapter
    openfigi_exch_code: str | None = universe.OPENFIGI_US_PASS
    reference: Path | None = None  # evidence root, defaults to reference/
    as_of: date | None = None

    def __post_init__(self) -> None:
        self.out = Path(self.out)
        self.prices_root = Path(self.prices_root) if self.prices_root else self.out / "prices"
        unknown = [stage for stage in self.stages if stage not in STAGES]
        if unknown:
            raise ValueError(f"unknown stage(s): {unknown}; known: {list(STAGES)}")

    def runs(self, stage: str) -> bool:
        return stage in self.stages

    def path(self, table: str) -> Path:
        return self.out / f"{table}.parquet"

    @property
    def provenance_root(self) -> Path:
        """Where the merge's audit trail goes.

        Not `out`: `data/processed/` is committed to git and holds exactly the
        tables `schema.py` declares, while the provenance table is a build
        artefact -- so by default it lands with the rest of them under the
        gitignored raw cache. A caller who redirected `out` (a test, a scratch
        build) gets it alongside their own output instead, so a run into a
        temporary directory stays inside it.
        """
        return config.RAW / "universe" if self.out == config.PROCESSED else self.out / "_merge"


@dataclass
class BuildState:
    """The frames in flight between stages. Populated as the run progresses."""

    funds: pd.DataFrame | None = None
    listings: pd.DataFrame | None = None
    symbols: pd.DataFrame | None = None
    actions: pd.DataFrame | None = None
    panel: dict[str, pd.DataFrame] = field(default_factory=dict)
    tables: dict[str, pa.Table] = field(default_factory=dict)
    merge: universe.MergeResult | None = None


@dataclass
class BuildReport:
    """What the run did, in the form the summary table prints."""

    stages_run: list[str] = field(default_factory=list)
    stage_seconds: dict[str, float] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)
    source_funds: dict[str, int] = field(default_factory=dict)
    source_listings: dict[str, int] = field(default_factory=dict)
    source_fields: dict[str, int] = field(default_factory=dict)
    failed_sources: list[tuple[str, str]] = field(default_factory=list)
    rejected_isins: dict[str, int] = field(default_factory=dict)
    rejected_examples: tuple[str, ...] = ()
    mic_rewrites: dict[str, int] = field(default_factory=dict)
    unknown_mics: dict[str, int] = field(default_factory=dict)
    duplicate_listings: int = 0
    orphan_listings: int = 0
    surrogate_keys: int = 0
    aum_nulled: int = 0
    untradable_funds: int = 0
    untradable_listings: int = 0
    funds_total: int = 0
    funds_with_symbol: int = 0
    listings_with_symbol: int = 0
    funds_with_prices: int = 0
    funds_without_prices: int = 0
    pea: dict[str, int] = field(default_factory=dict)
    price_fetch: dict[str, Any] = field(default_factory=dict)
    unsupported_currencies: dict[str, str] = field(default_factory=dict)
    limit: int | None = None

    @property
    def seconds(self) -> float:
        return sum(self.stage_seconds.values())

    def extrapolate(self, universe_size: int = 15_000) -> dict[str, float]:
        """Full-universe cost implied by this run's own measurements.

        Deliberately derived from what just happened rather than from the recon's
        headline figures: a developer's connection is not the one the recon was
        measured on, and a stale constant in a docstring is how people end up
        planning a five-minute job that takes an hour.
        """
        fetched = int(self.price_fetch.get("requested", 0) or 0)
        seconds = float(self.price_fetch.get("seconds", 0.0) or 0.0)
        megabytes = float(self.price_fetch.get("megabytes", 0.0) or 0.0)
        if not fetched or not seconds:
            return {}
        return {
            "funds": float(universe_size),
            "fetch_minutes": universe_size * seconds / fetched / 60.0,
            "transfer_gb": universe_size * megabytes / fetched / 1024.0,
            "tickers_per_second": fetched / seconds,
        }

    def render(self, universe_size: int = 15_000) -> str:
        lines: list[str] = []
        add = lines.append

        add("=" * 78)
        add(f"BUILD SUMMARY{'  (--limit %d)' % self.limit if self.limit else ''}")
        add("=" * 78)

        add("")
        add(f"{'table':<22}{'rows':>12}")
        add("-" * 34)
        for table, count in self.rows.items():
            add(f"{table:<22}{count:>12,}")

        add("")
        add(f"{'source':<16}{'funds':>9}{'listings':>10}{'fields won':>12}  status")
        add("-" * 66)
        failed = dict(self.failed_sources)
        for name in sorted(set(self.source_funds) | set(self.source_fields) | set(failed)):
            status = f"FAILED: {failed[name][:40]}" if name in failed else "ok"
            add(
                f"{name:<16}{self.source_funds.get(name, 0):>9,}"
                f"{self.source_listings.get(name, 0):>10,}"
                f"{self.source_fields.get(name, 0):>12,}  {status}"
            )

        add("")
        add("identity")
        add("-" * 60)
        rejected = sum(self.rejected_isins.values())
        add(f"  ISINs rejected (ISO 6166 check digit)   {rejected:>10,}")
        if self.rejected_examples:
            add(f"    e.g. {', '.join(self.rejected_examples[:5])}")
        add(f"  MIC rewrites (venue reconciliation)     {sum(self.mic_rewrites.values()):>10,}")
        for pair, count in sorted(self.mic_rewrites.items()):
            add(f"    {pair:<38}{count:>10,}")
        if self.unknown_mics:
            add(f"  MICs absent from ISO 10383              {sum(self.unknown_mics.values()):>10,}")
        add(f"  listing rows collapsed onto one venue   {self.duplicate_listings:>10,}")
        add(f"  listings with no fund, dropped          {self.orphan_listings:>10,}")
        add(f"  surrogate keys (no ISIN exists)         {self.surrogate_keys:>10,}")
        add(f"  AUM values nulled (sanity bound)        {self.aum_nulled:>10,}")
        add(f"  untradable, dropped (no ticker anywhere){self.untradable_funds:>10,}")
        add(f"    their listings                       {self.untradable_listings:>10,}")

        add("")
        add("coverage")
        add("-" * 60)
        add(f"  funds                                   {self.funds_total:>10,}")
        add(f"  with a resolvable price symbol          {self.funds_with_symbol:>10,}")
        add(f"  listings stamped with a Yahoo symbol    {self.listings_with_symbol:>10,}")
        add(f"  with price history                      {self.funds_with_prices:>10,}")
        add(f"  WITHOUT price history                   {self.funds_without_prices:>10,}")
        if self.unsupported_currencies:
            add(f"  quote currency with no EUR rate         {len(self.unsupported_currencies):>10,}")

        add("")
        add("PEA eligibility")
        add("-" * 60)
        for label in ("true", "false", "null"):
            add(f"  {label:<40}{self.pea.get(label, 0):>10,}")

        add("")
        add("timing")
        add("-" * 60)
        for stage, seconds in self.stage_seconds.items():
            add(f"  {stage:<40}{seconds:>10.1f}s")
        add(f"  {'total':<40}{self.seconds:>10.1f}s")

        extrapolated = self.extrapolate(universe_size)
        if extrapolated:
            add("")
            add(f"extrapolated to {int(extrapolated['funds']):,} funds, from this run's own rate")
            add("-" * 60)
            add(f"  price fetch                             {extrapolated['fetch_minutes']:>10.1f} min")
            add(f"  transferred                             {extrapolated['transfer_gb']:>10.2f} GB")
            add(f"  measured                                {extrapolated['tickers_per_second']:>10.1f} tk/s")

        add("=" * 78)
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stages": self.stages_run,
            "seconds": round(self.seconds, 2),
            "stage_seconds": {k: round(v, 2) for k, v in self.stage_seconds.items()},
            "rows": self.rows,
            "source_funds": self.source_funds,
            "source_listings": self.source_listings,
            "source_fields": self.source_fields,
            "failed_sources": self.failed_sources,
            "rejected_isins": self.rejected_isins,
            "mic_rewrites": self.mic_rewrites,
            "duplicate_listings": self.duplicate_listings,
            "orphan_listings": self.orphan_listings,
            "surrogate_keys": self.surrogate_keys,
            "aum_nulled": self.aum_nulled,
            "untradable_funds": self.untradable_funds,
            "untradable_listings": self.untradable_listings,
            "funds_total": self.funds_total,
            "funds_without_prices": self.funds_without_prices,
            "pea": self.pea,
            "price_fetch": self.price_fetch,
            "extrapolated": self.extrapolate(),
        }


# --------------------------------------------------------------------------- #
# Stage plumbing
# --------------------------------------------------------------------------- #


def _read_table(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def _require(frame: pd.DataFrame | None, stage: str, what: str) -> pd.DataFrame:
    if frame is None:
        raise FileNotFoundError(
            f"stage {stage!r} needs {what}, which neither ran nor exists on disk. "
            f"Re-run with that stage enabled."
        )
    return frame


# --------------------------------------------------------------------------- #
# The stages
# --------------------------------------------------------------------------- #


def stage_universe(
    cfg: BuildConfig,
    state: BuildState,
    report: BuildReport,
    merge_result: universe.MergeResult | None = None,
) -> None:
    """Run and merge the adapters, then cut the result down to `--limit`."""
    if merge_result is None:
        adapters = universe.select_adapters(cfg.sources) if cfg.sources else None
        merge_result = universe.run(
            refresh=cfg.refresh,
            adapters=adapters,
            openfigi_exch_code=cfg.openfigi_exch_code,
        )

    state.merge = merge_result
    merge_report = merge_result.report
    report.source_funds = {s.name: s.funds_kept for s in merge_report.sources}
    report.source_listings = {s.name: s.listings_in for s in merge_report.sources}
    report.source_fields = {s.name: s.fields_won for s in merge_report.sources}
    report.failed_sources = [(s.name, s.error or "unknown") for s in merge_report.failed_sources]
    report.rejected_isins = {k: len(v) for k, v in merge_report.rejected_isins.items()}
    report.rejected_examples = tuple(
        code for codes in merge_report.rejected_isins.values() for code in codes
    )[:10]
    report.mic_rewrites = dict(merge_report.mic_rewrites)
    report.unknown_mics = dict(merge_report.unknown_mics)
    report.duplicate_listings = merge_report.duplicate_listings
    report.orphan_listings = merge_report.orphan_listings
    report.surrogate_keys = merge_report.surrogate_keys
    report.aum_nulled = merge_report.aum_nulled
    # Surfaced in the summary rather than left in merge_report.json: 1,070 rows
    # leaving the universe is exactly the kind of thing that should be visible
    # in the log rather than discovered later as a gap in the totals.
    report.untradable_funds = merge_report.untradable_funds
    report.untradable_listings = merge_report.untradable_listings

    funds, listings = universe.sample(merge_result.funds, merge_result.listings, cfg.limit)
    state.funds, state.listings = funds, listings

    try:
        universe.write_provenance(merge_result, cfg.provenance_root)
    except OSError as exc:  # an audit artefact is never a reason to fail a build
        log.warning("could not write the merge provenance: %s", exc)

    log.info(
        "universe: %d funds, %d listings (%d surrogate keys, %d sources failed)",
        len(funds),
        len(listings),
        merge_report.surrogate_keys,
        len(merge_report.failed_sources),
    )


def stage_eligibility(cfg: BuildConfig, state: BuildState, report: BuildReport) -> None:
    """Classify PEA eligibility and CTO accessibility, and build broker_availability.

    `on_invalid_isin="raise"` is deliberate. The merge has already refused every
    key that is neither a valid ISIN nor a surrogate, so a violation here is a
    bug in the merge rather than a data problem, and it must not be swallowed.
    """
    funds = _require(state.funds, "eligibility", "the funds table")
    evidence = eligibility.load_evidence(cfg.reference)
    state.funds = eligibility.classify_all(funds, evidence, on_invalid_isin="raise")
    state.tables["broker_availability"] = eligibility.broker_availability(evidence)

    verdicts = state.funds["pea_eligible"]
    report.pea = {
        "true": int(verdicts.eq(True).fillna(False).sum()),
        "false": int(verdicts.eq(False).fillna(False).sum()),
        "null": int(verdicts.isna().sum()),
    }
    log.info("eligibility: PEA %s", report.pea)


def stage_resolve(cfg: BuildConfig, state: BuildState, report: BuildReport) -> None:
    """Work out which Yahoo symbol to ask for, per fund, in order of preference."""
    listings = _require(state.listings, "resolve", "the listings table")
    funds = _require(state.funds, "resolve", "the funds table")

    candidates = universe.yahoo_candidates(listings)
    state.symbols = candidates[candidates["isin"].isin(set(funds["isin"]))].reset_index(drop=True)

    # Persist the resolved symbols onto the listings themselves. They were being
    # derived here and thrown away, leaving `listings.yahoo_ticker` null for
    # every row in the published table even though 11,195 funds had prices
    # fetched through those very symbols -- a column the schema declares and
    # nothing filled. Joining on (isin, MIC) rather than ISIN alone matters: one
    # fund has a different symbol on each venue, and ISIN alone would smear one
    # venue's ticker across all of them.
    resolved = candidates[["isin", "exchange_mic", "yahoo_ticker"]].drop_duplicates(
        subset=["isin", "exchange_mic"]
    )
    merged = listings.drop(columns=["yahoo_ticker"]).merge(
        resolved, on=["isin", "exchange_mic"], how="left"
    )
    state.listings = merged.reindex(columns=listings.columns)
    report.listings_with_symbol = int(state.listings["yahoo_ticker"].notna().sum())

    report.funds_total = int(len(funds))
    report.funds_with_symbol = int(state.symbols["isin"].nunique())
    log.info(
        "resolve: %d/%d funds have at least one price symbol (%d candidates, %d listings stamped)",
        report.funds_with_symbol,
        report.funds_total,
        len(state.symbols),
        report.listings_with_symbol,
    )


def stage_prices(
    cfg: BuildConfig,
    state: BuildState,
    report: BuildReport,
    transport: Any = None,
) -> None:
    """Fetch bars and corporate actions, retrying each fund's next venue.

    Incremental by default: `prices.refresh` asks only for what the store is
    missing, which is the difference between 22 MB and 4.8 GB a day. The retry
    rounds exist because Yahoo carries every fund on two to four venues with
    near-identical history and no single venue covers everything -- so a fund
    that comes back empty on Amsterdam is asked for on Milan before being
    recorded as having no history.
    """
    symbols = _require(state.symbols, "prices", "resolved price symbols")
    root = cfg.prices_root
    fetched: list[pd.DataFrame] = []
    actions: list[pd.DataFrame] = []
    totals = {"requested": 0, "succeeded": 0, "bars": 0, "events": 0, "seconds": 0.0, "megabytes": 0.0}

    outstanding = set(symbols["isin"])
    already = set(prices.store_index(root)["isin"]) if root.exists() else set()

    for attempt in range(max(1, cfg.attempts)):
        wanted = symbols[(symbols["rank"] == attempt) & (symbols["isin"].isin(outstanding))]
        if wanted.empty:
            break
        tickers = dict(zip(wanted["isin"].astype(str), wanted["yahoo_ticker"].astype(str)))
        log.info("prices: round %d, %d fund(s)", attempt, len(tickers))

        if cfg.full_prices:
            bars, events = prices.fetch_history(
                tickers, workers=cfg.workers, transport=transport, progress=cfg.progress
            )
        else:
            bars, events = prices.refresh(
                root,
                tickers,
                workers=cfg.workers,
                transport=transport,
                progress=cfg.progress,
                actions_path=cfg.path("corporate_actions"),
            )

        fetch_report = bars.attrs.get("report")
        if fetch_report is not None:
            totals["requested"] += fetch_report.requested
            totals["succeeded"] += fetch_report.succeeded
            totals["bars"] += fetch_report.bars
            totals["events"] += fetch_report.events
            totals["seconds"] += fetch_report.seconds
            totals["megabytes"] += fetch_report.bytes_received / 1e6

        if not bars.empty:
            fetched.append(bars)
            # A fund that answered on this venue is not asked for on the next.
            outstanding -= set(bars["isin"].astype(str))
        if not events.empty:
            actions.append(events)
        # Funds already in the store need no second venue either: an empty
        # incremental answer means "nothing new", not "no history".
        outstanding -= already

    combined = pd.concat(fetched, ignore_index=True) if fetched else None
    if combined is not None and not combined.empty:
        # One venue per fund: a retry round can only add a fund that produced
        # nothing, but a shared symbol could still duplicate an (isin, date).
        combined = combined.drop_duplicates(subset=["isin", "date"], keep="first")
        written = prices.write_prices(combined, root, mode="append")
        log.info("prices: %s rows written to %s", f"{written['rows_written']:,}", root)

    event_frame = pd.concat(actions, ignore_index=True) if actions else pd.DataFrame()
    if not event_frame.empty:
        prices.write_actions(event_frame, cfg.path("corporate_actions"), mode="append")

    totals["megabytes"] = round(totals["megabytes"], 2)
    totals["seconds"] = round(totals["seconds"], 2)
    report.price_fetch = totals


def stage_fx(
    cfg: BuildConfig,
    state: BuildState,
    report: BuildReport,
    rates: pd.DataFrame | None = None,
) -> None:
    """Read the stored history back and restate it, and its dividends, in EUR.

    Reads from the store rather than using the bars the previous stage fetched:
    the incremental path returns a five-day delta, and statistics need the whole
    history. This is also where a fund's quote currency is settled -- the venue's
    own answer, off the bars, rather than the listing table's guess.
    """
    funds = _require(state.funds, "fx", "the funds table")
    keys = [str(key) for key in funds["isin"]]
    stored = prices.read_prices(cfg.prices_root, isins=keys)
    if stored.empty:
        log.warning("fx: the price store holds nothing for these funds")
        state.panel = {}
        state.actions = pd.DataFrame()
        return

    rates = fx.load_rates(refresh=cfg.refresh) if rates is None else rates
    currency_by_isin = (
        stored.dropna(subset=["currency"])
        .groupby("isin")["currency"]
        .last()
        .astype(str)
        .to_dict()
    )
    panel = {key: block.reset_index(drop=True) for key, block in stored.groupby("isin", sort=True)}

    conversion = fx.convert_all(panel, currency_by_isin, rates)
    report.unsupported_currencies = dict(conversion.unsupported)
    if conversion.unsupported:
        log.warning(
            "fx: %d fund(s) quoted in a currency with no euro rate: %s",
            len(conversion.unsupported),
            sorted(set(conversion.unsupported.values()))[:5],
        )
    state.panel = conversion.prices
    state.actions = _actions_in_eur(cfg, keys, currency_by_isin, rates)


def _actions_in_eur(
    cfg: BuildConfig,
    keys: Sequence[str],
    currency_by_isin: Mapping[str, str],
    rates: pd.DataFrame,
) -> pd.DataFrame:
    """Restate dividend amounts in EUR, at the rate of the day they were paid.

    The adjustment that follows subtracts each distribution from the price it
    came out of, so a EUR price minus a USD dividend would overstate the
    adjustment by the exchange rate. Splits carry a ratio, not an amount, and are
    currency-free -- they pass through untouched.
    """
    stored = _read_table(cfg.path("corporate_actions"))
    if stored is None or stored.empty:
        return pd.DataFrame()

    events = stored[stored["isin"].isin(set(keys))].copy()
    if events.empty:
        return events

    events["date"] = pd.to_datetime(events["date"])
    # The event's own currency when it has one, else the fund's quote currency.
    quoted = events["currency"].where(
        events["currency"].notna(), events["isin"].map(dict(currency_by_isin))
    )

    resolved = [_normalise_currency(value) for value in quoted]
    events["currency"] = [iso for iso, _unit in resolved]
    # The sub-unit multiplier rides along as a column rather than a parallel
    # Series: `attach_rates` sorts by date and hands back a fresh index, so
    # anything aligned on the input's index silently lands on the wrong rows.
    events["_unit"] = [unit for _iso, unit in resolved]
    events["amount"] = pd.to_numeric(events["amount"], errors="coerce").astype("float64")

    merged = fx.attach_rates(events, rates)
    # A euro amount needs no rate, and the ECB table carries none for its own
    # base currency -- so that null is a one, not a gap.
    rate = merged["rate_to_eur"].where(merged["currency"].ne(config.BASE_CURRENCY), 1.0)

    dividends = merged["kind"].eq("dividend")
    merged.loc[dividends, "amount"] = (
        merged.loc[dividends, "amount"] * merged.loc[dividends, "_unit"] / rate[dividends]
    )
    merged["currency"] = config.BASE_CURRENCY

    # A dividend we cannot convert becomes null rather than crossing over in the
    # wrong currency. That understates the fund's total return, which is the
    # recoverable error; the alternative inflates it invisibly.
    lost = int((merged["amount"].isna() & dividends).sum())
    if lost:
        log.warning("fx: %d dividend(s) had no euro rate and were dropped from the adjustment", lost)
    return merged.drop(columns=[c for c in ("rate_to_eur", "_unit") if c in merged.columns])


def _normalise_currency(value: object) -> tuple[str | None, float]:
    """(ISO code, multiplier), or (None, 1.0) when the code means nothing to fx."""
    try:
        return fx.normalise_currency(str(value))
    except fx.UnsupportedCurrencyError:
        return None, 1.0


def stage_adjust(cfg: BuildConfig, state: BuildState, report: BuildReport) -> None:
    """Rebuild the total-return series from EUR bars and EUR distributions.

    Never the vendor's adjusted column: Yahoo publishes 40 dividend events for
    ISF.L and applies none of them, understating that fund's ten-year annualised
    return by 4.03 points. `adjust.py` reproduces the vendor's figure to the
    basis point wherever the vendor is right, and fixes it where it is not.
    """
    if not state.panel:
        return

    long = pd.concat(
        [block.assign(isin=key) for key, block in state.panel.items()], ignore_index=True
    )
    adjusted = adjust.apply_adjustment(long, state.actions)
    state.panel = {
        key: block.reset_index(drop=True)
        for key, block in adjusted.groupby("isin", sort=True)
    }

    adjustment = adjusted.attrs.get("adjustment")
    if adjustment is not None:
        log.info("adjust: %s", adjustment.summary())


def stage_stats(cfg: BuildConfig, state: BuildState, report: BuildReport) -> None:
    """Compute performance, yearly and monthly returns over the adjusted panel."""
    funds = _require(state.funds, "stats", "the funds table")
    keys = [str(key) for key in funds["isin"]]
    if not state.panel and len(prices.store_index(cfg.prices_root)):
        # The bars exist but nothing put them in front of the engine, which would
        # publish a table of nulls that reads as a data outage.
        log.warning(
            "stats: the price store is not empty but no panel reached this stage; "
            "the fx and adjust stages have to run too (--only fx,adjust,stats,write)"
        )
    # Every fund gets a row, including the ones with no bars: a missing row and a
    # null row mean different things to the frontend.
    panel = {key: state.panel.get(key, _empty_bars()) for key in keys}

    performance, yearly, monthly = stats.compute_all(panel, as_of=cfg.as_of)
    state.tables["performance"] = performance
    state.tables["returns_yearly"] = yearly
    state.tables["returns_monthly"] = monthly

    with_history = int(
        sum(1 for block in state.panel.values() if block is not None and not block.empty)
    )
    report.funds_with_prices = with_history
    report.funds_without_prices = len(keys) - with_history


def _empty_bars() -> pd.DataFrame:
    frame = pd.DataFrame(columns=[f.name for f in schema.PRICES])
    frame["date"] = pd.to_datetime(frame["date"])
    for column in ("open", "high", "low", "close", "adj_close", "volume"):
        frame[column] = frame[column].astype("float64")
    return frame


def stage_write(cfg: BuildConfig, state: BuildState, report: BuildReport) -> None:
    """Write every table this build produced, conformed to the declared schema."""
    cfg.out.mkdir(parents=True, exist_ok=True)

    if state.funds is not None:
        state.tables["funds"] = universe.conform(state.funds, "funds")
    if state.listings is not None:
        state.tables["listings"] = universe.conform(state.listings, "listings")

    for table in WRITTEN_TABLES:
        arrow = state.tables.get(table)
        if arrow is None:
            continue
        path = cfg.path(table)
        pq.write_table(arrow, path, compression="zstd")
        report.rows[table] = arrow.num_rows
        log.info("wrote %s (%d rows)", path, arrow.num_rows)


# --------------------------------------------------------------------------- #
# The run
# --------------------------------------------------------------------------- #


def run(
    cfg: BuildConfig | None = None,
    *,
    merge_result: universe.MergeResult | None = None,
    transport: Any = None,
    rates: pd.DataFrame | None = None,
) -> BuildReport:
    """Execute the pipeline, one stage at a time, and return what it did.

    `merge_result`, `transport` and `rates` are injection points rather than
    conveniences: they are what let the end-to-end test exercise every stage of
    this file without a network, which is the only way an orchestrator ever gets
    tested at all.
    """
    cfg = cfg or BuildConfig()
    state = BuildState()
    report = BuildReport(limit=cfg.limit)

    stages: list[tuple[str, Callable[[], None]]] = [
        ("universe", lambda: stage_universe(cfg, state, report, merge_result)),
        ("eligibility", lambda: stage_eligibility(cfg, state, report)),
        ("resolve", lambda: stage_resolve(cfg, state, report)),
        ("prices", lambda: stage_prices(cfg, state, report, transport)),
        ("fx", lambda: stage_fx(cfg, state, report, rates)),
        ("adjust", lambda: stage_adjust(cfg, state, report)),
        ("stats", lambda: stage_stats(cfg, state, report)),
        ("write", lambda: stage_write(cfg, state, report)),
    ]

    for name, stage in stages:
        if not cfg.runs(name):
            _resume(cfg, state, name)
            continue
        started = time.perf_counter()
        stage()
        report.stage_seconds[name] = time.perf_counter() - started
        report.stages_run.append(name)

    _finalise(cfg, state, report)
    return report


def _resume(cfg: BuildConfig, state: BuildState, stage: str) -> None:
    """Load a skipped stage's output from disk so the next stage can still run.

    This is what makes `--only stats` a two-second loop instead of a rebuild.
    Nothing is fabricated: if the file is not there the next stage says so by
    name rather than failing on a None twenty frames down.
    """
    if stage == "universe":
        state.funds = _read_table(cfg.path("funds"))
        state.listings = _read_table(cfg.path("listings"))
        if state.funds is not None and cfg.limit:
            state.funds, state.listings = universe.sample(
                state.funds, state.listings, cfg.limit
            )
        if state.funds is not None:
            log.info(
                "resumed %d funds and %d listings from %s",
                len(state.funds),
                0 if state.listings is None else len(state.listings),
                cfg.out,
            )
    elif stage == "resolve" and state.listings is not None:
        state.symbols = universe.yahoo_candidates(state.listings)


def _finalise(cfg: BuildConfig, state: BuildState, report: BuildReport) -> None:
    """Fill in the counters the summary needs, whichever stages actually ran."""
    if state.funds is not None:
        report.funds_total = int(len(state.funds))
        report.rows.setdefault("funds", int(len(state.funds)))
        if "pea_eligible" in state.funds.columns and not report.pea:
            verdicts = state.funds["pea_eligible"]
            report.pea = {
                "true": int(verdicts.eq(True).fillna(False).sum()),
                "false": int(verdicts.eq(False).fillna(False).sum()),
                "null": int(verdicts.isna().sum()),
            }
    if state.listings is not None:
        report.rows.setdefault("listings", int(len(state.listings)))

    index = prices.store_index(cfg.prices_root)
    if not index.empty and state.funds is not None:
        held = index[index["isin"].isin(set(state.funds["isin"]))]
        report.rows["prices"] = int(pd.to_numeric(held["bars"], errors="coerce").fillna(0).sum())
        if not report.funds_with_prices:
            report.funds_with_prices = int(len(held))
            report.funds_without_prices = report.funds_total - report.funds_with_prices

    actions = _read_table(cfg.path("corporate_actions"))
    if actions is not None:
        report.rows["corporate_actions"] = int(len(actions))

    for table, arrow in state.tables.items():
        report.rows.setdefault(table, arrow.num_rows)

    # Stable, readable order rather than insertion order.
    order = ("funds", "listings", "prices", "corporate_actions", *WRITTEN_TABLES)
    report.rows = {
        table: report.rows[table]
        for table in dict.fromkeys(order + tuple(report.rows))
        if table in report.rows
    }


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _stage_list(raw: str | None) -> tuple[str, ...] | None:
    if not raw:
        return None
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def parse_args(argv: Sequence[str] | None = None) -> tuple[BuildConfig, argparse.Namespace]:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline.build",
        description="Build the ETF database end to end.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="build over a hash-ordered sample of N funds (seconds, not minutes)",
    )
    parser.add_argument("--out", type=Path, default=config.PROCESSED, help="output directory")
    parser.add_argument(
        "--only", default=None, metavar="STAGES", help=f"run only these, comma separated: {','.join(STAGES)}"
    )
    parser.add_argument("--skip", default=None, metavar="STAGES", help="run everything except these")
    parser.add_argument(
        "--sources", default=None, metavar="NAMES", help="restrict the universe merge to these adapters"
    )
    parser.add_argument(
        "--full-prices",
        action="store_true",
        help="cold backfill instead of the incremental refresh (~4.8 GB at full scale)",
    )
    parser.add_argument("--workers", type=int, default=prices.DEFAULT_WORKERS)
    parser.add_argument(
        "--attempts",
        type=int,
        default=2,
        help="venues to try per fund before recording it as having no history",
    )
    parser.add_argument("--refresh", action="store_true", help="bypass every on-disk cache")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument(
        "--openfigi-full",
        action="store_true",
        help="ask OpenFIGI for every venue, not just the US pass (52 min keyless)",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--json", action="store_true", help="print the report as JSON as well")

    args = parser.parse_args(argv)

    only, skip = _stage_list(args.only), _stage_list(args.skip)
    stages = only if only else STAGES
    if skip:
        stages = tuple(stage for stage in stages if stage not in skip)

    return BuildConfig(
        out=args.out,
        stages=tuple(stages),
        limit=args.limit,
        refresh=args.refresh,
        full_prices=args.full_prices,
        workers=args.workers,
        attempts=args.attempts,
        progress=not args.no_progress,
        sources=_stage_list(args.sources),
        openfigi_exch_code=None if args.openfigi_full else universe.OPENFIGI_US_PASS,
    ), args


def main(argv: Sequence[str] | None = None) -> int:
    cfg, args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    report = run(cfg)
    print(report.render())
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    # A build that produced no funds is a failure, however cleanly it degraded.
    return 0 if report.funds_total else 1


if __name__ == "__main__":
    sys.exit(main())
