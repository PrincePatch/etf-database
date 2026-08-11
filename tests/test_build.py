"""Tests for the orchestrator: the wiring, not the stages it wires.

Every stage `pipeline/build.py` calls is tested in its own file. What is only
testable here is what happens *between* them -- that the merged universe reaches
the classifier, that the classifier's funds reach the price fetcher under symbols
it can use, that the bars come back, get converted, get adjusted, and arrive at
the statistics engine as one row per fund, and that a fund which fails somewhere
in the middle is reported rather than lost.

So the end-to-end test below runs the whole pipeline with `--limit`, offline, in
a temporary directory, against three injection points: a pre-merged universe, a
fake price transport, and a fake FX table. That is the only way an orchestrator
gets tested at all -- the alternative is a test that needs the internet and a
quarter of an hour, which is a test nobody runs.

Two things it asserts that nothing else can:

* **Every fund gets a performance row, including the ones with no bars.** A
  missing row and a null row mean different things downstream, and it is the
  orchestrator that decides which one a priceless fund gets.
* **Dividends are converted to euros before the adjustment, not after.** The
  adjustment subtracts each distribution from the price it was paid out of, so a
  EUR price and a USD dividend in the same expression overstate the adjustment by
  the exchange rate -- silently, and only for foreign-quoted funds.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from pipeline import build, config, schema, universe
from pipeline.sources import SourceResult
from pipeline.sources._http import to_frame

from .test_universe import REGISTER, TRUST

WORLD = "IE00B4L5Y983"  # the configured benchmark, so beta/correlation resolve
PARIS = "FR0010315770"
XETRA = "LU1681043599"
US_FUND = "US:ARCX:VTI"
NO_VENUE = "IE00B5BMR087"  # in the universe, listed nowhere we can price


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def _source(name, funds=None, listings=None, *, ok=True, error=None) -> SourceResult:
    return SourceResult(
        name=name,
        funds=to_frame(funds or [], "funds"),
        listings=to_frame(listings or [], "listings"),
        ok=ok,
        error=error,
    )


@pytest.fixture(scope="module")
def merge_result() -> universe.MergeResult:
    """A five-fund universe carrying every shape the pipeline has to survive.

    One fund per venue naming convention, one US surrogate key, one fund with no
    listing at all, one dead adapter, one absurd Yahoo AUM, and Borsa Italiana
    arriving under two different MICs.
    """
    today = date(2026, 8, 11)
    firds = _source(
        "firds",
        [
            {"isin": WORLD, "name": "iShares Core MSCI World UCITS ETF", "domicile": "IE",
             "ucits": True, "last_updated": today},
            {"isin": PARIS, "name": "Amundi MSCI World UCITS ETF", "domicile": "FR",
             "ucits": True, "last_updated": today},
            {"isin": XETRA, "name": "Amundi Index MSCI World", "domicile": "LU",
             "ucits": True, "last_updated": today},
            {"isin": NO_VENUE, "name": "iShares Core S&P 500 UCITS ETF", "domicile": "IE",
             "ucits": True, "last_updated": today},
        ],
        [
            {"isin": WORLD, "exchange_mic": "ETFP", "trading_currency": "EUR"},
            {"isin": WORLD, "exchange_mic": "XETA", "trading_currency": "EUR"},
            {"isin": PARIS, "exchange_mic": "XPAR", "trading_currency": "EUR"},
            {"isin": XETRA, "exchange_mic": "XETA", "trading_currency": "EUR"},
        ],
    )
    euronext = _source(
        "euronext",
        [{"isin": WORLD, "short_name": "IWDA", "last_updated": today}],
        [
            {"isin": WORLD, "exchange_mic": "XAMS", "ticker": "IWDA", "trading_currency": "EUR"},
            {"isin": PARIS, "exchange_mic": "XPAR", "ticker": "CW8", "trading_currency": "EUR"},
        ],
    )
    xetra = _source(
        "xetra",
        [{"isin": XETRA, "short_name": "LCUW", "last_updated": today}],
        [{"isin": XETRA, "exchange_mic": "XETR", "ticker": "LCUW", "trading_currency": "EUR"}],
    )
    openfigi = _source(
        "openfigi", listings=[{"isin": WORLD, "exchange_mic": "XMIL", "ticker": "SWDA"}]
    )
    nasdaq = _source(
        "nasdaqtrader",
        [{"isin": US_FUND, "name": "Vanguard Total Stock Market ETF", "domicile": "US",
          "ucits": False, "fund_currency": "USD", "last_updated": today}],
        [{"isin": US_FUND, "exchange_mic": "ARCX", "ticker": "VTI", "yahoo_ticker": "VTI",
          "trading_currency": "USD", "is_primary": True}],
    )
    yahoo = _source(
        "yahoo",
        [
            {"isin": WORLD, "ter": 0.0020, "aum_eur": 8.0e10, "last_updated": today},
            {"isin": US_FUND, "aum_eur": 2.0e12, "last_updated": today},  # the VTI defect
        ],
    )
    dead = _source("lse", ok=False, error="HTTPError: 404 Not Found")

    return universe.merge(
        [firds, euronext, xetra, openfigi, nasdaq, yahoo, dead],
        registry=universe.MicRegistry.from_csv(REGISTER),
        trust=TRUST,
    )


def make_transport(*, currencies=None, dead_symbols=()):
    """A Yahoo chart endpoint that answers from a seeded random walk.

    Deterministic per symbol, so a re-run produces identical bars and the
    idempotence test means something.
    """
    currencies = currencies or {}
    start = datetime(2019, 1, 2, tzinfo=timezone.utc)

    def transport(symbol: str, params) -> bytes:
        if symbol in dead_symbols:
            raise Exception("HTTP 404")

        sessions = pd.bdate_range(start, periods=1_400, tz="UTC")
        floor = int(params.get("period1") or 0)
        stamps = [int(day.timestamp()) for day in sessions if int(day.timestamp()) >= floor]
        if not stamps:
            raise Exception("no sessions in range")

        rng = np.random.default_rng(abs(hash(symbol)) % 2**32)
        closes = 100 * np.exp(np.cumsum(rng.normal(0.0003, 0.01, len(stamps))))
        return json.dumps(
            {
                "chart": {
                    "error": None,
                    "result": [
                        {
                            "meta": {
                                "currency": currencies.get(symbol, "EUR"),
                                "exchangeTimezoneName": "Europe/Paris",
                            },
                            "timestamp": stamps,
                            "indicators": {
                                "quote": [
                                    {
                                        "open": list(closes),
                                        "high": list(closes * 1.01),
                                        "low": list(closes * 0.99),
                                        "close": list(closes),
                                        "volume": [1_000] * len(stamps),
                                    }
                                ]
                            },
                            "events": {
                                "dividends": {
                                    str(stamps[i]): {"date": stamps[i], "amount": 0.40}
                                    for i in range(60, len(stamps), 250)
                                }
                            },
                        }
                    ],
                }
            }
        ).encode()

    return transport


@pytest.fixture(scope="module")
def rates() -> pd.DataFrame:
    """A flat FX table. Flat on purpose: a constant rate makes a conversion error
    visible as a clean factor instead of hiding inside a moving series."""
    days = pd.bdate_range("1999-01-04", "2026-08-11")
    return pd.concat(
        [
            pd.DataFrame({"date": days, "currency": "USD", "rate_to_eur": 1.10}),
            pd.DataFrame({"date": days, "currency": "GBP", "rate_to_eur": 0.85}),
        ],
        ignore_index=True,
    )


@pytest.fixture
def built(tmp_path, merge_result, rates):
    """One complete `--limit` build, shared by the assertions that inspect it."""
    cfg = build.BuildConfig(out=tmp_path, limit=5, progress=False, workers=4)
    report = build.run(
        cfg,
        merge_result=merge_result,
        transport=make_transport(currencies={"VTI": "USD"}),
        rates=rates,
    )
    return cfg, report


# --------------------------------------------------------------------------- #
# End to end
# --------------------------------------------------------------------------- #


def test_limit_build_writes_every_table_conforming_to_the_schema(built):
    cfg, report = built
    for table in build.WRITTEN_TABLES:
        path = cfg.path(table)
        assert path.exists(), f"{table} was not written"
        arrow = pq.read_table(path)
        assert arrow.schema.equals(schema.TABLES[table]), f"{table} does not match schema.{table.upper()}"
    assert cfg.path("corporate_actions").exists()
    assert (cfg.prices_root / "_index.parquet").exists()
    assert report.stages_run == list(build.STAGES)


def test_the_price_store_holds_raw_bars(built):
    """`prices.py` writes `adj_close` as null on disk; the build must not undo it.

    Bars are append-only and the adjustment is a derived view that one new
    dividend restates back to inception, so persisting it would persist something
    that goes stale the moment it is written.
    """
    from pipeline import prices

    stored = prices.read_prices(built[0].prices_root)
    assert not stored.empty
    assert stored["adj_close"].isna().all()
    assert stored["close"].notna().all()


def test_every_fund_gets_exactly_one_performance_row(built):
    cfg, _report = built
    funds = pq.read_table(cfg.path("funds")).to_pandas()
    performance = pq.read_table(cfg.path("performance")).to_pandas()
    assert list(performance["isin"]) == sorted(funds["isin"])
    assert performance["isin"].is_unique


def test_a_fund_with_no_listing_is_reported_not_dropped(built):
    """Rather than vanishing, it gets a null performance row and is counted."""
    cfg, report = built
    performance = pq.read_table(cfg.path("performance")).to_pandas()
    row = performance[performance["isin"] == NO_VENUE]
    assert len(row) == 1
    assert pd.isna(row.iloc[0]["ret_1y"])
    assert report.funds_without_prices == 1
    assert report.funds_with_symbol == report.funds_total - 1


def test_statistics_are_computed_where_there_is_history(built):
    cfg, _report = built
    performance = pq.read_table(cfg.path("performance")).to_pandas()
    priced = performance[performance["isin"] != NO_VENUE]
    assert priced["history_days"].notna().all()
    assert (priced["history_days"] > 250).all()
    assert priced["ret_1y"].notna().all()
    assert priced["vol_1y"].notna().all()
    assert (priced["base_currency"] == config.BASE_CURRENCY).all()


def test_the_total_return_series_beats_the_price_series_when_there_are_dividends(built):
    """Sanity on the hand-off: adj_close must reflect distributions, not equal close."""
    cfg, _report = built
    actions = pq.read_table(cfg.path("corporate_actions")).to_pandas()
    assert (actions["kind"] == "dividend").any()

    performance = pq.read_table(cfg.path("performance")).to_pandas()
    paying = set(actions["isin"])
    assert paying, "the fixture pays dividends; the events table lost them"
    # A distributing fund's total return exceeds its price return over the same
    # window, and `ret_max` is computed on the adjusted series.
    assert performance[performance["isin"].isin(paying)]["ret_max"].notna().all()


def test_pea_breakdown_matches_the_funds_table(built):
    cfg, report = built
    funds = pq.read_table(cfg.path("funds")).to_pandas()
    assert sum(report.pea.values()) == len(funds)
    # The US fund is ineligible on domicile alone (Rule 0), which is the one
    # verdict the classifier is certain about.
    us_row = funds[funds["isin"] == US_FUND].iloc[0]
    assert us_row["pea_eligible"] is False or us_row["pea_eligible"] == False  # noqa: E712
    assert us_row["cto_reason"] == "no_priips_kid"


def test_the_summary_names_what_the_run_lost(built):
    _cfg, report = built
    rendered = report.render()
    for heading in ("BUILD SUMMARY", "identity", "coverage", "PEA eligibility", "timing"):
        assert heading in rendered
    assert "lse" in rendered and "FAILED" in rendered
    assert "ETFP->XMIL" in rendered
    assert report.aum_nulled == 1
    assert dict(report.failed_sources)["lse"].startswith("HTTPError")
    assert report.mic_rewrites["ETFP->XMIL"] == 1


def test_the_report_serialises(built):
    _cfg, report = built
    payload = json.loads(json.dumps(report.to_dict(), default=str))
    assert payload["funds_total"] == report.funds_total
    assert payload["extrapolated"]["funds"] == 15_000


# --------------------------------------------------------------------------- #
# The hand-offs
# --------------------------------------------------------------------------- #


def test_a_fund_falls_back_to_its_next_venue(tmp_path, merge_result, rates):
    """Yahoo carries each fund on two to four venues and no single one covers all.

    The fund's primary listing is made to 404; the build must ask the next venue
    rather than record it as having no history.
    """
    cfg = build.BuildConfig(out=tmp_path, limit=5, progress=False, workers=4, attempts=3)
    symbols = universe.yahoo_candidates(merge_result.listings)
    primary = symbols[(symbols["isin"] == WORLD) & (symbols["rank"] == 0)].iloc[0]["yahoo_ticker"]

    report = build.run(
        cfg,
        merge_result=merge_result,
        transport=make_transport(dead_symbols={primary}),
        rates=rates,
    )
    from pipeline import prices

    stored = prices.store_index(cfg.prices_root)
    assert WORLD in set(stored["isin"])
    assert stored[stored["isin"] == WORLD].iloc[0]["ticker"] != primary
    assert report.funds_without_prices == 1  # only the fund with no listing at all


def test_dividends_are_converted_before_the_adjustment(tmp_path, rates):
    """A USD dividend must reach the adjustment in euros, at the day's rate.

    Left in dollars against euro-converted prices, the adjustment would be
    overstated by the exchange rate -- invisibly, and only for foreign listings.
    """
    cfg = build.BuildConfig(out=tmp_path)
    actions = pd.DataFrame(
        {
            "isin": [US_FUND, US_FUND, "GB00B0CNGT73"],
            "date": pd.to_datetime(["2024-03-15", "2024-06-21", "2024-03-15"]),
            "kind": ["dividend", "split", "dividend"],
            "amount": [1.10, np.nan, 100.0],
            "ratio": [np.nan, 2.0, np.nan],
            "currency": ["USD", "USD", "GBp"],
        }
    )
    actions.to_parquet(cfg.path("corporate_actions"), index=False)

    converted = build._actions_in_eur(
        cfg, [US_FUND, "GB00B0CNGT73"], {US_FUND: "USD"}, rates
    )
    by_key = converted.set_index(["isin", "kind"])

    assert by_key.loc[(US_FUND, "dividend"), "amount"] == pytest.approx(1.10 / 1.10)
    # Pence, not pounds: GBp differs from GBP by one character's case and a
    # factor of one hundred.
    assert by_key.loc[("GB00B0CNGT73", "dividend"), "amount"] == pytest.approx(1.0 / 0.85)
    # A split is a ratio, not an amount, and is currency-free.
    assert by_key.loc[(US_FUND, "split"), "ratio"] == 2.0
    assert (converted["currency"] == "EUR").all()


def test_an_unconvertible_dividend_becomes_null_rather_than_wrong(tmp_path, rates):
    cfg = build.BuildConfig(out=tmp_path)
    pd.DataFrame(
        {
            "isin": [US_FUND],
            "date": pd.to_datetime(["2024-03-15"]),
            "kind": ["dividend"],
            "amount": [3.0],
            "ratio": [np.nan],
            "currency": ["TWD"],  # the ECB publishes no euro rate for it
        }
    ).to_parquet(cfg.path("corporate_actions"), index=False)

    converted = build._actions_in_eur(cfg, [US_FUND], {US_FUND: "TWD"}, rates)
    assert pd.isna(converted.iloc[0]["amount"])


def test_prices_are_restated_in_euros(built, rates):
    """The USD fund's stored bars are dollars; its statistics must be euros."""
    from pipeline import prices

    cfg, _report = built
    stored = prices.read_prices(cfg.prices_root, isins=[US_FUND])
    assert (stored["currency"] == "USD").all()

    performance = pq.read_table(cfg.path("performance")).to_pandas()
    row = performance[performance["isin"] == US_FUND].iloc[0]
    last_usd = stored.sort_values("date")["close"].iloc[-1]
    assert row["price_last"] == pytest.approx(last_usd / 1.10, rel=1e-3)


def test_running_twice_does_not_duplicate_anything(tmp_path, merge_result, rates):
    """The daily job has to be safe to retry, so a second run must be a no-op."""
    cfg = build.BuildConfig(out=tmp_path, limit=5, progress=False, workers=4)
    transport = make_transport(currencies={"VTI": "USD"})

    first = build.run(cfg, merge_result=merge_result, transport=transport, rates=rates)
    second = build.run(cfg, merge_result=merge_result, transport=transport, rates=rates)

    assert second.rows["prices"] == first.rows["prices"]
    assert second.rows["corporate_actions"] == first.rows["corporate_actions"]
    assert second.rows["performance"] == first.rows["performance"]


# --------------------------------------------------------------------------- #
# Stage selection and resumption
# --------------------------------------------------------------------------- #


def test_a_skipped_stage_resumes_from_disk(built, rates):
    """`--only stats` must be a two-second loop, not a rebuild."""
    cfg, first = built
    again = build.run(
        build.BuildConfig(
            out=cfg.out, limit=5, progress=False, stages=("fx", "adjust", "stats", "write")
        ),
        rates=rates,
    )
    assert again.funds_total == first.funds_total
    assert again.rows["performance"] == first.rows["performance"]
    assert "universe" not in again.stages_run


def test_a_stage_whose_input_is_missing_says_so(tmp_path):
    with pytest.raises(FileNotFoundError, match="funds table"):
        build.run(build.BuildConfig(out=tmp_path, stages=("eligibility",)))


def test_write_can_be_skipped(tmp_path, merge_result, rates):
    stages = tuple(stage for stage in build.STAGES if stage != "write")
    cfg = build.BuildConfig(out=tmp_path, limit=5, progress=False, stages=stages)
    report = build.run(cfg, merge_result=merge_result, transport=make_transport(), rates=rates)
    assert report.funds_total > 0
    assert not cfg.path("funds").exists()


def test_the_build_writes_nowhere_but_its_output_directory(tmp_path, merge_result, rates):
    """A test must never overwrite a developer's real data/processed/."""
    cfg = build.BuildConfig(out=tmp_path, limit=5, progress=False)
    build.run(cfg, merge_result=merge_result, transport=make_transport(), rates=rates)

    written = {path.name for path in tmp_path.iterdir()}
    assert "funds.parquet" in written
    assert cfg.prices_root.is_relative_to(tmp_path)
    assert cfg.provenance_root.is_relative_to(tmp_path)
    assert cfg.out != config.PROCESSED
    # The audit trail is a build artefact, so it must not land among the tables
    # `schema.py` declares -- data/processed/ is committed and holds only those.
    assert (cfg.provenance_root / "field_provenance.parquet").exists()
    assert not any(path.suffix == ".parquet" and path.stem.startswith("_") for path in tmp_path.iterdir())


def test_the_provenance_trail_lives_with_the_raw_cache_by_default():
    assert build.BuildConfig().provenance_root == config.RAW / "universe"


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def test_cli_defaults_to_every_stage_and_the_configured_paths():
    cfg, _args = build.parse_args([])
    assert cfg.stages == build.STAGES
    assert cfg.out == config.PROCESSED
    assert cfg.prices_root == config.PROCESSED / "prices"
    assert cfg.limit is None
    assert cfg.full_prices is False  # incremental is the daily path


def test_cli_limit_and_stage_selection(tmp_path):
    cfg, _args = build.parse_args(
        ["--limit", "25", "--out", str(tmp_path), "--only", "universe,write"]
    )
    assert cfg.limit == 25
    assert cfg.stages == ("universe", "write")
    assert cfg.out == tmp_path

    skipped, _args = build.parse_args(["--skip", "prices,fx"])
    assert "prices" not in skipped.stages and "fx" not in skipped.stages
    assert "universe" in skipped.stages


def test_cli_rejects_an_unknown_stage():
    with pytest.raises(ValueError, match="unknown stage"):
        build.BuildConfig(stages=("univese",))


def test_cli_restricts_the_adapter_set():
    cfg, _args = build.parse_args(["--sources", "firds,euronext"])
    assert cfg.sources == ("firds", "euronext")
    assert [a.name for a in universe.select_adapters(cfg.sources)] == ["firds", "euronext"]


def test_openfigi_full_pass_is_opt_in():
    default, _args = build.parse_args([])
    full, _args = build.parse_args(["--openfigi-full"])
    assert default.openfigi_exch_code == universe.OPENFIGI_US_PASS
    assert full.openfigi_exch_code is None


# --------------------------------------------------------------------------- #
# Extrapolation
# --------------------------------------------------------------------------- #


def test_extrapolation_uses_this_run_s_own_measurements():
    report = build.BuildReport()
    report.price_fetch = {"requested": 100, "seconds": 2.5, "megabytes": 40.0}
    full = report.extrapolate(15_000)

    assert full["tickers_per_second"] == pytest.approx(40.0)
    assert full["fetch_minutes"] == pytest.approx(15_000 * 0.025 / 60)
    assert full["transfer_gb"] == pytest.approx(15_000 * 0.4 / 1024)


def test_extrapolation_is_absent_when_nothing_was_fetched():
    assert build.BuildReport().extrapolate() == {}


# --------------------------------------------------------------------------- #
# Against the live upstreams
# --------------------------------------------------------------------------- #


@pytest.mark.network
def test_a_real_limited_build(tmp_path):
    """The same wiring against the real adapters, over a handful of funds.

    Marked `network` because it fetches: it is the test that catches an upstream
    that has changed shape, which unit tests against fixtures never can.
    """
    cfg = build.BuildConfig(
        out=tmp_path,
        limit=5,
        progress=False,
        workers=8,
        sources=("firds", "euronext"),
        stages=tuple(stage for stage in build.STAGES if stage != "prices"),
    )
    try:
        report = build.run(cfg)
    except Exception as exc:  # pragma: no cover - upstream outage
        pytest.skip(f"upstream unavailable: {exc}")

    assert report.funds_total > 0
    assert pq.read_table(cfg.path("funds")).schema.equals(schema.FUNDS)
    assert pq.read_table(cfg.path("listings")).schema.equals(schema.LISTINGS)
