"""Build the published dataset the static site reads, from the processed tables.

`docs/` is a plain directory on GitHub Pages: no server, no API, no database
process. The browser runs DuckDB-WASM and range-reads these Parquet files
directly over HTTP, pulling only the byte ranges a query touches -- GitHub Pages
answers `206 Partial Content` with `Accept-Ranges: bytes`, which is the load-
bearing assumption of the whole design. So the shape of what this module writes
*is* the query plan: a column the screener never reads costs a visitor nothing,
and a file laid out badly costs every visitor on every query.

What may be published, and what may not
---------------------------------------
This project declines sources whose terms forbid automated access -- justETF is
excluded on exactly that ground -- and the price endpoint the pipeline depends
on carries the same blanket `User-agent: * / Disallow: /`. The position that
follows is: **use it to compute, publish only what we derived.**

So `docs/data/` carries metadata and our own statistics in full, plus one price
artefact: a **weekly total-return index, denominated in EUR and rebased to 100
at each fund's first observation**. That series is a transformation of the raw
feed -- closes, plus the dividend reconstruction from `pipeline.adjust`, plus
the ECB conversion from `pipeline.fx`, plus the rebase -- and it can no longer
reproduce a quote.

The distinction is sharp and this module enforces it: **no `open`, `high`,
`low`, `close` or `volume` column, and no daily bar series, is ever written to
`out_dir`.** A weekly subset of raw closes would still be raw closes; only the
rebased index leaves. A single current price for display remains available to
the site through `performance.price_last`, which is one number per fund rather
than a series.

Layout decisions
----------------
**One wide table for the screener.** `funds.parquet` denormalises funds ⋈
performance ⋈ primary listing into one row per ISIN. The screener filters and
sorts over ~40 numeric columns; a join in the browser would mean fetching both
sides in full, while a single table lets DuckDB read only the handful of column
chunks a given query mentions. It is written as a single row group so each
column is one contiguous byte range, i.e. one HTTP request.

**The index ships as one file.** Weekly sampling and a single float column take
~15 years of history from ~3,800 daily bars in five columns to ~780 points in
one, which is roughly two orders of magnitude off the payload; the whole index
for the universe fits in a file far below GitHub's 100 MB hard limit, so the
sharding and release-asset workarounds a daily OHLCV export needed are gone.
Rows are ordered by (isin, date) and the row groups are kept small, because the
query that actually repeats is "one fund's series" and a smaller group means
less over-read around it. If the universe ever grew enough to threaten the file
limit, sharding by fund is the first lever and a GitHub Release asset (2 GB per
file, no bandwidth limit) the second.

Usage
-----
    python -m pipeline.export                # everything
    python -m pipeline.export --skip-index   # metadata only, for a quick site rebuild
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from . import fx
from .config import BASE_CURRENCY, DOCS_DATA, PROCESSED

# The published index is weekly. Anything finer starts to look like the feed it
# was derived from, and a chart of 15 years does not benefit from daily points.
INDEX_FREQUENCY = "weekly"
INDEX_BASE = 100.0

# Small groups: the repeated query is one fund's series, and a fund holds only
# a few hundred weekly points, so a large group would be mostly over-read.
INDEX_ROW_GROUP = 8_192

# The screener's tables are small enough that a single row group per file is the
# right call -- one column, one byte range, one request.
META_ROW_GROUP = 1_000_000

# A quote whose euro rate is older than this is treated as absent rather than
# merely stale, matching the rule `pipeline.fx` applies to daily bars.
MAX_RATE_AGE_DAYS = 10

# Set by scripts that fill data/processed with a development fixture. The site
# renders an unmissable banner when it is present, so a demo build can never be
# mistaken for the real dataset.
SYNTHETIC_MARKER = "_SYNTHETIC"

# Columns that must never reach out_dir, asserted after every write.
FORBIDDEN_COLUMNS = frozenset({"open", "high", "low", "close", "volume", "adj_close"})


def _relation(con: duckdb.DuckDBPyConnection, table: str) -> str:
    """SQL reference to a processed table, whether it is one file or a directory.

    `prices` is written as a partitioned directory (see .gitignore); the smaller
    tables are single files. Both shapes are read the same way here so this
    module does not have to care which producer chose which.
    """
    single = PROCESSED / f"{table}.parquet"
    if single.exists():
        return f"read_parquet('{single.as_posix()}')"
    directory = PROCESSED / table
    if directory.is_dir() and any(directory.glob("**/*.parquet")):
        return f"read_parquet('{directory.as_posix()}/**/*.parquet', hive_partitioning=false)"
    raise FileNotFoundError(
        f"no processed table '{table}': expected {single} or {directory}/*.parquet"
    )


def _optional(con: duckdb.DuckDBPyConnection, table: str) -> str | None:
    try:
        return _relation(con, table)
    except FileNotFoundError:
        return None


def _write(con: duckdb.DuckDBPyConnection, sql: str, out: Path, row_group: int, level: int = 9) -> int:
    table = con.sql(sql).fetch_arrow_table()
    leaked = FORBIDDEN_COLUMNS.intersection(table.column_names)
    if leaked:
        # A guard rather than a comment: the rule that raw quotes are not
        # redistributed is easy to break by adding one convenient column, and
        # the failure would be silent and public.
        raise ValueError(f"refusing to publish raw price columns {sorted(leaked)} to {out.name}")
    pq.write_table(
        table,
        out,
        compression="zstd",
        compression_level=level,
        row_group_size=row_group,
        use_dictionary=True,
        write_statistics=True,
    )
    return table.num_rows


def export_funds(con: duckdb.DuckDBPyConnection, out_dir: Path) -> int:
    """The screener's single source: funds ⋈ performance ⋈ primary listing."""
    funds, perf = _relation(con, "funds"), _relation(con, "performance")
    listings = _relation(con, "listings")

    # `search_blob` exists so free-text search reads one column instead of five.
    # It is lowercased at build time because a LIKE over a 13k-row column is the
    # single most expensive thing the screener does, and doing the fold here
    # makes it a plain LIKE in the browser rather than an ILIKE.
    sql = f"""
    WITH primary_listing AS (
        SELECT isin,
               first(ticker           ORDER BY is_primary DESC NULLS LAST, exchange_mic) AS primary_ticker,
               first(exchange_mic     ORDER BY is_primary DESC NULLS LAST, exchange_mic) AS primary_mic,
               first(exchange_name    ORDER BY is_primary DESC NULLS LAST, exchange_mic) AS primary_exchange,
               first(trading_currency ORDER BY is_primary DESC NULLS LAST, exchange_mic) AS primary_currency,
               count(*) AS n_listings,
               string_agg(DISTINCT ticker, ' ') AS all_tickers
        FROM {listings} GROUP BY isin
    )
    SELECT
        f.isin, f.name, f.short_name, f.issuer, f.brand, f.domicile, f.ucits,
        f.fund_currency, f.inception_date, f.ter, f.aum_eur, f.replication,
        f.distribution_policy, f.dividend_frequency, f.index_name, f.index_provider,
        f.asset_class, f.region, f.sector, f.strategy, f.leverage, f.esg,
        f.currency_hedged_to, f.securities_lending,
        f.pea_eligible, f.pea_confidence, f.pea_mechanism, f.pea_source, f.pea_as_of,
        f.cto_accessible, f.cto_reason, f.cto_note, f.has_priips_kid, f.authorised_fr,
        f.data_sources, f.last_updated,
        l.primary_ticker, l.primary_mic, l.primary_exchange, l.primary_currency,
        coalesce(l.n_listings, 0)::SMALLINT AS n_listings,
        p.ret_1d, p.ret_1w, p.ret_1m, p.ret_3m, p.ret_6m, p.ret_ytd,
        p.ret_1y, p.ret_3y, p.ret_5y, p.ret_10y, p.ret_max,
        p.cagr_3y, p.cagr_5y, p.cagr_10y, p.cagr_inception,
        p.vol_1y, p.vol_3y, p.vol_5y,
        p.sharpe_1y, p.sharpe_3y, p.sharpe_5y, p.sortino_3y,
        p.max_drawdown_1y, p.max_drawdown_3y, p.max_drawdown_5y, p.max_drawdown_max,
        p.current_drawdown, p.best_month, p.worst_month, p.positive_months_pct,
        p.beta_vs_world, p.correlation_vs_world,
        -- Single derived numbers for display. The series behind them is not
        -- published, and one level cannot reconstitute a feed.
        p.price_last, p.price_date, p.ath, p.ath_date, p.distance_from_ath,
        p.history_start, p.history_days,
        (i.isin IS NOT NULL) AS has_index,
        i.index_points, i.index_start,
        lower(concat_ws(' ', f.name, f.isin, f.issuer, f.brand, f.index_name,
                        f.index_provider, l.all_tickers)) AS search_blob
    FROM {funds} f
    LEFT JOIN primary_listing l USING (isin)
    LEFT JOIN {perf} p USING (isin)
    LEFT JOIN _index_coverage i USING (isin)
    ORDER BY f.aum_eur DESC NULLS LAST
    """
    return _write(con, sql, out_dir / "funds.parquet", META_ROW_GROUP)


def build_index(con: duckdb.DuckDBPyConnection) -> int:
    """Derive the weekly EUR total-return index into the temp table `_tr_index`.

    Three transformations, in this order, none of them reversible into a quote:

    1. **Sample to weekly.** The last bar of each ISO week per fund.
    2. **Convert to EUR.** An as-of join onto the ECB reference rates, because a
       fund that gained 10% in dollars while the dollar lost 10% gained nothing
       for a euro-based holder. Rows whose currency has no rate within
       `MAX_RATE_AGE_DAYS` are dropped rather than converted at a stale price.
    3. **Rebase to 100** at each fund's first surviving observation, so every
       published series starts at the same place and a comparison chart needs no
       client-side normalisation.

    `adj_close` is the input because it is the pipeline's own reconstructed
    total-return series, rebuilt from raw bars and the corporate-actions table --
    never the upstream vendor's adjusted column.
    """
    prices = _relation(con, "prices")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _weekly AS
        SELECT isin, date, adj_close, currency FROM (
            SELECT isin, date, adj_close, coalesce(currency, '{BASE_CURRENCY}') AS currency,
                   row_number() OVER (PARTITION BY isin, date_trunc('week', date) ORDER BY date DESC) AS rn
            FROM {prices}
            WHERE adj_close IS NOT NULL AND adj_close > 0
        ) WHERE rn = 1
    """)

    # Rates are fetched only when something actually needs converting, so a
    # euro-only universe builds offline and a missing FX cache fails loudly for
    # the funds that do need it rather than for everyone.
    foreign = con.sql(f"SELECT count(*) FROM _weekly WHERE currency <> '{BASE_CURRENCY}'").fetchone()[0]
    if foreign:
        con.register("_fx_rates", fx.load_rates())
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _rates AS "
            "SELECT date::DATE AS date, currency, rate_to_eur FROM _fx_rates WHERE rate_to_eur > 0"
        )
    else:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _rates AS "
            "SELECT NULL::DATE AS date, NULL::VARCHAR AS currency, NULL::DOUBLE AS rate_to_eur WHERE false"
        )

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _eur AS
        SELECT w.isin, w.date,
               CASE WHEN w.currency = '{BASE_CURRENCY}' THEN w.adj_close
                    ELSE w.adj_close / r.rate_to_eur END AS eur
        FROM _weekly w
        ASOF LEFT JOIN _rates r ON r.currency = w.currency AND w.date >= r.date
        WHERE w.currency = '{BASE_CURRENCY}'
           OR (r.rate_to_eur IS NOT NULL AND datediff('day', r.date, w.date) <= {MAX_RATE_AGE_DAYS})
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _tr_index AS
        SELECT isin, date,
               ({INDEX_BASE} * eur / first_value(eur) OVER (
                    PARTITION BY isin ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))::FLOAT AS tr_index
        FROM _eur
        ORDER BY isin, date
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _index_coverage AS
        SELECT isin, count(*)::INTEGER AS index_points, min(date) AS index_start
        FROM _tr_index GROUP BY isin
    """)
    return con.sql("SELECT count(*) FROM _tr_index").fetchone()[0]


def export_index(con: duckdb.DuckDBPyConnection, out_dir: Path) -> int:
    return _write(
        con,
        "SELECT isin, date, tr_index FROM _tr_index ORDER BY isin, date",
        out_dir / "tr_index.parquet",
        INDEX_ROW_GROUP,
    )


def export(out_dir: Path = DOCS_DATA, skip_index: bool = False) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    # A fund with no usable history gets has_index = false and the site says so
    # instead of firing a request that comes back empty. Missing prices are
    # survivable: the rest of the site is worth publishing while ingestion is
    # still being built.
    prices = _optional(con, "prices")
    if prices is None:
        skip_index = True
    if skip_index:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _index_coverage AS "
            "SELECT NULL::VARCHAR AS isin, NULL::INTEGER AS index_points, NULL::DATE AS index_start WHERE false"
        )
        index_rows = None
    else:
        index_rows = build_index(con)

    # Any earlier daily-bar export is removed, not left behind: a stale
    # docs/data/prices/ directory would still be served by Pages.
    legacy = out_dir / "prices"
    if legacy.exists():
        shutil.rmtree(legacy)

    counts = {"funds": export_funds(con, out_dir)}
    for table, order in (
        ("listings", "isin, is_primary DESC, exchange_mic"),
        ("returns_yearly", "isin, year"),
        ("returns_monthly", "isin, year, month"),
        ("broker_availability", "isin, broker"),
    ):
        relation = _optional(con, table)
        counts[table] = 0 if relation is None else _write(
            con, f"SELECT * FROM {relation} ORDER BY {order}", out_dir / f"{table}.parquet", META_ROW_GROUP
        )
    if index_rows is None:
        # A skipped rebuild leaves the previous index file in place, so the
        # manifest keeps reporting what is actually being served rather than
        # claiming there is no history.
        previous = out_dir / "manifest.json"
        counts["tr_index"] = (
            json.loads(previous.read_text(encoding="utf-8")).get("counts", {}).get("tr_index")
            if previous.exists() and (out_dir / "tr_index.parquet").exists() else None
        )
    else:
        counts["tr_index"] = export_index(con, out_dir)

    as_of = con.sql(f"SELECT max(last_updated) FROM {_relation(con, 'funds')}").fetchone()[0]
    manifest = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "as_of": as_of.isoformat() if as_of else None,
        "synthetic": (PROCESSED / SYNTHETIC_MARKER).exists(),
        "counts": counts,
        # The site reads price history only through this. Repoint `file` at a
        # release download URL to move it out of the repository without a
        # frontend change.
        "tr_index": {
            "file": "tr_index.parquet",
            "frequency": INDEX_FREQUENCY,
            "base": INDEX_BASE,
            "currency": BASE_CURRENCY,
            "note": "Indice de performance totale reconstruit, base 100 à la première observation.",
        },
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    con.close()
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=DOCS_DATA)
    parser.add_argument("--skip-index", action="store_true",
                        help="rebuild only the metadata tables (seconds instead of minutes)")
    args = parser.parse_args()

    manifest = export(args.out, skip_index=args.skip_index)
    for name, n in manifest["counts"].items():
        print(f"  {name:22s} {'skipped' if n is None else f'{n:>12,} rows'}")
    files = sorted(args.out.rglob("*.parquet"), key=lambda p: -p.stat().st_size)
    for p in files:
        print(f"  {p.name:22s} {p.stat().st_size/1e6:>9.2f} MB")
    total = sum(p.stat().st_size for p in args.out.rglob("*"))
    print(f"  published {total/1e6:.1f} MB total, largest file {files[0].stat().st_size/1e6:.1f} MB")
    if manifest["synthetic"]:
        print("  WARNING: built from a synthetic fixture (data/processed/_SYNTHETIC present)")


if __name__ == "__main__":
    main()
