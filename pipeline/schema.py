"""Canonical data schema for the ETF database.

Every producer in the pipeline writes tables that conform to the PyArrow schemas
defined here, and every consumer (stats engine, exporters, frontend) reads them.
This module is the single source of truth: if a column is not declared here, it
does not exist.

Design notes
------------
A fund and a listing are deliberately separated. One fund (one ISIN) is very
often listed on several exchanges under different tickers -- IE00B4L5Y983 trades
as EUNL on Xetra, IWDA on Euronext Amsterdam and SWDA on Borsa Italiana. Stats
belong to the fund; tickers, currencies and exchanges belong to the listing.

All returns and ratios are stored as decimal fractions, not percentages: a 12.5%
annual return is 0.125. Formatting is the frontend's job.

Money amounts are stored in EUR. The base currency for performance is EUR too,
which is the relevant frame for a French investor -- a fund quoted in USD that
gained 10% in USD while the dollar lost 10% did not gain 10% for the holder.
"""

from __future__ import annotations

import pyarrow as pa

# --------------------------------------------------------------------------- #
# Controlled vocabularies
#
# Free text in these fields makes the frontend filters unusable, so producers
# must map source values onto these lists and fall back to "unknown" rather than
# inventing a new value.
# --------------------------------------------------------------------------- #

REPLICATION = ["physical-full", "physical-sampling", "synthetic-swap", "unknown"]

DISTRIBUTION_POLICY = ["accumulating", "distributing", "unknown"]

ASSET_CLASS = [
    "equity",
    "bond",
    "commodity",
    "money-market",
    "real-estate",
    "crypto",
    "multi-asset",
    "currency",
    "unknown",
]

STRATEGY = [
    "broad-market",
    "sector",
    "country",
    "factor",
    "thematic",
    "esg",
    "dividend",
    "leveraged",
    "inverse",
    "covered-call",
    "active",
    "unknown",
]

# Tri-state on purpose. `False` means "we established it is not eligible",
# `None` means "we could not establish it" -- collapsing those two would tell the
# user an ETF is barred from their PEA when we simply do not know.
ELIGIBILITY = [True, False, None]

# How good the evidence behind a `pea_eligible = True` is. The three best public
# screeners agree on only 88 of 243 contested ISINs, so publishing a bare boolean
# would present a coin flip and a prospectus reading as the same fact.
PEA_CONFIDENCE = [
    "highest",  # the fund's own prospectus / DIC states PEA eligibility
    "high",  # the listing venue's flag, or the issuer's own screener
    "medium",  # two or more independent brokers list it as PEA
    "low",  # a single broker; queued for human review
    "hint",  # structurally plausible only -- never sufficient for True
    "none",
]

# How the fund qualifies. Worth storing separately because the two mechanisms
# carry different political risk: a standing Sénat question (QE n° 08783, still
# unanswered) targets exactly the synthetic route, so if it ever closes, the
# affected rows must be re-flaggable with one query rather than a re-derivation.
PEA_MECHANISM = ["physical_eu", "synthetic_swap", "unknown"]

CTO_REASON = [
    "ucits_eea_with_kid",  # accessible
    "in_broker_catalogue",  # accessible
    "no_priips_kid",  # US-domiciled 40-Act funds land here
    "uk_ucits_is_third_country",
    "not_passported_to_france",
    "kid_language_unconfirmed",
    "not_in_broker_catalogue",
    "unknown",
]


# --------------------------------------------------------------------------- #
# funds -- one row per ISIN
# --------------------------------------------------------------------------- #

FUNDS = pa.schema(
    [
        pa.field("isin", pa.string(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("short_name", pa.string()),
        pa.field("issuer", pa.string()),
        pa.field("brand", pa.string()),
        pa.field("domicile", pa.string()),  # ISO-3166 alpha-2: IE, LU, FR, US, DE
        pa.field("ucits", pa.bool_()),
        pa.field("fund_currency", pa.string()),  # ISO-4217
        pa.field("inception_date", pa.date32()),
        pa.field("ter", pa.float32()),  # 0.0020 == 0.20% per year
        pa.field("aum_eur", pa.float64()),
        pa.field("replication", pa.string()),
        pa.field("distribution_policy", pa.string()),
        pa.field("dividend_frequency", pa.string()),
        pa.field("index_name", pa.string()),
        pa.field("index_provider", pa.string()),
        pa.field("asset_class", pa.string()),
        pa.field("region", pa.string()),
        pa.field("sector", pa.string()),
        pa.field("strategy", pa.string()),
        pa.field("leverage", pa.float32()),  # 1.0 plain, 2.0 levered, -1.0 inverse
        pa.field("esg", pa.bool_()),
        pa.field("currency_hedged_to", pa.string()),  # null when unhedged
        pa.field("securities_lending", pa.bool_()),
        # Eligibility for the two French wrappers this database is built around.
        #
        # There is no authoritative public register of PEA-eligible funds, and
        # Décret 2026-189 confirms none is coming: the AMF holds a per-fund
        # engagement but publishes nothing. So eligibility is assembled from
        # positive evidence of varying quality, and the quality travels with the
        # answer instead of being flattened away.
        #
        # The asymmetry matters. Holding an ineligible fund in a PEA can force
        # the plan closed (BOI-RPPM-RCM-40-50-50), losing the tax clock; missing
        # an eligible one merely means not buying it. A wrong `true` is
        # therefore far more expensive than a `null`, which is why only the
        # structural disqualifiers in the classifier may return `false` and
        # everything else falls back to null.
        pa.field("pea_eligible", pa.bool_()),
        pa.field("pea_confidence", pa.string()),  # see PEA_CONFIDENCE
        pa.field("pea_mechanism", pa.string()),  # see PEA_MECHANISM
        pa.field("pea_source", pa.string()),  # URL backing the flag
        pa.field("pea_as_of", pa.date32()),  # when that source was read
        pa.field("cto_accessible", pa.bool_()),
        pa.field("cto_reason", pa.string()),  # see CTO_REASON
        pa.field("cto_note", pa.string()),  # free text for the UI
        # The two facts CTO accessibility decomposes into, kept separately
        # because they fail for different reasons and are fixed by different
        # sources.
        pa.field("has_priips_kid", pa.bool_()),
        pa.field("authorised_fr", pa.bool_()),  # AMF marketing passport
        pa.field("data_sources", pa.list_(pa.string())),
        pa.field("last_updated", pa.date32()),
    ]
)


# --------------------------------------------------------------------------- #
# listings -- one row per (ISIN, exchange)
# --------------------------------------------------------------------------- #

LISTINGS = pa.schema(
    [
        pa.field("isin", pa.string(), nullable=False),
        pa.field("exchange_mic", pa.string(), nullable=False),  # XPAR, XETR, XAMS...
        pa.field("exchange_name", pa.string()),
        pa.field("ticker", pa.string()),  # local ticker, e.g. EUNL
        pa.field("yahoo_ticker", pa.string()),  # resolved Yahoo symbol, e.g. EUNL.DE
        pa.field("trading_currency", pa.string()),
        # The listing whose price series we treat as authoritative for the fund.
        pa.field("is_primary", pa.bool_()),
    ]
)


# --------------------------------------------------------------------------- #
# prices -- daily bars, one row per (ISIN, date)
#
# Only the primary listing of each fund is stored. Keeping every listing of every
# fund would multiply the largest table by ~3 for series that are the same asset
# quoted through a different currency lens.
#
# `close` is the raw quote; `adj_close` is adjusted for distributions, so a
# distributing ETF is comparable with its accumulating twin. Every return in the
# performance table is computed from `adj_close`.
# --------------------------------------------------------------------------- #

PRICES = pa.schema(
    [
        pa.field("isin", pa.string(), nullable=False),
        pa.field("date", pa.date32(), nullable=False),
        pa.field("close", pa.float32()),
        pa.field("adj_close", pa.float32()),
        pa.field("volume", pa.float32()),
        pa.field("currency", pa.string()),
    ]
)


# --------------------------------------------------------------------------- #
# performance -- one precomputed row per ISIN
#
# Precomputed rather than derived in the browser: computing a 10-year CAGR for
# 13k funds client-side would mean shipping the whole price history to every
# visitor, which defeats the point of a static site.
#
# Trailing windows are calendar-anchored (1y == same date last year), not
# fixed bar counts, so funds on exchanges with different holiday calendars stay
# comparable. Windows longer than the fund's history are null, never zero.
# --------------------------------------------------------------------------- #

PERFORMANCE = pa.schema(
    [
        pa.field("isin", pa.string(), nullable=False),
        pa.field("base_currency", pa.string()),  # "EUR" for the published dataset
        # Trailing total returns, cumulative over the window.
        pa.field("ret_1d", pa.float32()),
        pa.field("ret_1w", pa.float32()),
        pa.field("ret_1m", pa.float32()),
        pa.field("ret_3m", pa.float32()),
        pa.field("ret_6m", pa.float32()),
        pa.field("ret_ytd", pa.float32()),
        pa.field("ret_1y", pa.float32()),
        pa.field("ret_3y", pa.float32()),
        pa.field("ret_5y", pa.float32()),
        pa.field("ret_10y", pa.float32()),
        pa.field("ret_max", pa.float32()),
        # Annualised returns. ret_3y is cumulative over 3 years, cagr_3y is the
        # per-year rate -- the two are constantly confused and both are useful.
        pa.field("cagr_3y", pa.float32()),
        pa.field("cagr_5y", pa.float32()),
        pa.field("cagr_10y", pa.float32()),
        pa.field("cagr_inception", pa.float32()),
        # Risk. Volatility is annualised stdev of daily log returns.
        pa.field("vol_1y", pa.float32()),
        pa.field("vol_3y", pa.float32()),
        pa.field("vol_5y", pa.float32()),
        pa.field("sharpe_1y", pa.float32()),
        pa.field("sharpe_3y", pa.float32()),
        pa.field("sharpe_5y", pa.float32()),
        pa.field("sortino_3y", pa.float32()),
        pa.field("max_drawdown_1y", pa.float32()),
        pa.field("max_drawdown_3y", pa.float32()),
        pa.field("max_drawdown_5y", pa.float32()),
        pa.field("max_drawdown_max", pa.float32()),
        pa.field("current_drawdown", pa.float32()),
        # Distribution of monthly outcomes -- what a holder actually lives through.
        pa.field("best_month", pa.float32()),
        pa.field("worst_month", pa.float32()),
        pa.field("positive_months_pct", pa.float32()),
        # Versus a common yardstick (MSCI World, in EUR).
        pa.field("beta_vs_world", pa.float32()),
        pa.field("correlation_vs_world", pa.float32()),
        # Price context.
        pa.field("price_last", pa.float32()),
        pa.field("price_date", pa.date32()),
        pa.field("ath", pa.float32()),
        pa.field("ath_date", pa.date32()),
        pa.field("distance_from_ath", pa.float32()),  # negative when below ATH
        # Provenance of the numbers above, so thin histories can be flagged in
        # the UI instead of being silently compared against 20-year track records.
        pa.field("history_start", pa.date32()),
        pa.field("history_days", pa.int32()),
        pa.field("computed_at", pa.date32()),
    ]
)


# --------------------------------------------------------------------------- #
# Calendar-period returns -- what "gains per year / per month" means in the UI
# --------------------------------------------------------------------------- #

RETURNS_YEARLY = pa.schema(
    [
        pa.field("isin", pa.string(), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("ret", pa.float32()),
        # A fund launched in June has a 2019 figure covering half a year; without
        # this flag it would be ranked against full years as if it were one.
        pa.field("partial", pa.bool_()),
    ]
)

RETURNS_MONTHLY = pa.schema(
    [
        pa.field("isin", pa.string(), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
        pa.field("ret", pa.float32()),
        pa.field("partial", pa.bool_()),
    ]
)


# --------------------------------------------------------------------------- #
# broker_availability -- one row per (broker, ISIN)
#
# Separate from funds because catalogue inclusion cannot be derived from any
# property of the fund. DEGIRO and Trade Republic both diverge from plain venue
# reachability in ways only their own catalogues reveal, so "this ETF exists and
# is PEA-eligible" and "you can actually buy it at your broker" are two
# questions, and the second one is per-broker fact, not inference.
# --------------------------------------------------------------------------- #

BROKER_AVAILABILITY = pa.schema(
    [
        pa.field("broker", pa.string(), nullable=False),
        pa.field("isin", pa.string(), nullable=False),
        pa.field("available", pa.bool_()),
        pa.field("wrapper", pa.string()),  # "pea", "cto", or "both"
        pa.field("source_url", pa.string()),
        pa.field("as_of", pa.date32()),
    ]
)


TABLES = {
    "funds": FUNDS,
    "listings": LISTINGS,
    "prices": PRICES,
    "performance": PERFORMANCE,
    "returns_yearly": RETURNS_YEARLY,
    "returns_monthly": RETURNS_MONTHLY,
    "broker_availability": BROKER_AVAILABILITY,
}


def empty(table: str) -> pa.Table:
    """Return an empty table with the declared schema, for producers to append to."""
    return TABLES[table].empty_table()


def conform(table: pa.Table, name: str) -> pa.Table:
    """Reorder, cast and fill a table so it matches its declared schema.

    Producers assemble columns in whatever order is convenient; this makes the
    result byte-compatible with the schema so partitioned writes from different
    producers can be read as one dataset.
    """
    schema = TABLES[name]
    columns = []
    for field in schema:
        if field.name in table.column_names:
            columns.append(table.column(field.name).cast(field.type))
        else:
            columns.append(pa.nulls(table.num_rows, type=field.type))
    return pa.Table.from_arrays(columns, schema=schema)
