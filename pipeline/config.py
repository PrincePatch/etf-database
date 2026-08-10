"""Paths, constants and tunables shared by every pipeline stage."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
REFERENCE = ROOT / "reference"
DOCS_DATA = ROOT / "docs" / "data"

for _d in (RAW, PROCESSED, DOCS_DATA):
    _d.mkdir(parents=True, exist_ok=True)

# Everything is expressed in the currency the French holder actually spends.
BASE_CURRENCY = "EUR"

# Used for Sharpe and Sortino. Kept as a flat assumption rather than a live
# curve: a daily-varying risk-free rate changes the ratios far less than the
# choice of window does, and it would make historical figures unstable between
# refreshes for no analytical gain.
RISK_FREE_RATE = 0.025

# 252 exchange sessions is the conventional annualisation factor for daily bars.
TRADING_DAYS_PER_YEAR = 252

# The yardstick every fund's beta and correlation are measured against.
BENCHMARK_ISIN = "IE00B4L5Y983"  # iShares Core MSCI World UCITS ETF (acc)

# A fund with a handful of bars produces arithmetically valid but meaningless
# statistics. Below this, performance rows are written as null rather than
# computed, so the UI can say "not enough history" instead of showing noise.
MIN_HISTORY_DAYS = 30

# Politeness / throttling for the public endpoints the sources hit.
HTTP_TIMEOUT = 30
HTTP_RETRIES = 4
USER_AGENT = (
    "etf-database/0.1 (+https://github.com/PrincePatch/etf-database) "
    "open-data pipeline"
)

# Optional, free-tier key that widens ISIN -> ticker resolution. The pipeline
# degrades to unauthenticated (heavily rate-limited) calls when it is absent.
OPENFIGI_API_KEY = os.environ.get("OPENFIGI_API_KEY")


def processed_path(table: str) -> Path:
    """Location of a processed table, per the names declared in schema.TABLES."""
    return PROCESSED / f"{table}.parquet"
