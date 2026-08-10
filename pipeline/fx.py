"""Currency conversion: every published figure in this database is in EUR.

Why this module exists
----------------------
The universe quotes in roughly twenty-five currencies across three continents,
but a French holder only ever experiences one of them. A fund that gained 10% in
USD while the dollar lost 10% against the euro gained nothing for that holder,
so every price series is converted to EUR *before* a single return is computed.
Doing it here, once, keeps the stats engine currency-blind.

Source
------
Primary is the ECB's euro foreign exchange reference rates -- the full history,
taken from the ZIP mirror:

    https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip

Authoritative, free, key-less, and euro-based by construction: no triangulation
through a third currency and no vendor's private idea of a mid rate.

The plain-CSV mirror of the same file (.../eurofxref-hist.csv) is deliberately
NOT used. When this module was written that endpoint served, reproducibly, a
stale copy: last observation 2010-02-10 (sixteen years behind the ZIP), no ILS
column, and two leading rows whose "rates" were the sequence 1, 2, 3, ... -- one
of them dated on a Sunday, which the ECB never publishes because reference rates
exist only for TARGET business days. The ZIP of the nominally identical file,
same host, same fetch, was complete and current. That is a bad enough failure
mode to justify both the source choice and `_validate`, which enforces the
structural invariants that caught it.

Fallback is frankfurter.app, an ECB wrapper serving the identical numbers as
JSON. It is the fallback rather than the primary precisely because it is a third
party re-publishing data we can get first-hand.

Direction of the quote
----------------------
The most dangerous line in this file is the division below. ECB rates are *units
of foreign currency per one euro*: on 1999-01-04 the USD rate was 1.1789,
meaning one euro bought 1.1789 dollars. A price quoted in USD is therefore
DIVIDED by that rate to become euros. Inverting it yields figures that look
entirely plausible -- right order of magnitude, sensible-looking chart -- while
being wrong for every fund in the database. `tests/test_fx.py` pins the
direction against published historical rates for exactly that reason.

Aligning two different calendars
--------------------------------
The ECB publishes on TARGET business days; exchanges trade on their own
calendars. Tokyo trades on Easter Monday, London does not; the ECB publishes on
neither. So a fund's bars routinely fall on days the FX table has no row for.
Each bar takes the last rate published on or before it (`merge_asof`, backward)
-- the rate a holder could actually have transacted at. Never interpolated,
because interpolation between a Thursday and the following Tuesday leaks
Tuesday's information into Friday, and this data feeds backtest-shaped numbers.

Two cases deliberately do not forward-fill:

* Dates before a currency's first published rate yield NaN, not the earliest
  rate back-projected. The ECB added CNY in 2005, MXN and BRL in 2008, INR in
  2009, ILS in 2011; back-projecting 2011's shekel onto a 2005 bar would invent
  history.
* A rate more than MAX_STALE_DAYS old is treated as absent rather than carried
  forward indefinitely. The largest legitimate gap in the ECB series is five
  calendar days (Easter and Christmas), so this only ever bites when a currency
  stops being published -- RUB was suspended on 2022-03-01, and dragging that
  quote across the following years would be fiction, not a stale quote.

Currencies that joined the euro are the one exception to staleness: their final
rate is a statutory irrevocable conversion rate that is still legally exact
today, so EURO_LEGACY continues them forward forever.
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Mapping

import numpy as np
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import BASE_CURRENCY, HTTP_RETRIES, HTTP_TIMEOUT, RAW, USER_AGENT

log = logging.getLogger(__name__)

ECB_HISTORY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
FRANKFURTER_URL = "https://api.frankfurter.app/{start}..{end}?from=EUR"

# The euro reference rate series begins the first trading day of the euro.
HISTORY_START = date(1999, 1, 4)

CACHE_PATH = RAW / "fx_rates_eur.parquet"
CACHE_META_PATH = RAW / "fx_rates_eur.json"

# Above the largest real publication gap (5 days, Easter/Christmas) and far
# below the multi-year silence that follows a currency being dropped.
MAX_STALE_DAYS = 10

RATES_COLUMNS = ["date", "currency", "rate_to_eur"]


class UnsupportedCurrencyError(KeyError):
    """Raised when no euro rate exists for a currency at all.

    Distinct from "no rate on this date": a fund quoted in TWD cannot be
    converted at any point in its life, and the pipeline must say so rather than
    write a column of nulls that reads as a data outage.
    """


# --------------------------------------------------------------------------- #
# Sub-unit quotes
#
# The LSE quotes many ETFs in pence, and Yahoo reports that currency as "GBp" --
# differing from "GBP" by one character's case, for a factor of 100. Left
# unhandled it makes a fund look 99% down against its own twin. Johannesburg
# (ZAc, cents) and Tel Aviv (ILA, agorot) have the same trap.
#
# Lookup is case-SENSITIVE for GBp, because upper-casing it silently turns pence
# into pounds. The upper-case aliases below (GBX, ZAC, ILA) collide with no ISO
# 4217 code, so those may be matched case-insensitively.
# --------------------------------------------------------------------------- #

_SUBUNITS_EXACT: dict[str, tuple[str, float]] = {
    "GBp": ("GBP", 0.01),
    "ZAc": ("ZAR", 0.01),
    "ILa": ("ILS", 0.01),
}

_SUBUNITS_ANYCASE: dict[str, tuple[str, float]] = {
    "GBX": ("GBP", 0.01),
    "ZAC": ("ZAR", 0.01),
    "ILA": ("ILS", 0.01),
}


# --------------------------------------------------------------------------- #
# Euro legacy
#
# When a currency joins the euro the ECB stops publishing it, but the conversion
# does not become unknown -- it becomes a fixed rate set by Council regulation
# and legally exact in perpetuity. A 2005 bar quoted in SKK converts at the
# published market rate; a 2010 one converts at 30.1260 forever.
#
# Rates are in the same direction as the ECB file: units of the old currency per
# one euro. The 1999 founders never appear in the reference series (it starts
# after their entry), so they exist here only so that an old FRF- or DEM-quoted
# listing resolves instead of raising.
# --------------------------------------------------------------------------- #

EURO_LEGACY: dict[str, tuple[date, float]] = {
    "ATS": (date(1999, 1, 1), 13.7603),
    "BEF": (date(1999, 1, 1), 40.3399),
    "DEM": (date(1999, 1, 1), 1.95583),
    "ESP": (date(1999, 1, 1), 166.386),
    "FIM": (date(1999, 1, 1), 5.94573),
    "FRF": (date(1999, 1, 1), 6.55957),
    "IEP": (date(1999, 1, 1), 0.787564),
    "ITL": (date(1999, 1, 1), 1936.27),
    "LUF": (date(1999, 1, 1), 40.3399),
    "NLG": (date(1999, 1, 1), 2.20371),
    "PTE": (date(1999, 1, 1), 200.482),
    "GRD": (date(2001, 1, 1), 340.750),
    "SIT": (date(2007, 1, 1), 239.640),
    "CYP": (date(2008, 1, 1), 0.585274),
    "MTL": (date(2008, 1, 1), 0.429300),
    "SKK": (date(2009, 1, 1), 30.1260),
    "EEK": (date(2011, 1, 1), 15.6466),
    "LVL": (date(2014, 1, 1), 0.702804),
    "LTL": (date(2015, 1, 1), 3.45280),
    "HRK": (date(2023, 1, 1), 7.53450),
    "BGN": (date(2026, 1, 1), 1.95583),
}


# --------------------------------------------------------------------------- #
# Fetching
# --------------------------------------------------------------------------- #


@retry(
    stop=stop_after_attempt(HTTP_RETRIES),
    wait=wait_exponential(multiplier=1, max=20),
    reraise=True,
)
def _get(url: str) -> bytes:
    response = requests.get(
        url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT}
    )
    response.raise_for_status()
    return response.content


def _tidy(wide: pd.DataFrame) -> pd.DataFrame:
    """Melt a date-by-currency matrix into the tidy (date, currency, rate) form.

    Tidy rather than wide because currencies enter and leave the series at
    different dates; a wide frame carries a rectangle of nulls describing days on
    which a currency simply did not exist yet.
    """
    tidy = wide.melt(
        id_vars="date", var_name="currency", value_name="rate_to_eur"
    ).dropna(subset=["rate_to_eur"])
    # Plain object dtype, not pandas' StringDtype: merge_asof matches the `by`
    # key by dtype as well as value, and a StringDtype table silently fails to
    # join object-dtype price frames.
    tidy["currency"] = tidy["currency"].astype(str).str.strip().str.upper()
    tidy["rate_to_eur"] = pd.to_numeric(tidy["rate_to_eur"], errors="coerce")
    tidy = tidy.dropna(subset=["rate_to_eur"])
    return tidy.sort_values(["currency", "date"], kind="mergesort").reset_index(
        drop=True
    )


def fetch_ecb() -> pd.DataFrame:
    """Primary source: the ECB reference rate history, tidy and validated."""
    payload = _get(ECB_HISTORY_URL)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        member = next(n for n in archive.namelist() if n.lower().endswith(".csv"))
        raw = archive.read(member)

    wide = pd.read_csv(io.BytesIO(raw), na_values=["N/A"], skipinitialspace=True)
    # The file carries a trailing comma on every line, which pandas reads as a
    # nameless all-null column.
    wide = wide.loc[:, ~wide.columns.str.startswith("Unnamed")]
    wide = wide.rename(columns={"Date": "date"})
    wide["date"] = pd.to_datetime(wide["date"])
    return _tidy(wide)


def fetch_frankfurter(start: date = HISTORY_START, end: date | None = None) -> pd.DataFrame:
    """Fallback source: frankfurter.app, an ECB wrapper returning the same rates."""
    end = end or date.today()
    url = FRANKFURTER_URL.format(start=start.isoformat(), end=end.isoformat())
    payload = json.loads(_get(url))

    wide = pd.DataFrame.from_dict(payload["rates"], orient="index")
    wide.index = pd.to_datetime(wide.index)
    wide = wide.rename_axis("date").reset_index()
    return _tidy(wide)


def _validate(rates: pd.DataFrame) -> pd.DataFrame:
    """Reject a rates table that cannot be what the source claims it is.

    These are structural impossibilities, not statistical outliers: the ECB does
    not publish at weekends and does not publish non-positive rates. A corrupt FX
    table is worse than a missing one, because it silently rewrites every
    performance figure downstream instead of failing loudly.
    """
    if rates.empty:
        raise ValueError("FX rate table is empty")

    missing = set(RATES_COLUMNS) - set(rates.columns)
    if missing:
        raise ValueError(f"FX rate table is missing columns: {sorted(missing)}")

    bad = rates[~np.isfinite(rates["rate_to_eur"]) | (rates["rate_to_eur"] <= 0)]
    if not bad.empty:
        raise ValueError(f"FX rate table has {len(bad)} non-positive/non-finite rates")

    weekend = rates[rates["date"].dt.dayofweek >= 5]
    if not weekend.empty:
        days = sorted(weekend["date"].dt.date.unique())[:5]
        raise ValueError(
            f"FX rate table has {len(weekend)} rows on weekends "
            f"(reference rates exist only on TARGET business days): {days}"
        )

    return rates


def _with_euro_legacy(rates: pd.DataFrame) -> pd.DataFrame:
    """Continue euro-joined currencies forward at their irrevocable fixed rate.

    Appended after validation, not before: two entry dates (Estonia 2011-01-01,
    Croatia 2023-01-01) fall at weekends, which is legitimate for a statutory
    rate and illegitimate for a published one.
    """
    pegs = pd.DataFrame(
        [
            {
                "date": pd.Timestamp(entry),
                "currency": currency,
                "rate_to_eur": float(rate),
            }
            for currency, (entry, rate) in EURO_LEGACY.items()
        ]
    )
    # Drop any published observation dated on or after entry so the statutory
    # rate, not a last-day market quote, is what survives from that date on.
    entries = {c: pd.Timestamp(e) for c, (e, _) in EURO_LEGACY.items()}
    cutoff = rates["currency"].map(entries)
    superseded = cutoff.notna() & (rates["date"] >= cutoff)

    combined = pd.concat([rates[~superseded], pegs], ignore_index=True)
    return combined.sort_values(["currency", "date"], kind="mergesort").reset_index(
        drop=True
    )


def load_rates(refresh: bool = False) -> pd.DataFrame:
    """Return the daily euro rate table, fetching only when the cache is absent.

    Columns: `date` (datetime64), `currency` (ISO 4217), `rate_to_eur`.

    `rate_to_eur` follows the ECB convention -- units of `currency` per one euro.
    Divide a price expressed in `currency` by it to obtain euros; do not
    multiply. See the module docstring.
    """
    if not refresh and CACHE_PATH.exists():
        cached = pd.read_parquet(CACHE_PATH)
        cached["date"] = pd.to_datetime(cached["date"])
        return cached[RATES_COLUMNS]

    try:
        rates = _validate(fetch_ecb())
        source = "ecb-eurofxref-hist"
        url = ECB_HISTORY_URL
    except Exception as exc:  # network, parse, or an integrity failure
        log.warning("ECB reference rates unusable (%s); falling back", exc)
        rates = _validate(fetch_frankfurter())
        source = "frankfurter.app"
        url = FRANKFURTER_URL

    rates = _with_euro_legacy(rates)[RATES_COLUMNS]

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    rates.to_parquet(CACHE_PATH, index=False)
    CACHE_META_PATH.write_text(
        json.dumps(
            {
                "source": source,
                "url": url,
                "fetched_at": date.today().isoformat(),
                "rows": int(len(rates)),
                "first_date": rates["date"].min().date().isoformat(),
                "last_date": rates["date"].max().date().isoformat(),
                "currencies": sorted(rates["currency"].unique()),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return rates


def supported_currencies(rates: pd.DataFrame | None = None) -> set[str]:
    """ISO codes convertible to EUR, including EUR itself."""
    rates = load_rates() if rates is None else rates
    return set(rates["currency"].unique()) | {BASE_CURRENCY}


def normalise_currency(code: str) -> tuple[str, float]:
    """Resolve a quote currency to (ISO code, multiplier into the major unit).

    ``normalise_currency("GBp") == ("GBP", 0.01)`` -- pence are hundredths of a
    pound, and the caller must apply the multiplier before dividing by the rate.
    """
    if not code or not isinstance(code, str):
        raise UnsupportedCurrencyError(f"missing quote currency: {code!r}")

    stripped = code.strip()
    if stripped in _SUBUNITS_EXACT:
        return _SUBUNITS_EXACT[stripped]
    if stripped.upper() in _SUBUNITS_ANYCASE:
        return _SUBUNITS_ANYCASE[stripped.upper()]
    return stripped.upper(), 1.0


# --------------------------------------------------------------------------- #
# Conversion
# --------------------------------------------------------------------------- #


def _as_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def attach_rates(bars: pd.DataFrame, rates: pd.DataFrame | None = None) -> pd.DataFrame:
    """Attach the applicable euro rate to every bar of a long price frame.

    `bars` needs `date` and `currency` (already normalised to an ISO code). The
    result adds `rate_to_eur`, NaN where no rate applies. This is the single
    place the two calendars are reconciled, and it is one vectorised pass over
    the whole universe rather than a merge per fund.
    """
    rates = load_rates() if rates is None else rates

    left = bars.copy()
    left["date"] = _as_datetime(left["date"])
    left["currency"] = left["currency"].astype(str)
    left = left.sort_values("date", kind="mergesort")

    right = rates.copy()
    right["date"] = _as_datetime(right["date"])
    right["currency"] = right["currency"].astype(str)
    right["rate_date"] = right["date"]
    right = right.sort_values("date", kind="mergesort")

    merged = pd.merge_asof(
        left,
        right[["date", "rate_date", "currency", "rate_to_eur"]],
        on="date",
        by="currency",
        direction="backward",  # last rate published on or before the bar
    )

    # A quote that stopped being published is absent, not merely old -- except
    # for euro joiners, whose final rate is fixed by law and never goes stale.
    age = (merged["date"] - merged["rate_date"]).dt.days
    stale = (age > MAX_STALE_DAYS) & ~merged["currency"].isin(EURO_LEGACY)
    merged.loc[stale, "rate_to_eur"] = np.nan

    return merged.drop(columns=["rate_date"])


def to_eur(
    prices: pd.DataFrame, currency: str, rates: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Convert one fund's daily bars (PRICES schema) into EUR.

    Returns the same columns with `currency` set to EUR and `close`/`adj_close`
    restated. `volume` is a share count, not an amount of money, so it is left
    alone.

    Bars whose date precedes the currency's first published rate keep their row
    and carry NaN prices: the fund traded that day, we just cannot say what it
    was worth in euros, and inventing a rate would be worse than admitting it.

    Raises UnsupportedCurrencyError if the currency has no euro rate at all.
    """
    iso, unit = normalise_currency(currency)

    if iso == BASE_CURRENCY:
        return _passthrough(prices)

    rates = load_rates() if rates is None else rates
    if iso not in set(rates["currency"].unique()):
        raise UnsupportedCurrencyError(
            f"no EUR reference rate for {currency!r} (resolved to {iso!r})"
        )

    bars = prices.copy()
    bars["currency"] = iso
    merged = attach_rates(bars, rates)

    # The direction that matters: ECB quotes foreign units per euro, so a price
    # in `iso` is divided, never multiplied. `unit` folds pence into pounds first.
    for column in ("close", "adj_close"):
        if column in merged.columns:
            merged[column] = merged[column] * unit / merged["rate_to_eur"]

    out = merged.reindex(columns=list(prices.columns))
    out["currency"] = BASE_CURRENCY
    out["date"] = _restore_dates(out["date"], prices["date"])
    return out.reset_index(drop=True)


def _passthrough(prices: pd.DataFrame) -> pd.DataFrame:
    """An already-euro series is returned as-is, only stamped with the base code.

    Deliberately does not go through the rate table: a euro price needs no rate,
    so it must not inherit the rate table's calendar or its 1999 start.
    """
    out = prices.copy()
    out["currency"] = BASE_CURRENCY
    return out


def _restore_dates(converted: pd.Series, original: pd.Series) -> pd.Series:
    """Hand back dates in the dtype the caller supplied (date objects or datetimes)."""
    if original.dtype == object:
        return converted.dt.date
    return converted


@dataclass(frozen=True)
class ConversionReport:
    """Outcome of converting the whole universe.

    `unsupported` and `bars_without_rate` exist so the pipeline can report gaps
    instead of publishing a fund whose history quietly starts late.
    """

    prices: dict[str, pd.DataFrame]
    unsupported: dict[str, str]
    bars_without_rate: dict[str, int]


def convert_all(
    prices_by_isin: Mapping[str, pd.DataFrame],
    currency_by_isin: Mapping[str, str],
    rates: pd.DataFrame | None = None,
) -> ConversionReport:
    """Convert every fund's bars to EUR in one pass over the universe.

    The whole universe is concatenated and joined against the rate table once.
    Thirteen thousand per-fund merges against a 200k-row table is the difference
    between seconds and minutes, and the alignment rules are identical for every
    fund anyway.
    """
    rates = load_rates() if rates is None else rates
    available = set(rates["currency"].unique()) | {BASE_CURRENCY}

    converted: dict[str, pd.DataFrame] = {}
    unsupported: dict[str, str] = {}
    frames: list[pd.DataFrame] = []
    units: dict[str, float] = {}
    date_dtypes: dict[str, np.dtype] = {}

    for isin, bars in prices_by_isin.items():
        quoted = currency_by_isin.get(isin)
        try:
            iso, unit = normalise_currency(quoted)
        except UnsupportedCurrencyError:
            unsupported[isin] = str(quoted)
            continue

        if iso not in available:
            unsupported[isin] = str(quoted)
            continue

        # Euro funds skip the join entirely, so that `convert_all` and `to_eur`
        # agree bar for bar rather than the batch path quietly imposing the rate
        # table's calendar on a series that never needed a rate.
        if iso == BASE_CURRENCY:
            converted[isin] = _passthrough(bars)
            continue

        frame = bars.copy()
        frame["isin"] = isin
        frame["currency"] = iso
        units[isin] = unit
        date_dtypes[isin] = bars["date"].dtype
        frames.append(frame)

    if not frames:
        return ConversionReport(converted, unsupported, {})

    universe = pd.concat(frames, ignore_index=True)
    merged = attach_rates(universe, rates)
    merged["_unit"] = merged["isin"].map(units)

    for column in ("close", "adj_close"):
        if column in merged.columns:
            merged[column] = merged[column] * merged["_unit"] / merged["rate_to_eur"]

    bars_without_rate: dict[str, int] = {}
    price_columns = [c for c in ("close", "adj_close") if c in merged.columns]

    for isin, group in merged.groupby("isin", sort=False):
        template = prices_by_isin[isin]
        out = group.reindex(columns=list(template.columns))
        out["currency"] = BASE_CURRENCY
        if date_dtypes[isin] == object:
            out["date"] = out["date"].dt.date
        converted[isin] = out.sort_values("date", kind="mergesort").reset_index(
            drop=True
        )

        missing = int(group["rate_to_eur"].isna().sum()) if price_columns else 0
        if missing:
            bars_without_rate[isin] = missing

    return ConversionReport(converted, unsupported, bars_without_rate)


def coverage(currencies: Iterable[str], rates: pd.DataFrame | None = None) -> pd.DataFrame:
    """Report, per currency, whether a euro rate exists and from when.

    Used to answer "which of the universe's quote currencies can we actually
    publish?" before a run, rather than discovering it fund by fund.
    """
    rates = load_rates() if rates is None else rates
    spans = rates.groupby("currency")["date"].agg(["min", "max", "count"])

    rows = []
    for code in dict.fromkeys(currencies):
        iso, unit = normalise_currency(code)
        if iso == BASE_CURRENCY:
            rows.append((code, iso, unit, True, None, None, 0))
        elif iso in spans.index:
            span = spans.loc[iso]
            rows.append(
                (
                    code,
                    iso,
                    unit,
                    True,
                    span["min"].date(),
                    span["max"].date(),
                    int(span["count"]),
                )
            )
        else:
            rows.append((code, iso, unit, False, None, None, 0))

    return pd.DataFrame(
        rows,
        columns=[
            "quoted_as",
            "currency",
            "unit",
            "supported",
            "first_rate",
            "last_rate",
            "observations",
        ],
    )
