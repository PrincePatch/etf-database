"""Total-return reconstruction: rebuilding `adj_close` from raw bars and events.

`prices.close` is what the venue printed. On the morning a fund goes ex-dividend
that number drops by roughly the distribution, and a holder who lost nothing
sees a loss. Every return in this database is therefore computed from
`adj_close`, the back-adjusted series this module produces, and `close` survives
only as the quote shown next to the fund's name.

Why we recompute rather than fetch
----------------------------------
Because the vendor's adjusted close cannot be trusted, and the failure is
invisible. Yahoo publishes 40 dividend events for ISF.L and applies none of them
to its own `adjclose`: the fund's 10-year annualised return comes back as 4.57%
against a true 8.60%, a 4.03pp/yr error compounding to roughly 48%. The same
fund on Amsterdam and Milan is adjusted correctly, so it is a per-listing defect
with no signature you can detect without redoing the arithmetic. Measured on the
nine listings where the vendor *is* right, the reconstruction below reproduces
its figure to the basis point -- it agrees everywhere the vendor works and fixes
it where it does not.

The second reason is idempotency. Vendors restate the entire adjusted history on
every new distribution, so a stored `adj_close` column silently drifts out of
sync with an incrementally-appended price table, while raw bars plus events stay
append-only and this module is a pure function of them.

The arithmetic
--------------
CRSP-style back-adjustment. An event on ex-date `t` scales every bar *strictly
before* `t`; bars from `t` onward are untouched, so the most recent bar always
satisfies `adj_close == close` and the series is quoted in today's money.

For a cash distribution `D` with ex-date `t`, taking `C` as the close of the
last session before `t`::

    f = (C - D) / C

`C` and not the ex-date close: `f` must express the fraction of value that
*stayed* in the share price, and the ex-date close has already had `D` taken out
of it. For a split of `r` new shares per old::

    f = 1 / r

and an ex-date carrying both composes them, `f = f_split * f_div`, which is
order-independent.

`adj_close[i] = close[i] * prod(f for every event after bar i)`, evaluated for
the whole universe at once as one cumulative sum of logs (see `_adjust`). Log
space rather than a running product because the panel is one 49M-row array
covering 13,000 funds: a global `cumprod` across all of them would underflow to
zero long before the last fund, whereas the cumulative sum telescopes exactly
over each fund's own slice. A fund with no events has every log-factor exactly
`0.0`, so its cumulative sums are bit-identical and `adj_close == close` to the
last bit -- not merely to a tolerance, which is what the accumulating half of
the universe requires.

Splits and who has already applied them
---------------------------------------
`close_is_split_adjusted` is the sharpest edge in this file. Yahoo returns
prices that are *already* split-adjusted -- AAPL's 2020-08-27 close comes back
as 125.01, not the ~500 the tape printed before that week's 4:1 split -- and its
dividend amounts are restated to match (0.1925 for the 2020-02-07 dividend that
actually paid 0.77). Re-applying the split factor to that series would divide
the pre-split history by four a second time. Since the vendor feeding this
pipeline today is Yahoo, the default is `True`, meaning "splits are already in
the close, do not re-apply". A source that prints the tape unretouched -- an
exchange EOD file -- must pass `False`, and then dividend amounts are assumed to
be quoted in the same pre-split units as the close they are divided by, which is
what the exchange prints.

What is deliberately not adjusted
---------------------------------
* An event dated after the fund's last bar. Vendors publish *announced* future
  ex-dates, and applying an unrealised distribution would rebase the entire
  history on cash nobody has received.
* An event at or before the fund's first bar. There is no earlier bar to scale,
  so the event is outside the window by construction.
* Any event whose reference close is missing, zero or negative, or that would
  produce a non-positive factor (a distribution larger than the share price,
  which happens on liquidation). The bar keeps its unadjusted value and the
  event is counted in `AdjustmentReport.skipped`, because a factor of zero or
  less does not merely mis-scale one bar, it annihilates or flips the sign of
  every bar before it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# A property of the vendor, not of the arithmetic: see the module docstring.
# Yahoo -- the only source in this pipeline -- serves split-adjusted closes and
# split-adjusted dividend amounts, so splits must not be applied a second time.
CLOSE_IS_SPLIT_ADJUSTED = True

DIVIDEND = "dividend"
SPLIT = "split"


@dataclass
class AdjustmentReport:
    """Attached to the result as `.attrs["adjustment"]`.

    `skipped` is the number that matters: a silently unapplied dividend is
    exactly the ISF.L bug this module exists to fix, so it is counted rather
    than swallowed.
    """

    funds: int = 0
    bars: int = 0
    events: int = 0
    applied: int = 0
    skipped: dict[str, int] = field(default_factory=dict)

    def _skip(self, reason: str, count: int) -> None:
        if count:
            self.skipped[reason] = self.skipped.get(reason, 0) + int(count)

    @property
    def total_skipped(self) -> int:
        return sum(self.skipped.values())

    def summary(self) -> str:
        return (
            f"{self.funds} funds, {self.bars:,} bars, "
            f"{self.applied}/{self.events} events applied"
            + (f", skipped {self.skipped}" if self.skipped else "")
        )


# --------------------------------------------------------------------------- #
# Panel helpers
#
# The engine is deliberately universe-wide and array-shaped, in the same spirit
# as stats.py: 13,000 pandas groupby-apply calls cost minutes of interpreter
# overhead for arithmetic that is three numpy passes.
# --------------------------------------------------------------------------- #

# Keeps (fund, day) orderable as one int64. Day numbers since the epoch are a
# five-digit number; 2**32 leaves the two fields from ever colliding.
_DAY_STRIDE = 1 << 32


def _days(values: Any) -> np.ndarray:
    """Dates as integer days since the epoch, whatever container they arrive in."""
    stamps = pd.to_datetime(pd.Series(values), errors="coerce")
    return (stamps.to_numpy(dtype="datetime64[ns]").astype("datetime64[D]")
            .astype("int64"))


def _segment_bounds(codes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """First and last index of each fund's contiguous slice.

    `codes` must be non-decreasing, which sorting by (isin, date) guarantees.
    """
    n = codes.size
    if n == 0:
        return np.empty(0, dtype="int64"), np.empty(0, dtype="int64")
    boundary = np.flatnonzero(np.diff(codes)) + 1
    starts = np.concatenate([[0], boundary]).astype("int64")
    ends = np.concatenate([boundary - 1, [n - 1]]).astype("int64")
    return starts, ends


def _last_valid_position(close: np.ndarray, codes: np.ndarray, starts: np.ndarray) -> np.ndarray:
    """For each bar, the index of the most recent usable close at or before it.

    A halted session arrives with a null close and a liquidating fund can print
    zero; either one would make the dividend factor undefined. Walking back to
    the last real quote is a running maximum over candidate indices, and because
    indices increase monotonically across the whole panel a single global
    `maximum.accumulate` is correct once results that leaked in from the
    previous fund are masked out.
    """
    n = close.size
    usable = np.isfinite(close) & (close > 0)
    candidate = np.where(usable, np.arange(n, dtype="int64"), -1)
    running = np.maximum.accumulate(candidate)
    return np.where(running >= starts[codes], running, -1)


def _event_factors(
    events: pd.DataFrame,
    codes_of_key: dict[Any, int],
    bar_keys: np.ndarray,
    close: np.ndarray,
    codes: np.ndarray,
    starts: np.ndarray,
    ends: np.ndarray,
    last_valid: np.ndarray,
    close_is_split_adjusted: bool,
    report: AdjustmentReport,
) -> np.ndarray:
    """Log-factor per bar position, where a factor at `j` scales bars `<= j`."""
    logf = np.zeros(close.size, dtype="float64")
    if events is None or events.empty:
        return logf

    known = events.loc[events["isin"].isin(codes_of_key)]
    report._skip("fund not in price panel", len(events) - len(known))

    frame = known.loc[pd.to_datetime(known["date"], errors="coerce").notna()].copy()
    report._skip("undated event", len(known) - len(frame))
    if frame.empty:
        return logf

    event_codes = frame["isin"].map(codes_of_key).to_numpy(dtype="int64")
    event_days = _days(frame["date"])

    # The last bar strictly before the ex-date, within the same fund. The
    # composite key is sorted by construction, so one searchsorted resolves the
    # whole universe.
    keys = event_codes * _DAY_STRIDE + event_days
    position = np.searchsorted(bar_keys, keys, side="left") - 1

    in_fund = (position >= 0) & (position >= starts[event_codes])
    # An announced-but-unpaid future distribution has no bar it belongs to.
    not_future = event_days <= (bar_keys[ends[event_codes]] - event_codes * _DAY_STRIDE)
    usable = in_fund & not_future
    report._skip("outside the fund's price window", int((~usable).sum()))

    reference_position = np.where(usable, last_valid[np.where(usable, position, 0)], -1)
    has_reference = reference_position >= 0
    report._skip("no usable reference close", int((usable & ~has_reference).sum()))
    usable &= has_reference

    if not usable.any():
        return logf

    reference = np.where(usable, close[np.where(usable, reference_position, 0)], np.nan)
    kind = frame["kind"].astype(str).to_numpy()
    amount = pd.to_numeric(frame.get("amount"), errors="coerce").to_numpy(dtype="float64")
    ratio = pd.to_numeric(frame.get("ratio"), errors="coerce").to_numpy(dtype="float64")

    factor = np.ones(len(frame), dtype="float64")

    is_dividend = usable & (kind == DIVIDEND) & np.isfinite(amount)
    with np.errstate(divide="ignore", invalid="ignore"):
        factor = np.where(
            is_dividend, (reference - amount) / reference, factor
        )

    is_split = usable & (kind == SPLIT) & np.isfinite(ratio) & (ratio > 0)
    if not close_is_split_adjusted:
        factor = np.where(is_split, factor / np.where(is_split, ratio, 1.0), factor)

    recognised = is_dividend | is_split
    report._skip("unrecognised event kind", int((usable & ~recognised).sum()))
    usable &= recognised

    # A factor of zero wipes out every earlier bar and a negative one flips their
    # sign; neither is a price series, so the event is dropped rather than
    # applied. Real cause: a distribution at or above the share price.
    sane = np.isfinite(factor) & (factor > 0)
    report._skip("non-positive adjustment factor", int((usable & ~sane).sum()))
    usable &= sane

    report.applied += int(usable.sum())
    np.add.at(logf, position[usable], np.log(factor[usable]))
    return logf


def _adjust(
    prices: pd.DataFrame,
    actions: pd.DataFrame | None,
    close_is_split_adjusted: bool,
) -> tuple[np.ndarray, AdjustmentReport]:
    """Kernel: adjusted closes for a whole universe, in the caller's row order.

    Everything downstream is positional rather than index-label based, so a
    frame with a duplicated or non-unique index -- which a naive `pd.concat` of
    per-fund blocks produces -- cannot scramble the result.
    """
    report = AdjustmentReport(bars=len(prices))

    origin = np.arange(len(prices), dtype="int64")
    ordered = prices.assign(_origin=origin).sort_values(
        ["isin", "date"], kind="mergesort"
    )
    origin = ordered["_origin"].to_numpy(dtype="int64")

    # factorize, not np.unique: on an already-sorted column it yields codes that
    # are non-decreasing by construction, which is what makes every fund a
    # contiguous slice, and it does not choke on a null key.
    codes, unique = pd.factorize(ordered["isin"], use_na_sentinel=False)
    codes = codes.astype("int64")
    report.funds = len(unique)

    close = pd.to_numeric(ordered["close"], errors="coerce").to_numpy(dtype="float64")
    days = _days(ordered["date"])
    bar_keys = codes * _DAY_STRIDE + days

    starts, ends = _segment_bounds(codes)
    last_valid = _last_valid_position(close, codes, starts)

    report.events = 0 if actions is None else len(actions)
    logf = _event_factors(
        actions,
        {key: index for index, key in enumerate(unique)},
        bar_keys,
        close,
        codes,
        starts,
        ends,
        last_valid,
        close_is_split_adjusted,
        report,
    )

    # adj[i] = close[i] * prod(f[j] for j in [i, end_of_fund]).
    # cumulative[end] - exclusive[i] telescopes over exactly that range, and is
    # bit-exactly 0.0 for a fund that never paid anything.
    cumulative = np.cumsum(logf)
    exclusive = cumulative - logf
    adjusted = close * np.exp(cumulative[ends[codes]] - exclusive)

    restored = np.empty_like(adjusted)
    restored[origin] = adjusted
    return restored, report


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def total_return_series(
    prices: pd.DataFrame,
    actions: pd.DataFrame | None = None,
    *,
    close_is_split_adjusted: bool = CLOSE_IS_SPLIT_ADJUSTED,
) -> pd.Series:
    """Back-adjusted total-return closes for **one** fund.

    `prices` needs `date` and `close`; `actions` needs `date`, `kind` and
    whichever of `amount`/`ratio` the kind uses. An `isin` column is optional
    here and is only used to reject a frame that holds more than one fund --
    silently adjusting two funds as if they were one would splice their
    histories together.

    Returns a float64 Series named `adj_close`, indexed exactly like `prices`,
    so it can be assigned straight back onto the frame regardless of the order
    the bars arrived in.
    """
    if "isin" in prices.columns and prices["isin"].nunique(dropna=False) > 1:
        raise ValueError(
            "total_return_series takes one fund; use apply_adjustment for a universe"
        )

    if prices.empty:
        return pd.Series(dtype="float64", index=prices.index, name="adj_close")

    single = prices.copy()
    single["isin"] = "_"
    events = None
    if actions is not None and not actions.empty:
        events = actions.copy()
        events["isin"] = "_"

    adjusted, report = _adjust(single, events, close_is_split_adjusted)
    series = pd.Series(adjusted, index=prices.index, name="adj_close")
    series.attrs["adjustment"] = report
    return series


def apply_adjustment(
    prices_df: pd.DataFrame,
    actions_df: pd.DataFrame | None = None,
    *,
    close_is_split_adjusted: bool = CLOSE_IS_SPLIT_ADJUSTED,
) -> pd.DataFrame:
    """Fill `adj_close` for the whole universe in one vectorised pass.

    `prices_df` conforms to `schema.PRICES` (an existing `adj_close` is
    recomputed, never trusted); `actions_df` to `schema.CORPORATE_ACTIONS`.
    Events belonging to a fund with no bars are ignored, and funds with no
    events come back with `adj_close` bit-identical to `close`.

    Rows are returned in the caller's original order with `adj_close` filled,
    and an `AdjustmentReport` is attached as `.attrs["adjustment"]`.
    """
    result = prices_df.copy()
    if result.empty:
        result["adj_close"] = pd.Series(dtype="float64")
        result.attrs["adjustment"] = AdjustmentReport()
        return result

    missing = {"isin", "date", "close"} - set(result.columns)
    if missing:
        raise ValueError(f"prices is missing required columns: {sorted(missing)}")

    adjusted, report = _adjust(result, actions_df, close_is_split_adjusted)
    result["adj_close"] = adjusted
    result.attrs["adjustment"] = report

    if report.total_skipped:
        log.info("apply_adjustment: %s", report.summary())
    return result
