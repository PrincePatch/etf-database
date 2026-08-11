"""ISO 6166 identifier handling: the gate every ISIN passes before it is stored.

Why this is its own module
--------------------------
An ISIN is the primary key of this database: prices, performance, eligibility and
broker availability all hang off it. A single mistyped character does not produce
a missing row, it produces a *different* row -- one that silently joins to the
wrong fund or to nothing at all. The ISO 6166 check digit exists precisely to
catch that, and it is cheap enough that there is no excuse for inserting an ISIN
without running it.

It earned its keep during the eligibility research: five ISINs read out of French
finance blogs (`FR0014002CG7`, `FR0014002CH5`, `FR0013412345`, ...) failed the
check and were rejected rather than written. Two of them were transposition
typos, one was a placeholder someone had left in an article. Any of the three,
inserted, would have produced a fund that does not exist carrying a
`pea_eligible = true` flag.

What the check digit does and does not prove
-------------------------------------------
The algorithm is Luhn over the alphanumeric expansion (A=10 ... Z=35). It catches
every single-character error and all but one transposition pattern (09 <-> 90).
It proves nothing about existence: `FR0000000000` is checksum-valid and is not a
security. Existence is the universe table's job (ESMA FIRDS); this module only
rules out identifiers that cannot be right.

The country prefix
------------------
The first two characters are the code of the national numbering agency that
allocated the ISIN, not a declared domicile. For collective investment vehicles
the two coincide in practice -- an Irish-domiciled UCITS is numbered IE by the
Irish NNA -- which makes the prefix a usable fallback when a source omits the
domicile. It is a *fallback*, so `country()` returns None for the prefixes that
encode no country at all (XS for international issues, EU for EU institutions,
QS/QT for substitute codes) rather than handing back a two-letter string that
looks like a jurisdiction and is not one.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

import pandas as pd

LENGTH = 12

# Two letters, nine alphanumerics, one check digit. Lower case and separators are
# removed by `normalise` before this is applied.
FORMAT = re.compile(r"\A[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")

_SEPARATORS = re.compile(r"[\s\-._/]+")

# Prefixes allocated by supranational numbering agencies. They are syntactically
# valid ISIN prefixes and are not ISO 3166 country codes, so no domicile may be
# read out of them.
NON_COUNTRY_PREFIXES = frozenset({"XS", "EU", "QS", "QT", "XA", "XB", "XC", "XD", "XF"})


class InvalidIsinError(ValueError):
    """Raised when a string cannot be a valid ISIN.

    Deliberately an exception rather than a silent None: callers that insert into
    the database must be forced to decide what to do with a bad identifier, and
    the default decision is to refuse the row.
    """


def normalise(raw: Any) -> str | None:
    """Upper-case, strip separators, and return None for anything empty.

    Handles the shapes real sources produce: `"ie00b4l5y983"`, `"IE00 B4L5 Y983"`,
    `"IE00-B4L5-Y983"`, a NaN from a partly-filled CSV column. Normalising does
    not validate -- the result may still fail the check digit.
    """
    if raw is None:
        return None
    if isinstance(raw, float) and raw != raw:  # NaN
        return None
    try:
        if raw is pd.NA or pd.isna(raw):
            return None
    except (TypeError, ValueError):
        pass  # arrays and lists land here; they are not ISINs either way

    text = _SEPARATORS.sub("", str(raw)).strip().upper()
    return text or None


def check_digit(body: str) -> str:
    """Compute the ISO 6166 check digit for the first 11 characters of an ISIN.

    Letters expand to their base-36 value (A=10 ... Z=35) and the resulting digit
    string is summed under Luhn, doubling every second digit counted from the
    right of the *complete* ISIN -- which is every second digit counted from the
    right of the body, since the check digit occupies the rightmost position.
    """
    if len(body) != LENGTH - 1:
        raise InvalidIsinError(f"ISIN body must be {LENGTH - 1} characters: {body!r}")

    try:
        digits = "".join(str(int(char, 36)) for char in body)
    except ValueError as exc:
        raise InvalidIsinError(f"ISIN body is not alphanumeric: {body!r}") from exc

    total = 0
    for position, char in enumerate(reversed(digits)):
        value = int(char)
        if position % 2 == 0:
            value *= 2
            if value > 9:
                value -= 9
        total += value

    return str((10 - total % 10) % 10)


def is_valid(raw: Any) -> bool:
    """True when `raw` normalises to a syntactically correct, checksum-valid ISIN."""
    code = normalise(raw)
    if code is None or not FORMAT.match(code):
        return False
    return check_digit(code[:-1]) == code[-1]


def require(raw: Any) -> str:
    """Return the normalised ISIN, or raise InvalidIsinError.

    This is the call sites' contract with invariant C5: nothing reaches a table
    without having passed through here.
    """
    code = normalise(raw)
    if code is None:
        raise InvalidIsinError("missing ISIN")
    if not FORMAT.match(code):
        raise InvalidIsinError(f"malformed ISIN: {code!r}")
    expected = check_digit(code[:-1])
    if expected != code[-1]:
        raise InvalidIsinError(
            f"ISIN {code!r} fails the ISO 6166 check digit "
            f"(expected {expected}, found {code[-1]})"
        )
    return code


def country(raw: Any) -> str | None:
    """The allocating country of a valid ISIN, or None when it encodes none.

    Returns None for invalid input and for the supranational prefixes, so a
    caller can treat "no country" and "not a country" identically instead of
    reading `XS` as a jurisdiction.
    """
    code = normalise(raw)
    if code is None or not is_valid(code):
        return None
    prefix = code[:2]
    return None if prefix in NON_COUNTRY_PREFIXES else prefix


def valid_mask(values: Iterable[Any]) -> pd.Series:
    """Vectorised `is_valid` over a column, for filtering a whole frame at once."""
    return pd.Series([is_valid(value) for value in values], dtype="bool")
