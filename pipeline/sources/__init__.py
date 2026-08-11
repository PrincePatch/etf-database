"""Source adapters: one module per upstream provider.

Every adapter converts one upstream into the same two frames, so the merge stage
never learns anything about where a row came from. Adding a venue means adding a
module here and one line in the registry -- nothing downstream changes.

The contract
------------
Each module exposes::

    NAME: str             # short id, also used in funds.data_sources
    TRUST: int            # merge precedence, see below
    def fetch(refresh: bool = False) -> SourceResult

`SourceResult.funds` conforms to schema.FUNDS and `SourceResult.listings` to
schema.LISTINGS. Columns an upstream does not carry are left null -- adapters
never guess. A source that knows only ISINs still returns a funds frame with
every other column null, and the merge fills it from a better-informed source.

Trust levels drive conflict resolution when two sources disagree about the same
field of the same ISIN. Higher wins.

    100  regulator filings (ESMA FIRDS, SEC) -- authoritative on identity
     80  the listing venue itself (Euronext, Xetra, LSE, SIX)
     60  issuer-published data
     40  aggregators and third-party enrichment (OpenFIGI, Yahoo)
     20  heuristics derived from names

Fail-soft is mandatory. Most of these endpoints are undocumented: the Xetra URL
carries a rotating content hash, the LSE workbook filename increments monthly,
Yahoo rotates a crumb token. A dead source must degrade the dataset, not abort
the build -- so `fetch` raises only on programming errors, and reports upstream
failure through `SourceResult.ok = False` with a populated `error`.

Politeness is not optional either. These are public services and this pipeline
is not entitled to hammer them: identify via config.USER_AGENT, cache
aggressively into data/raw/, and rate-limit.

On terms of use, the project draws its line between *using* data and
*republishing* it, and applies that line consistently:

  - A source with an explicit contractual ban on automated access is not used
    at all. justETF is the case in point -- beyond its robots.txt, its terms
    prohibit "programs to carry out automated price inquiries" -- so no adapter
    for it exists, however convenient its coverage would be.
  - A source whose robots.txt disallows crawling, but which imposes no such
    contractual ban, may be read to compute with; its data is never
    redistributed. Yahoo is the case in point, and pipeline/export.py enforces
    the boundary: derived statistics and our own rebased total-return index are
    published, raw OHLCV never is.

The tempting inconsistency would be to exclude one and quietly use the other
because the second is harder to replace. See the README for the full policy.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class SourceResult:
    """What every adapter returns, whether it succeeded or not."""

    name: str
    funds: pd.DataFrame
    listings: pd.DataFrame
    ok: bool = True
    error: str | None = None
    fetched_at: str | None = None
    # Free-form counters the build log surfaces: row counts, how many rows were
    # dropped and why, how much came from cache.
    stats: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.ok and self.error:
            raise ValueError("a successful SourceResult cannot carry an error")


# Populated by pipeline/universe.py at import time; adapters do not import each
# other, so a broken adapter cannot take its neighbours down with it.
REGISTRY: dict[str, object] = {}
