"""PEA eligibility and CTO accessibility: the classifier that is not allowed to guess.

Why this module is written defensively
--------------------------------------
The two answers it produces are not symmetric in cost. Holding an ineligible fund
inside a PEA is a ground for **closure of the plan** (BOI-RPPM-RCM-40-50-50): the
holder loses the five-year tax clock and the whole wrapper, not just the trade.
Failing to flag an eligible fund costs a missed purchase. So a wrong `True` is
catastrophic and a `None` is merely unhelpful, and every rule below is bent in
that direction:

* `True` requires **positive evidence**, ranked and attributed. Never a guess.
* `False` is reserved for the structural disqualifiers of Rule 0 -- things that
  cannot be an OPCVM established in the EEA. Nothing else may say "no".
* Everything else is `None`. Absence from a broker's catalogue means that broker
  does not offer it; it does not mean the fund is ineligible.

The mistake this module exists to prevent
-----------------------------------------
Eligibility follows from the fund's *balance sheet*, not from what it tracks.
Article L221-31 I-2° C. mon. fin. requires that the fund permanently employ more
than 75% of its assets in EU/EEA-headquartered equities; it says nothing about
the benchmark. A synthetic ETF holds a substitute basket of European shares and
swaps its performance for the target index, so:

    FR0011871128  Amundi PEA S&P 500          synthetic  -> PEA-eligible
    IE000DQLYVB9  iShares S&P 500 Swap PEA    synthetic  -> PEA-eligible
    IE00B5BMR087  iShares Core S&P 500        physical   -> NOT PEA-eligible

Same index, opposite answers. Judging by the benchmark inverts the verdict for
the most popular funds in the database, which is why invariant C2 forbids the
tracked index from appearing anywhere in this file -- not merely in the branches
that return True. `tests/test_eligibility.py` asserts that statically over the
module's own syntax tree, so the rule cannot rot. It is also why the spec's
optional Rule 2b (guess from an "EU-heavy index") is deliberately **not**
implemented: the review queue is fed instead by the unverified seed rows and the
screener-only rows, which carry no such trap.

Evidence tiers
--------------
Ranked, first match wins; each tier carries its own confidence, because the three
best public screeners agree on only 88 of 243 contested ISINs and publishing a
bare boolean would present a coin flip and a prospectus reading as one fact.

    highest  the fund's prospectus / DIC states PEA eligibility
    high     the listing venue's own flag (Euronext `pea=Yes`), or the issuer's
             own screener (Amundi FUND_PEA, iShares peaFlag, BNP, Ossiam, SPDR)
    medium   two or more independent brokers list it under their PEA filter
    low      a single broker, or membership of the curated verified seed
    hint      screener/community lists or the unverified seed section only
             -> never sufficient for True; the row stays null and is queued
    none     nothing at all

Two notes on the ranking. A broker counts *singly* because a regulated
distributor carries regulatory risk for getting this wrong, while screeners,
GitHub lists and blogs carry none -- those never promote on their own. And
"independent" is counted per broker, not per URL: Boursorama appears in two
different exports and Bourse Direct in two, and letting them vote twice would
manufacture corroboration out of one opinion.

The seed's two sections
-----------------------
`reference/pea_eligible.csv` is one file with two bodies separated by a fenced
`# ===== UNVERIFIED =====` block. They are parsed apart and never merged: the
verified body met a documented bar (≥1 first-party source, or ≥2 independent
source families) and lands at `low` or better; the unverified body is
single-sourced or unconfirmed as an ETF and can only ever produce `hint`. The
unverified section is not counter-evidence either -- an ISIN listed there that a
venue or issuer independently flags still resolves at that source's tier, because
only Rule 0 is allowed to demote.

Rows that have no ISIN
----------------------
About 58% of the US universe cannot be assigned an ISIN at all: the algorithm
takes a CUSIP as input and CUSIP is a paid licence, so the US adapter emits a
surrogate key of the form `US:{exchange}:{ticker}` and is forbidden from
synthesising an identifier. Those rows are *not* unidentifiable, and they are the
rows this module classifies with the most certainty -- a US-domiciled fund is
ineligible and inaccessible on domicile alone, a determination the ISIN plays no
part in. Refusing them would delete ~3,250 confidently-classified funds.

So three cases are kept apart, and `_identify` is the only place that decides:

    a valid ISIN                      -> classified, evidence looked up by it
    no ISIN but a surrogate key       -> classified; evidence finds nothing,
                                         which is the null path, not an error
    an ISIN present and checksum-wrong-> refused (C5), because it is not absent,
                                         it is wrong, and it will join to the
                                         wrong fund or to none

The surrogate is read from the `isin` column itself when it carries the
`US:{exchange}:{ticker}` shape -- which is where it has to live, since
schema.FUNDS declares `isin` non-nullable -- or from a `surrogate_key`/`key`
column on rows still in flight. A row with neither is refused: it cannot be
joined to anything.

CTO accessibility is three facts, not one
-----------------------------------------
`cto_accessible` is the AND of: a PRIIPs KID exists (Reg. (EU) 1286/2014 art. 5),
it is in a language France accepts (art. 7), and the fund is passported for
marketing in France (AMF host-State notification, GECO). `has_priips_kid` and
`authorised_fr` are stored as separate columns because they fail for different
reasons and are repaired by different sources. US-domiciled 40-Act ETFs fail the
first one -- their sponsors have no EU distribution interest and publish no KID --
and that, not any exchange or tax rule, is why VOO and SPY cannot be sold to a
French retail client.

The synthetic-swap political risk, and the one query that answers it
--------------------------------------------------------------------
Sénat question écrite n° 08783 (14 May 2026, sén. Maurey) asks the government
what it intends to do about extra-European ETFs held through the PEA. It is still
marked "en attente de réponse". If it is ever answered with primary legislation,
every synthetic row in this database becomes suspect at once.

`pea_mechanism` is therefore populated on every row this module marks eligible,
so the blast radius is a single predicate rather than a re-scrape:

    -- re-flag everything exposed to the synthetic route
    UPDATE funds
       SET pea_eligible = NULL, pea_confidence = 'none'
     WHERE pea_mechanism = 'synthetic_swap';

`synthetic_swap_mask()` is the same predicate in pandas, so the claim is
executable and tested rather than aspirational.
"""

from __future__ import annotations

import csv
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import pyarrow as pa

from . import schema
from .config import REFERENCE
from .isin import (
    NON_COUNTRY_PREFIXES,
    InvalidIsinError,
    country,
    is_valid,
    normalise,
    require,
)

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Dates and citations
#
# C4 requires every non-null verdict to carry a source URL and the date that
# source was read, so these are constants rather than `date.today()`: the flag
# describes what a source said on a day, and it does not become fresher because
# the pipeline ran again.
# --------------------------------------------------------------------------- #

# reference/PEA_CTO_RULES.md: "every one fetch-verified on 2026-08-10".
RECON_AS_OF = date(2026, 8, 10)

# Article L221-31 CMF, version in force since 2025-02-16 (LOI 2025-127 art. 92-93).
LEGIFRANCE_L221_31 = (
    "https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000049720217"
)
PRIIPS_REGULATION = (
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32014R1286"
)

# Repo-relative, and naming the section: the two bodies of the seed file are two
# different claims, and a row that rests on the curated list must say which one.
SEED_VERIFIED_CITATION = "reference/pea_eligible.csv#verified"
SEED_UNVERIFIED_CITATION = "reference/pea_eligible.csv#unverified"


# --------------------------------------------------------------------------- #
# Jurisdiction
#
# EU-27 plus the three EEA States that signed an administrative-assistance
# convention with France. This is the set L221-31 I-2° c) admits; it is not a
# proxy for anything else, and in particular being domiciled here does NOT make a
# fund eligible -- it only fails to disqualify it.
# --------------------------------------------------------------------------- #

EEA = frozenset(
    {
        "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
        "FR", "GR", "HR", "HU", "IE", "IS", "IT", "LI", "LT", "LU",
        "LV", "MT", "NL", "NO", "PL", "PT", "RO", "SE", "SI", "SK",
    }
)


# --------------------------------------------------------------------------- #
# Source families
#
# Keyed by *provider*, not by URL. Boursorama publishes two different PEA
# listings and Bourse Direct two; counting URLs instead of providers would turn
# one broker's opinion into "two independent brokers" and promote a `low` row to
# `medium` on no new information.
# --------------------------------------------------------------------------- #

VENUE = "venue"  # the listing exchange's own flag
ISSUER = "issuer"  # the fund manufacturer's own screener
BROKER = "broker"  # a regulated distributor's PEA filter
SCREENER = "screener"  # third-party data sites
COMMUNITY = "community"  # GitHub lists, blogs
STALE = "stale"  # real, but too old to carry a flag
NO_FLAG = "no_flag"  # catalogues that publish no PEA information at all

# Only these promote a row to True.
PROMOTING_KINDS = frozenset({VENUE, ISSUER, BROKER})

_KIND: dict[str, str] = {
    "euronext": VENUE,
    "amundi": ISSUER,
    "ishares": ISSUER,
    "bnp-paribas-easy": ISSUER,
    "ossiam": ISSUER,
    "spdr": ISSUER,
    "vanguard": ISSUER,
    "hsbc": ISSUER,
    "bourse-direct": BROKER,
    "fortuneo": BROKER,
    "boursorama": BROKER,
    # The Saxo x Amundi PDF self-stamps "Sélection valable au 20 mars 2025".
    # Seventeen months is long enough for a fund to have breached the 75% quota
    # and been de-listed, so it is recorded (with its date) in
    # broker_availability and never used to set the flag.
    "saxo": STALE,
    "justetf": SCREENER,
    "scanetf": SCREENER,
    "github-pea-comparator": COMMUNITY,
    "github-etf-scrapper": COMMUNITY,
    "xtrackers": NO_FLAG,
}

# Matched as substrings of the source URL, first hit wins.
_SOURCE_BY_URL: tuple[tuple[str, str], ...] = (
    ("live.euronext.com", "euronext"),
    ("boursedirect.fr", "bourse-direct"),
    ("fortuneo.fr", "fortuneo"),
    ("boursorama.com", "boursorama"),
    ("home.saxo", "saxo"),
    ("amundietf.fr", "amundi"),
    ("blackrock.com", "ishares"),
    ("bnpparibas-am.com", "bnp-paribas-easy"),
    ("ossiam.net", "ossiam"),
    ("ssga.com", "spdr"),
    ("vanguard", "vanguard"),
    ("assetmanagement.hsbc.fr", "hsbc"),
    ("justetf.com", "justetf"),
    ("scanetf.com", "scanetf"),
    ("majordomef-sudo/pea-comparator", "github-pea-comparator"),
    ("corentinbouton/ETF-scrapper", "github-etf-scrapper"),
    ("etf.dws.com", "xtrackers"),
)

# Sources that carry their own date. Everything else was read on RECON_AS_OF;
# Euronext is overridden again from the stamp inside its own export.
_AS_OF: dict[str, date] = {
    "saxo": date(2025, 3, 20),  # "Sélection valable au 20 mars 2025"
    "github-etf-scrapper": date(2024, 7, 20),  # filename ETFs_20-07-24.xlsx
}

# Human labels for the log and for cto_note.
_LABEL: dict[str, str] = {
    "euronext": "Euronext product directory (pea=Yes)",
    "amundi": "Amundi ETF product API (FUND_PEA)",
    "ishares": "iShares France product pages (Régime fiscal PEA)",
    "bnp-paribas-easy": "BNP Paribas Easy share search (pea_flag)",
    "ossiam": "Ossiam share classes for France (isPea)",
    "spdr": "State Street SPDR France fund pages",
    "vanguard": "Vanguard fund documents",
    "hsbc": "HSBC AM France fund documents",
    "bourse-direct": "Bourse Direct instrument search (pea=true)",
    "fortuneo": "Fortuneo tracker search (pea=true)",
    "boursorama": "Boursorama tracker screener (eligibility=taxation)",
    "saxo": "Saxo x Amundi selection PDF",
}


def _source_key(url: str) -> str:
    """Map a source URL onto its provider key.

    Anything unrecognised falls through to `community`, which never promotes a
    row to True. A new upstream therefore cannot silently start voting; it has to
    be declared here first.
    """
    for fragment, key in _SOURCE_BY_URL:
        if fragment in url:
            return key
    host = re.sub(r"^https?://(www\.)?", "", url).split("/")[0]
    return f"blog-{host}" if host else "blog-unknown"


def _kind_of(key: str) -> str:
    return _KIND.get(key, COMMUNITY)


# --------------------------------------------------------------------------- #
# Evidence
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Source:
    """One provider's assertion set: "these ISINs are PEA-eligible", as of a date.

    `as_of` is the freshest date the source itself asserts, and only falls back to
    the date it was read. That distinction is the difference between a live daily
    export and a PDF whose own cover says it describes March 2025.
    """

    key: str
    kind: str
    url: str
    as_of: date
    isins: frozenset[str]

    @property
    def label(self) -> str:
        return _LABEL.get(self.key, self.key)


@dataclass(frozen=True)
class Evidence:
    """Everything the classifier is allowed to consult, indexed and dated.

    Built once per run and passed down, so classification is a pure function of
    (fund, evidence) -- no hidden I/O, and a test can substitute a hand-built
    Evidence to pin a single branch.
    """

    sources: dict[str, Source]
    seed_verified: frozenset[str]
    seed_unverified: frozenset[str]
    prospectus: dict[str, Source]
    # Replication as *stated by a source*, never inferred. Feeds pea_mechanism
    # when the universe frame has no replication of its own.
    replication: dict[str, str]
    # PEA-eligible instruments that are not ETFs (actively managed SICAV/FCP that
    # leak out of broker "tracker" screens). Out of scope for this database, but
    # not ineligible -- so this set never produces a False, it only keeps
    # non-ETFs out of the broker_availability table.
    not_etf: frozenset[str]
    # Catalogues that carry no PEA information whatsoever (Xtrackers/DWS publish
    # none, confirmed four ways). Held so that "we have seen this ISIN" is never
    # mistaken for "we have seen a flag for this ISIN"; the classifier does not
    # consult it.
    unflagged: frozenset[str]
    rejected_isins: tuple[str, ...]
    as_of: date

    def of_kind(self, kind: str) -> list[Source]:
        return [source for source in self.sources.values() if source.kind == kind]

    def citing(self, code: str) -> list[Source]:
        """Every source that lists this ISIN, best-documented kinds first."""
        order = {VENUE: 0, ISSUER: 1, BROKER: 2, SCREENER: 3, COMMUNITY: 4, STALE: 5}
        hits = [s for s in self.sources.values() if code in s.isins]
        return sorted(hits, key=lambda s: (order.get(s.kind, 9), s.key))

    def brokers_citing(self, code: str) -> list[Source]:
        return [s for s in self.citing(code) if s.kind == BROKER]

    def catalogue(self, broker: str) -> Source | None:
        """The named broker's catalogue, or None when this run holds none for it."""
        source = self.sources.get(broker.strip().lower())
        return source if source is not None and source.kind in (BROKER, STALE) else None


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #

_FENCE = re.compile(r"unverified", re.IGNORECASE)
_EURONEXT_STAMP = re.compile(r"\A(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\Z")
_MONTHS = {
    m: i
    for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun",
         "jul", "aug", "sep", "oct", "nov", "dec"],
        start=1,
    )
}


def _split_seed(text: str) -> tuple[list[str], list[str]]:
    """Split the seed file on its fenced comment block.

    The two sections are different claims about the world and the loader refuses
    to flatten them: the header row repeats after the fence, and everything after
    it is a lower trust tier.
    """
    verified: list[str] = []
    unverified: list[str] = []
    target = verified

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            if _FENCE.search(stripped):
                target = unverified
            continue
        if stripped.split(",", 1)[0].strip().lower() == "isin":
            continue  # the header, repeated once per section
        target.append(line)

    return verified, unverified


def _rows(header: str, body: Iterable[str]) -> list[dict[str, str]]:
    reader = csv.DictReader([header, *body])
    return [row for row in reader]


def _euronext(path: Path) -> tuple[list[str], date]:
    """Read the Euronext export: ISINs plus the day the venue regenerated it.

    The file is semicolon-separated with a BOM, and carries three metadata lines
    between the header and the data. One of them is the generation date, which is
    the honest `as_of` for every row in it -- the directory is rebuilt every
    trading day.
    """
    stamp = RECON_AS_OF
    codes: list[str] = []

    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, [])
        columns = [n for n, name in enumerate(header) if name.strip().upper() == "ISIN"]
        if not columns:
            raise ValueError(f"{path.name} has no ISIN column")
        column = columns[0]

        for row in reader:
            if len(row) == 1:
                match = _EURONEXT_STAMP.match(row[0].strip())
                if match:
                    day, month, year = match.groups()
                    stamp = date(int(year), _MONTHS[month.lower()], int(day))
                continue
            if len(row) > column:
                codes.append(row[column])

    return codes, stamp


def _replication(raw: str | None) -> str | None:
    """Normalise a source's replication wording onto the schema vocabulary.

    Sources write `Indirect(Swap Based)`, `Synthétique / Swap`, `synthetic`,
    `Direct(Physical)`, `Physique / Réplication totale`. The swap test runs first
    because "Indirect(Swap Based)" contains the substring "direct".
    """
    if not raw:
        return None
    text = raw.strip().lower()
    if not text:
        return None
    if "swap" in text or "synth" in text or "indirect" in text:
        return "synthetic-swap"
    if "physi" in text or "direct" in text:
        return "physical-full"
    return None


def load_evidence(root: Path | None = None) -> Evidence:
    """Parse the seed and every evidence export into indexed, dated source sets.

    `root` defaults to `reference/`. Every ISIN is checksum-checked on the way in
    (invariant C5) and the rejects are kept in `Evidence.rejected_isins` rather
    than dropped in silence -- a source that starts emitting malformed
    identifiers is a source that has changed shape.
    """
    root = Path(root) if root is not None else REFERENCE
    evidence_dir = root / "evidence"

    codes_by_key: dict[str, set[str]] = defaultdict(set)
    url_by_key: dict[str, str] = {}
    as_of_by_key: dict[str, date] = {}
    replication: dict[str, str] = {}
    rejected: list[str] = []

    def record(raw_isin: Any, url: str, as_of: date | None = None) -> str | None:
        code = normalise(raw_isin)
        if code is None:
            return None
        if not is_valid(code):
            rejected.append(code)  # C5: never indexed, never inserted
            return None
        key = _source_key(url)
        codes_by_key[key].add(code)
        url_by_key.setdefault(key, url)
        stamp = as_of or _AS_OF.get(key, RECON_AS_OF)
        # Keep the most recent reading of a provider that appears twice.
        if stamp > as_of_by_key.get(key, date.min):
            as_of_by_key[key] = stamp
        return code

    # The venue export, which dates itself.
    euronext_path = evidence_dir / "euronext_pea_yes.csv"
    if euronext_path.exists():
        codes, stamp = _euronext(euronext_path)
        for code in codes:
            record(code, "https://live.euronext.com/en/products/etfs/list", stamp)

    # Every other export is a flat CSV carrying its own per-row source_url, so a
    # single loop attributes rows to providers regardless of which file they
    # arrived in -- Boursorama rows live in both broker_lists.csv and
    # community_lists.csv, and must resolve to the same broker either way.
    for name in (
        "issuer_lists.csv",
        "ishares_pea.csv",
        "bnp_xtrackers_pea.csv",
        "broker_lists.csv",
        "community_lists.csv",
    ):
        path = evidence_dir / name
        if not path.exists():
            log.warning("evidence file missing: %s", path)
            continue
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                code = record(row.get("isin"), (row.get("source_url") or "").strip())
                if code:
                    stated = _replication(row.get("replication"))
                    if stated:
                        replication.setdefault(code, stated)

    # The curated seed, kept in two tiers.
    seed_verified: set[str] = set()
    seed_unverified: set[str] = set()
    seed_path = root / "pea_eligible.csv"
    if seed_path.exists():
        text = seed_path.read_text(encoding="utf-8")
        header = text.splitlines()[0]
        verified_lines, unverified_lines = _split_seed(text)
        for block, bucket in (
            (verified_lines, seed_verified),
            (unverified_lines, seed_unverified),
        ):
            for row in _rows(header, block):
                url = (row.get("source_url") or "").strip()
                # A seed row is evidence twice over: it names the provider that
                # asserted the flag (so it feeds that provider's set) and it
                # carries the curator's tier.
                code = record(row.get("isin"), url)
                if not code:
                    continue
                bucket.add(code)
                stated = _replication(row.get("replication"))
                if stated:
                    replication.setdefault(code, stated)

    # Catalogues that publish no flag: held, never consulted.
    unflagged: set[str] = set()
    xtrackers_path = evidence_dir / "xtrackers_full_universe_no_pea_flag.csv"
    if xtrackers_path.exists():
        with xtrackers_path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                code = normalise(row.get("isin"))
                if code and is_valid(code):
                    unflagged.add(code)

    not_etf: set[str] = set()
    dropped_path = evidence_dir / "dropped_not_etf.txt"
    if dropped_path.exists():
        for line in dropped_path.read_text(encoding="utf-8").splitlines():
            token = line.strip().split(" ", 1)[0]
            if is_valid(token):
                not_etf.add(normalise(token) or "")

    # An optional, currently absent export: per-ISIN prospectus/DIC readings, the
    # only primary source there is. Décret 2026-189 confirms no public register
    # will ever exist, so this tier can only ever be filled by reading documents.
    prospectus: dict[str, Source] = {}
    prospectus_path = evidence_dir / "prospectus_pea.csv"
    if prospectus_path.exists():
        with prospectus_path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                code = normalise(row.get("isin"))
                if not code or not is_valid(code):
                    if code:
                        rejected.append(code)
                    continue
                stamp = row.get("as_of") or ""
                prospectus[code] = Source(
                    key="prospectus",
                    kind="prospectus",
                    url=(row.get("source_url") or "").strip(),
                    as_of=date.fromisoformat(stamp) if stamp else RECON_AS_OF,
                    isins=frozenset({code}),
                )

    sources = {
        key: Source(
            key=key,
            kind=_kind_of(key),
            url=url_by_key.get(key, ""),
            as_of=as_of_by_key.get(key, RECON_AS_OF),
            isins=frozenset(codes),
        )
        for key, codes in codes_by_key.items()
    }

    if rejected:
        log.warning(
            "%d ISIN(s) failed the ISO 6166 check digit and were not indexed: %s",
            len(rejected),
            sorted(set(rejected))[:10],
        )

    return Evidence(
        sources=sources,
        seed_verified=frozenset(seed_verified),
        seed_unverified=frozenset(seed_unverified),
        prospectus=prospectus,
        replication=replication,
        not_etf=frozenset(not_etf),
        unflagged=frozenset(unflagged),
        rejected_isins=tuple(sorted(set(rejected))),
        as_of=RECON_AS_OF,
    )


# --------------------------------------------------------------------------- #
# Reading a fund row
#
# `fund` is any mapping: a dict, a pandas Series, a row of a frame conforming to
# schema.FUNDS. Missing and NaN are the same thing here -- "the universe stage
# did not establish this" -- and both must read as None rather than as False.
# --------------------------------------------------------------------------- #


def _field(fund: Mapping[str, Any], key: str) -> Any:
    try:
        value = fund[key]
    except (KeyError, IndexError, TypeError):
        return None
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    try:
        if value is pd.NA or pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _text(fund: Mapping[str, Any], key: str) -> str:
    value = _field(fund, key)
    return "" if value is None else str(value).strip()


def _flag(fund: Mapping[str, Any], key: str) -> bool | None:
    value = _field(fund, key)
    return None if value is None else bool(value)


# `US:{exchange}:{ticker}`. A 12-character ISIN can never contain a colon, so the
# two shapes cannot be confused, and the surrogate is read verbatim -- tickers
# carry dots and hyphens (BRK.B, RDS-A) that ISIN normalisation would strip.
_SURROGATE_KEY = re.compile(r"\A[A-Za-z]{2,}:[^\s:]+:[^\s:]+\Z")


def _identify(fund: Mapping[str, Any]) -> tuple[str | None, str]:
    """Resolve a row's identity into (ISIN or None, the key to name it by).

    "No ISIN" and "a wrong ISIN" are different failures and get different
    treatment. An absent identifier is legitimate for the 58% of the US universe
    whose CUSIP -- the input to the ISIN algorithm -- sits behind a paid licence;
    those rows carry `US:{exchange}:{ticker}` instead, and are classified
    normally with the ISIN simply playing no part. A *present* identifier that
    fails the ISO 6166 check digit is refused, because it is not missing, it is
    wrong, and it will join to the wrong fund (C5).

    Raises InvalidIsinError when the ISIN is malformed, or when the row carries
    no identifier of either kind.
    """
    raw = _field(fund, "isin")
    text = "" if raw is None else str(raw).strip()

    if text:
        if _SURROGATE_KEY.match(text):
            return None, text
        code = require(text)
        return code, code

    for column in ("surrogate_key", "key"):
        surrogate = _text(fund, column)
        if surrogate:
            return None, surrogate

    raise InvalidIsinError(
        "row carries neither an ISIN nor a surrogate key, so it cannot be "
        "identified or joined to anything"
    )


def _isin_or_none(fund: Mapping[str, Any]) -> str | None:
    """The row's ISIN when it has a usable one, without raising on the ones that don't."""
    code = normalise(_field(fund, "isin"))
    return code if code is not None and is_valid(code) else None


def _domicile(fund: Mapping[str, Any]) -> str | None:
    """The fund's domicile: declared if the universe knows it, else the ISIN prefix.

    The fallback matters because it is the only thing standing between a
    half-populated universe row and a US 40-Act ETF being offered to a French
    holder. For collective vehicles the numbering agency that allocated the ISIN
    is the one in the fund's own jurisdiction, and `isin.country` already returns
    None for the supranational prefixes that encode no jurisdiction at all.
    """
    declared = _text(fund, "domicile").upper()
    # `XS` and friends are numbering-agency codes that some upstreams write into
    # a domicile column. They are not jurisdictions, and taking one at face value
    # would fail Rule 0 on a placeholder.
    if len(declared) == 2 and declared.isalpha() and declared not in NON_COUNTRY_PREFIXES:
        return declared
    return country(_field(fund, "isin"))


# --------------------------------------------------------------------------- #
# Rule 0 -- the only branch allowed to say False
# --------------------------------------------------------------------------- #

# Legal forms that are not collective investment vehicles. L221-31 I-2° admits
# OPCVM / SICAV / FCP only, so an exchange-traded commodity or note is out
# whatever it holds. Matched case-sensitively on the abbreviations, because "etc"
# in running prose is not a legal form.
_ABBREVIATED_NON_FUND = re.compile(r"(?<![A-Za-z])(ETC|ETN)(?![A-Za-z])")
_SPELLED_NON_FUND = re.compile(
    r"exchange[ -]traded[ -](commodit|note)|structured\s+note|\bcertificat",
    re.IGNORECASE,
)
_NON_FUND_FORMS = frozenset({"etc", "etn", "certificate", "certificat", "structured_note", "note"})

_SWAP_NAME = re.compile(r"\bswap\b", re.IGNORECASE)


def _not_a_fund(fund: Mapping[str, Any]) -> bool:
    """True when the instrument is structurally outside the OPCVM test.

    A declared `legal_form` wins; otherwise the name is read, which is all most
    upstreams give. Both are positive assertions about the instrument -- nothing
    here infers "not a fund" from missing data.
    """
    declared = _text(fund, "legal_form").lower().replace(" ", "_")
    if declared:
        return declared in _NON_FUND_FORMS

    name = _text(fund, "name")
    return bool(_ABBREVIATED_NON_FUND.search(name) or _SPELLED_NON_FUND.search(name))


def _disqualifier(fund: Mapping[str, Any]) -> tuple[str, str] | None:
    """Return (reason, citation) when the fund cannot be PEA-eligible, else None.

    Two branches only. The spec's third -- non-UCITS and outside the EEA -- is
    strictly narrower than the domicile branch and can never fire on its own, so
    it is folded in rather than duplicated.

    An unknown domicile is *not* a disqualifier. Silence is not a "no".
    """
    if _not_a_fund(fund):
        return "not_a_fund", LEGIFRANCE_L221_31

    domicile = _domicile(fund)
    if domicile is not None and domicile not in EEA:
        return "domicile_outside_eea", LEGIFRANCE_L221_31

    return None


# --------------------------------------------------------------------------- #
# Rule 1 -- positive evidence, ranked
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Support:
    """The best evidence found for one ISIN, and how good it is."""

    confidence: str
    reason: str
    source_url: str
    as_of: date | None
    citations: tuple[str, ...]

    @property
    def promotes(self) -> bool:
        """Whether this evidence is strong enough to publish a True."""
        return self.confidence in {"highest", "high", "medium", "low"}


_NO_SUPPORT = Support("none", "no_positive_evidence", "", None, ())


def _support(code: str | None, evidence: Evidence) -> Support:
    """Rank the evidence for one ISIN. First match wins; nothing here is inferred.

    A row with no ISIN can never match an evidence set, since every source we
    hold is ISIN-keyed. That is the null path, not an error: it says the source
    coverage cannot reach this row, which is exactly true of the US universe.

    Ordered by confidence rather than by the pseudocode's listing order: the spec
    puts the Euronext check above the prospectus check while grading the
    prospectus higher, and with first-match-wins that would record the weaker of
    two available proofs. The flag is identical either way; only the recorded
    confidence and source differ.
    """
    if code is None:
        return Support("none", "no_isin_to_match", "", None, ())

    citations = tuple(source.key for source in evidence.citing(code))

    document = evidence.prospectus.get(code)
    if document is not None:
        return Support("highest", "prospectus_or_dic", document.url, document.as_of, citations)

    for kind, reason in ((VENUE, "venue_flag"), (ISSUER, "issuer_flag")):
        hits = [s for s in evidence.citing(code) if s.kind == kind]
        if hits:
            best = max(hits, key=lambda s: s.as_of)
            return Support("high", reason, best.url, best.as_of, citations)

    brokers = evidence.brokers_citing(code)
    if len(brokers) >= 2:
        best = max(brokers, key=lambda s: s.as_of)
        return Support("medium", "brokers_corroborated", best.url, best.as_of, citations)
    if len(brokers) == 1:
        only = brokers[0]
        return Support("low", "single_broker", only.url, only.as_of, citations)

    # The curated seed's verified body met a documented bar: at least one
    # first-party source, or at least two independent source families. That bar
    # is the single-broker bar, so it lands at the single-broker tier -- the row
    # is publishable and belongs in the human review queue.
    if code in evidence.seed_verified:
        return Support("low", "seed_verified", SEED_VERIFIED_CITATION,
                       evidence.as_of, citations)

    # Everything below is structurally plausible and unproven. It never promotes;
    # it only marks the row for verification.
    if code in evidence.seed_unverified:
        return Support("hint", "seed_unverified", SEED_UNVERIFIED_CITATION,
                       evidence.as_of, citations)

    weak = [s for s in evidence.citing(code) if s.kind not in PROMOTING_KINDS]
    if weak:
        best = max(weak, key=lambda s: s.as_of)
        return Support("hint", "screener_or_community_only", best.url, best.as_of, citations)

    return Support("none", "no_positive_evidence", "", None, citations)


def mechanism_of(fund: Mapping[str, Any], evidence: Evidence) -> str:
    """How a fund satisfies the 75% quota: `physical_eu`, `synthetic_swap`, `unknown`.

    Derived from the replication method only -- declared on the universe row, or
    stated by a source, or spelled out in the fund's own name. The spec's version
    additionally consults the benchmark to decide "physical_eu"; this one does
    not, both because C2 bars the benchmark from this file and because the
    inference is unnecessary: a fund that is *evidenced* eligible and replicates
    physically must, by L221-31 I-2°, be holding EU/EEA equities directly. The
    benchmark adds nothing except a way to get it wrong.
    """
    code = _isin_or_none(fund)
    declared = _replication(_text(fund, "replication")) or (
        evidence.replication.get(code or "")
    )
    if declared == "synthetic-swap":
        return "synthetic_swap"
    if declared and declared.startswith("physical"):
        return "physical_eu"
    if _SWAP_NAME.search(_text(fund, "name")):
        return "synthetic_swap"
    return "unknown"


# --------------------------------------------------------------------------- #
# Verdicts
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PeaVerdict:
    """`eligible` is tri-state on purpose: True, False and "we do not know" differ.

    `reason` is not persisted -- schema.FUNDS has no column for it -- but it is
    what makes the verdict reviewable at the API level, and it is what the tests
    assert against.
    """

    eligible: bool | None
    confidence: str
    mechanism: str
    source: str
    as_of: date | None
    reason: str = ""


@dataclass(frozen=True)
class CtoVerdict:
    """The AND of three independent facts, with two of them kept visible.

    `has_priips_kid` and `authorised_fr` are separate because they fail for
    different reasons and are repaired by different sources: the first by the
    manufacturer publishing a KID, the second by the sponsor paying the AMF
    notification fee for the French market.
    """

    accessible: bool | None
    reason: str
    has_priips_kid: bool | None
    authorised_fr: bool | None
    note: str = ""


def classify_pea(fund: Mapping[str, Any], evidence: Evidence) -> PeaVerdict:
    """Decide PEA eligibility for one fund.

    Returns True only on positive, attributed evidence; False only for the
    structural disqualifiers of Rule 0; None for everything else, including every
    fund nobody has published a flag for.

    A row identified only by a surrogate key is classified normally -- Rule 0
    still applies, so a US-domiciled row without an ISIN still comes out False.
    Raises InvalidIsinError only when the ISIN is present and fails the ISO 6166
    check digit (C5), or when the row carries no identifier at all.
    """
    code, _key = _identify(fund)

    blocked = _disqualifier(fund)
    if blocked is not None:
        reason, citation = blocked
        # High, not highest: the certainty is in the statute, but the input fact
        # (legal form, domicile) came from a source that can be wrong.
        return PeaVerdict(False, "high", "unknown", citation, RECON_AS_OF, reason)

    support = _support(code, evidence)
    if support.promotes:
        return PeaVerdict(
            True,
            support.confidence,
            mechanism_of(fund, evidence),
            support.source_url,
            support.as_of,
            support.reason,
        )

    # Rule 2: negative inference is forbidden. Absence from every list we hold
    # says something about the lists, not about the fund.
    return PeaVerdict(None, support.confidence, "unknown", support.source_url,
                      support.as_of, support.reason)


def classify_cto(
    fund: Mapping[str, Any],
    evidence: Evidence,
    broker: str | None = None,
) -> CtoVerdict:
    """Decide whether a French retail client can buy this fund in a CTO.

    With `broker=None` this answers the fund-level question -- may a French
    distributor lawfully sell it at all -- and that is the answer stored on the
    funds table. With a broker named it answers the narrower question "is it in
    *their* catalogue", which legitimately returns None when we hold no catalogue
    for that broker or the fund is simply absent from it.

    Accepts a row identified only by a surrogate key, on the same terms as
    `classify_pea`: the domicile tests below never needed the ISIN.
    """
    code, _key = _identify(fund)
    domicile = _domicile(fund)
    ucits = _flag(fund, "ucits")

    # Facts a future GECO / KID adapter may already have established. A stored
    # fact always beats an inference drawn here.
    has_kid = _flag(fund, "has_priips_kid")
    authorised = _flag(fund, "authorised_fr")
    kid_in_french = _flag(fund, "kid_language_fr")

    # --- B1. Does a PRIIPs KID exist at all? -------------------------------- #
    if domicile == "US":
        # 40-Act sponsors have no EU distribution interest and publish no KID, so
        # art. 13(1) stops the distributor, not the exchange. This is the whole
        # reason VOO and SPY are unbuyable for a French retail client.
        return CtoVerdict(
            False,
            "no_priips_kid",
            has_priips_kid=False,
            authorised_fr=authorised,
            note=(
                "US-domiciled 40-Act fund: no PRIIPs KID, so no EU distributor may "
                f"sell it to a retail client ({PRIIPS_REGULATION}). Holding and "
                "selling an existing position remain possible."
            ),
        )

    if domicile == "GB":
        return CtoVerdict(
            False,
            "uk_ucits_is_third_country",
            has_priips_kid=has_kid,
            authorised_fr=False,
            note=(
                "UK UCITS is a third-country product since Brexit: the marketing "
                "passport into France lapsed, so it cannot be sold to French retail."
            ),
        )

    if domicile is not None and domicile not in EEA:
        # Deliberately softer than the spec's MEDIUM false. Plenty of Jersey- and
        # Guernsey-domiciled ETCs do publish a French KID and trade on Euronext
        # Paris, so "outside the EEA" is not by itself proof that no KID exists.
        # We hold no KID-document source, so the honest answer is "unknown".
        return CtoVerdict(
            None,
            "unknown",
            has_priips_kid=has_kid,
            authorised_fr=authorised,
            note=(
                f"domiciled in {domicile}, outside the EEA: a PRIIPs KID may still "
                "exist (many Channel Islands ETCs publish one) but we hold no "
                "document for it. Verify per fund before trusting."
            ),
        )

    if has_kid is None and domicile in EEA and ucits is True:
        # Since 1 Jan 2023 the UCITS carve-out of PRIIPs art. 32(1) is gone
        # (Reg. 2021/2259): every UCITS sold to EEA retail publishes a KID.
        has_kid = True

    # --- B2. Language France accepts (art. 7) ------------------------------- #
    if kid_in_french is False:
        return CtoVerdict(
            None,
            "kid_language_unconfirmed",
            has_priips_kid=has_kid,
            authorised_fr=authorised,
            note="no French-language KID confirmed; the AMF accepts another "
                 "language only case by case.",
        )

    # --- B3. Passported for marketing in France (AMF host-State notification) - #
    if authorised is False:
        return CtoVerdict(
            False,
            "not_passported_to_france",
            has_priips_kid=has_kid,
            authorised_fr=False,
            note="not notified to the AMF for marketing in France; the €2,000/year "
                 "per-fund contribution means sponsors notify selectively.",
        )

    # A fund positively evidenced as offered inside a French PEA is, by that same
    # fact, marketed in France: the AMF's 17 July 2025 reminder makes the
    # host-State notification and a French KID preconditions of distribution.
    # This is what makes invariant C1 structural rather than aspirational.
    support = _support(code, evidence)
    pea_offered = support.promotes and _disqualifier(fund) is None
    if pea_offered:
        has_kid = True if has_kid is None else has_kid
        authorised = True if authorised is None else authorised

    # --- B4. Broker layer: catalogue inclusion is not venue reachability ----- #
    note = ""
    if broker is not None:
        catalogue = evidence.catalogue(broker)
        if catalogue is None:
            note = (
                f"no catalogue held for {broker!r} in this run; answering the "
                "fund-level question instead."
            )
        elif code in catalogue.isins:
            return CtoVerdict(
                True,
                "in_broker_catalogue",
                has_priips_kid=True,
                authorised_fr=True,
                note=f"listed by {catalogue.label} as of {catalogue.as_of.isoformat()}",
            )
        else:
            # Absence from a catalogue is not evidence of anything about the fund.
            return CtoVerdict(
                None,
                "not_in_broker_catalogue",
                has_priips_kid=has_kid,
                authorised_fr=authorised,
                note=(
                    f"not in {catalogue.label} (read {catalogue.as_of.isoformat()}); "
                    "that is a statement about the broker, not about the fund."
                ),
            )

    if has_kid is True:
        detail = (
            "offered inside a French PEA, so it is marketed in France"
            if pea_offered
            else "EEA UCITS: a PRIIPs KID is mandatory since 1 Jan 2023 "
                 "(Reg. 2021/2259). AMF passport not independently checked."
        )
        return CtoVerdict(
            True,
            "ucits_eea_with_kid",
            has_priips_kid=True,
            authorised_fr=authorised,
            note=" ".join(filter(None, (note, detail))),
        )

    return CtoVerdict(
        None,
        "unknown",
        has_priips_kid=has_kid,
        authorised_fr=authorised,
        note=" ".join(
            filter(
                None,
                (
                    note,
                    "UCITS status or domicile unestablished, so the KID obligation "
                    "cannot be resolved.",
                ),
            )
        ),
    )


# --------------------------------------------------------------------------- #
# Whole-universe classification
# --------------------------------------------------------------------------- #

ELIGIBILITY_COLUMNS = (
    "pea_eligible",
    "pea_confidence",
    "pea_mechanism",
    "pea_source",
    "pea_as_of",
    "cto_accessible",
    "cto_reason",
    "cto_note",
    "has_priips_kid",
    "authorised_fr",
)


def classify_all(
    funds_df: pd.DataFrame,
    evidence: Evidence,
    *,
    on_invalid_isin: str = "raise",
) -> pd.DataFrame:
    """Fill the eligibility columns of a funds frame, then verify the invariants.

    Rows identified only by a surrogate key are classified like any other: they
    are the largest block of confidently-decided rows in the universe, and losing
    them would delete ~3,250 US funds that Rule 0 answers with certainty.

    `on_invalid_isin` governs only the rows whose identifier is present and
    *wrong*, and is "raise" by default because C5 says a checksum-invalid ISIN
    must be rejected before insert -- a producer emitting one is a bug, not a data
    gap. "drop" keeps the build alive; the number and the identifiers dropped are
    logged and returned on the result frame as `attrs["dropped_invalid_isin"]` and
    `attrs["dropped_rows"]`, so a build cannot lose rows unnoticed.

    The invariants are re-checked on the result: they are cheap, and a violation
    means two sources contradict each other about something that can cost a user
    their plan, which is a build failure and not a warning.
    """
    if on_invalid_isin not in {"raise", "drop"}:
        raise ValueError(f"unknown on_invalid_isin: {on_invalid_isin!r}")

    frame = funds_df.copy()
    if "isin" not in frame.columns:
        raise ValueError("funds frame has no isin column")

    records = frame.to_dict("records")
    ok: list[bool] = []
    offenders: list[str] = []
    for record in records:
        try:
            _identify(record)
            ok.append(True)
        except InvalidIsinError as exc:
            ok.append(False)
            offenders.append(f"{record.get('isin')!r} ({exc})")

    if offenders:
        if on_invalid_isin == "raise":
            raise InvalidIsinError(
                f"{len(offenders)} fund row(s) cannot be identified and must not be "
                f"inserted: {offenders[:10]}"
            )
        log.warning(
            "dropping %d unidentifiable row(s) of %d: %s",
            len(offenders),
            len(records),
            offenders[:10],
        )
        frame = frame.loc[pd.Series(ok, dtype="bool").to_numpy()]
        records = [record for record, good in zip(records, ok) if good]

    pea = [classify_pea(record, evidence) for record in records]
    cto = [classify_cto(record, evidence) for record in records]

    frame["pea_eligible"] = pd.array([v.eligible for v in pea], dtype="boolean")
    frame["pea_confidence"] = [v.confidence for v in pea]
    frame["pea_mechanism"] = [v.mechanism for v in pea]
    frame["pea_source"] = [v.source or None for v in pea]
    # Only a decided row carries a date, so C4 reads as "decided implies dated"
    # rather than "everything is dated with the day the pipeline happened to run".
    frame["pea_as_of"] = [
        v.as_of if v.eligible is not None else None for v in pea
    ]
    frame["cto_accessible"] = pd.array([v.accessible for v in cto], dtype="boolean")
    frame["cto_reason"] = [v.reason for v in cto]
    frame["cto_note"] = [v.note or None for v in cto]
    frame["has_priips_kid"] = pd.array([v.has_priips_kid for v in cto], dtype="boolean")
    frame["authorised_fr"] = pd.array([v.authorised_fr for v in cto], dtype="boolean")

    check_invariants(frame)
    frame.attrs["dropped_invalid_isin"] = len(offenders)
    frame.attrs["dropped_rows"] = tuple(offenders)
    return frame


def check_invariants(funds: pd.DataFrame) -> None:
    """Enforce C1, C3, C4 and C5 on a classified frame.

    Raises ValueError naming the offending ISINs. Called by `classify_all`, and
    worth calling again after any later stage that touches these columns.
    """
    # Comparisons on a nullable boolean column propagate NA, and a mask carrying
    # NA cannot select rows -- so every predicate below resolves the null case
    # explicitly instead of letting pandas raise.
    eligible = funds["pea_eligible"]
    accessible = funds["cto_accessible"]
    pea_true = eligible.eq(True).fillna(False)
    pea_false = eligible.eq(False).fillna(False)
    cto_true = accessible.eq(True).fillna(False)
    cto_false = accessible.eq(False).fillna(False)

    # C5 first: an unidentifiable row makes every other check meaningless. A row
    # legitimately carrying a surrogate key instead of an ISIN passes -- what is
    # barred is an identifier that is present and wrong.
    bad: list[str] = []
    for record in funds.to_dict("records"):
        try:
            _identify(record)
        except InvalidIsinError as exc:
            bad.append(f"{record.get('isin')!r} ({exc})")
    if bad:
        raise ValueError(f"C5 violated: {len(bad)} unidentifiable row(s): {bad[:10]}")

    # C1: PEA-eligible implies CTO-accessible. A PEA-eligible fund is an EEA
    # UCITS marketed in France by construction, so a False or null here means two
    # sources disagree about whether the fund is sold in France at all.
    broken_c1 = funds[pea_true & ~cto_true]
    if len(broken_c1):
        raise ValueError(
            f"C1 violated: {len(broken_c1)} PEA-eligible fund(s) not CTO-accessible: "
            f"{list(broken_c1['isin'][:10])}"
        )

    # C3: US domicile implies both False.
    if "domicile" in funds.columns:
        is_us = funds["domicile"].astype("string").str.upper().eq("US").fillna(False)
        broken_c3 = funds[is_us & ~(pea_false & cto_false)]
        if len(broken_c3):
            raise ValueError(
                f"C3 violated: {len(broken_c3)} US-domiciled fund(s) not marked "
                f"ineligible/inaccessible: {list(broken_c3['isin'][:10])}"
            )

    # C4: every decided row is attributed and dated.
    decided = funds[eligible.notna()]
    undated = decided[decided["pea_source"].isna() | decided["pea_as_of"].isna()]
    if len(undated):
        raise ValueError(
            f"C4 violated: {len(undated)} decided row(s) without a source URL and "
            f"an as_of date: {list(undated['isin'][:10])}"
        )


def synthetic_swap_mask(funds: pd.DataFrame) -> pd.Series:
    """Rows exposed to the synthetic-swap political risk, in one predicate.

    The executable form of the UPDATE in the module docstring. Sénat QE n° 08783
    is unanswered; if it is ever answered with legislation, this mask is the blast
    radius and re-flagging is a single assignment, not a re-derivation.
    """
    return funds["pea_mechanism"].astype("string").eq("synthetic_swap").fillna(False)


# --------------------------------------------------------------------------- #
# broker_availability
# --------------------------------------------------------------------------- #


def broker_availability(evidence: Evidence) -> pa.Table:
    """Build the (broker × ISIN) availability table declared in schema.

    Positive rows only. A row means "this broker's own PEA screen listed this
    ISIN when we read it"; the absence of a row means we have no information,
    which is why `available` is never written as False.

    Every row is dated, because these catalogues age at very different rates: the
    Saxo × Amundi selection is a PDF from March 2025 and Bourse Direct's API is
    live, and a consumer must be able to tell them apart.
    """
    brokers = [
        source
        for source in evidence.sources.values()
        if source.kind in (BROKER, STALE)
    ]

    rows: list[tuple[str, str, str, date]] = []
    for source in sorted(brokers, key=lambda s: s.key):
        # These screens leak actively managed SICAV/FCP; this table describes the
        # ETF database, so the known non-ETFs are left out rather than joined
        # against a funds table they will never match.
        for code in sorted(source.isins - evidence.not_etf):
            rows.append((source.key, code, source.url, source.as_of))

    table = pa.table(
        {
            "broker": pa.array([r[0] for r in rows], pa.string()),
            "isin": pa.array([r[1] for r in rows], pa.string()),
            "available": pa.array([True] * len(rows), pa.bool_()),
            "wrapper": pa.array(["pea"] * len(rows), pa.string()),
            "source_url": pa.array([r[2] for r in rows], pa.string()),
            "as_of": pa.array([r[3] for r in rows], pa.date32()),
        }
    )
    return schema.conform(table, "broker_availability")
