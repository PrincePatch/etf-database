"""Tests for the PEA / CTO classifier.

Three kinds of test live here.

The first kind runs against the real `reference/` corpus, because the facts this
module encodes are facts about that corpus: that Euronext flags 154 ISINs, that
the seed has 250 verified and 289 unverified rows, that `FR0011871128` is a
synthetic S&P 500 tracker which is nonetheless PEA-eligible. A fixture cannot
prove any of that.

The second kind runs against hand-built `Evidence`, because the tier boundaries
-- one broker versus two, a stale list versus a live one -- cannot be pinned
against real data where almost every broker-listed ISIN is also venue-flagged.

The third kind is the invariants, C1 to C5. They get their own tests because each
one, violated, is a specific way for this database to cost somebody their plan:

    C1  pea_eligible True implies cto_accessible True
    C2  the tracked index appears in no branch that returns True -- asserted
        statically over the module's syntax tree, and behaviourally by sweeping
        the benchmark fields and checking the verdict never moves
    C3  US domicile implies both False
    C4  every decided row carries a source URL and an as_of date
    C5  every ISIN passes the ISO 6166 check digit before insert

The asymmetry under all of it: a wrong True can force a PEA closed
(BOI-RPPM-RCM-40-50-50) and lose the holder five years of tax clock, while a null
costs a missed purchase. So several tests below assert `is None` and separately
assert `is not False`, which is not redundant -- it is the difference between
"we do not know" and "we told the user no".
"""

from __future__ import annotations

import ast
import inspect
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from pipeline import eligibility, isin, schema
from pipeline.isin import InvalidIsinError

DAY = date(2026, 8, 10)

# Real ISINs, all read out of reference/ or from a fund's own literature.
AMUNDI_PEA_SP500 = "FR0011871128"  # synthetic, Euronext pea=Yes
ISHARES_SP500_SWAP_PEA = "IE000DQLYVB9"  # synthetic, Euronext pea=Yes
ISHARES_CORE_SP500 = "IE00B5BMR087"  # physical, same index, NOT eligible
ISHARES_CORE_WORLD = "IE00B4L5Y983"  # physical, in none of the PEA lists
BNP_SP500 = "FR0011550185"  # issuer-flagged (BNP pea_flag)
TWO_BROKERS = "IE00BG0J9Y53"  # Bourse Direct + Fortuneo, no venue/issuer flag
SEED_VERIFIED_ONLY = "DE000A2QP331"  # verified body, no primary source of its own
SEED_UNVERIFIED_ONLY = "BG9000011163"  # unverified body, GitHub list only
UNFLAGGED_ONLY = "DE000A1E0HR8"  # in the Xtrackers catalogue, which has no PEA flag
VOO = "US9229083632"  # Vanguard S&P 500 ETF, 40-Act
SPY = "US78462F1030"  # SPDR S&P 500 ETF Trust, 40-Act
UK_FUND = "GB0002634946"
XETRA_GOLD = "DE000A0S9GB0"  # ETC, EEA-domiciled -- disqualified as a legal form
WISDOMTREE_GOLD = "JE00B1VS3770"  # ETC, Jersey
BAD_CHECKSUM = "FR0013412345"  # rejected during the seed build; a placeholder
US_SURROGATE = "US:XNYS:VOO"  # the shape the US adapter emits when no ISIN exists


# --------------------------------------------------------------------------- #
# Fixtures and builders
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def evidence() -> eligibility.Evidence:
    """The real reference corpus, parsed once."""
    return eligibility.load_evidence()


def fund(code: str, **overrides) -> dict:
    """One universe row, shaped like schema.FUNDS.

    Everything the classifier is not told stays None, which is the state most of
    a 15,000-fund universe is actually in.
    """
    row = {
        "isin": code,
        "name": "",
        "domicile": None,
        "ucits": None,
        "replication": None,
        "index_name": None,
        "index_provider": None,
    }
    row.update(overrides)
    return row


def universe(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def source(key: str, kind: str, codes: list[str], as_of: date = DAY) -> eligibility.Source:
    return eligibility.Source(
        key=key,
        kind=kind,
        url=f"https://example.test/{key}",
        as_of=as_of,
        isins=frozenset(codes),
    )


def synthetic_evidence(**overrides) -> eligibility.Evidence:
    """An Evidence built by hand, so one tier boundary can be isolated."""
    fields = {
        "sources": {},
        "seed_verified": frozenset(),
        "seed_unverified": frozenset(),
        "prospectus": {},
        "replication": {},
        "not_etf": frozenset(),
        "unflagged": frozenset(),
        "rejected_isins": (),
        "as_of": DAY,
    }
    fields.update(overrides)
    if isinstance(fields["sources"], list):
        fields["sources"] = {s.key: s for s in fields["sources"]}
    return eligibility.Evidence(**fields)


# --------------------------------------------------------------------------- #
# ISO 6166 -- invariant C5
# --------------------------------------------------------------------------- #


def test_check_digit_matches_published_isins():
    for code in (VOO, SPY, AMUNDI_PEA_SP500, ISHARES_CORE_WORLD, XETRA_GOLD, "US0378331005"):
        assert isin.check_digit(code[:-1]) == code[-1], code
        assert isin.is_valid(code)


def test_checksum_invalid_isins_are_rejected():
    # The three that the seed build actually caught in French finance blogs: two
    # transposition typos and a placeholder someone left in an article.
    for code in ("FR0014002CG7", "FR0014002CH5", BAD_CHECKSUM):
        assert not isin.is_valid(code)
        with pytest.raises(InvalidIsinError):
            isin.require(code)


def test_normalisation_accepts_the_shapes_sources_emit():
    assert isin.normalise("ie00 b4l5-y983") == ISHARES_CORE_WORLD
    assert isin.is_valid("ie00 b4l5-y983")
    assert isin.normalise(float("nan")) is None
    assert isin.normalise(None) is None


def test_country_refuses_to_read_a_jurisdiction_out_of_a_supranational_prefix():
    international = "XS123456789" + isin.check_digit("XS123456789")
    assert isin.is_valid(international)
    assert isin.country(international) is None  # XS is not a country
    assert isin.country(VOO) == "US"
    assert isin.country(BAD_CHECKSUM) is None


def test_c5_no_reference_isin_fails_the_check_digit(evidence):
    """The corpus itself: 539 seeded ISINs, zero invalid, as the recon claims."""
    assert evidence.rejected_isins == ()
    indexed = set().union(*(s.isins for s in evidence.sources.values()))
    indexed |= evidence.seed_verified | evidence.seed_unverified | evidence.unflagged
    assert indexed
    assert all(isin.is_valid(code) for code in indexed)


def test_c5_classify_pea_refuses_an_unidentifiable_row(evidence):
    with pytest.raises(InvalidIsinError):
        eligibility.classify_pea(fund(BAD_CHECKSUM), evidence)


def test_c5_classify_all_rejects_rather_than_inserts(evidence):
    frame = universe([fund(AMUNDI_PEA_SP500), fund(BAD_CHECKSUM)])

    with pytest.raises(InvalidIsinError) as raised:
        eligibility.classify_all(frame, evidence)
    assert BAD_CHECKSUM in str(raised.value)

    dropped = eligibility.classify_all(frame, evidence, on_invalid_isin="drop")
    assert list(dropped["isin"]) == [AMUNDI_PEA_SP500]


def test_drop_mode_reports_what_it_dropped(evidence, caplog):
    """A build that loses rows has to say so, or it loses them again next time."""
    frame = universe(
        [fund(AMUNDI_PEA_SP500), fund(BAD_CHECKSUM), fund(None), fund(US_SURROGATE)]
    )
    with caplog.at_level("WARNING", logger="pipeline.eligibility"):
        result = eligibility.classify_all(frame, evidence, on_invalid_isin="drop")

    assert result.attrs["dropped_invalid_isin"] == 2  # the bad checksum, and the
    assert len(result.attrs["dropped_rows"]) == 2  # row with no identifier at all
    assert BAD_CHECKSUM in " ".join(result.attrs["dropped_rows"])
    assert len(result) == 2  # the real ISIN and the surrogate-keyed row survive
    assert caplog.records

    clean = eligibility.classify_all(universe([fund(AMUNDI_PEA_SP500)]), evidence)
    assert clean.attrs["dropped_invalid_isin"] == 0


# --------------------------------------------------------------------------- #
# Rows with no ISIN
#
# CUSIP is a paid licence, so ~58% of the US universe cannot be assigned an ISIN
# and arrives under a `US:{exchange}:{ticker}` surrogate. Those rows are the ones
# Rule 0 answers with the most certainty, and refusing or dropping them would
# quietly delete ~3,250 funds we can classify without an identifier at all.
# --------------------------------------------------------------------------- #


def test_an_isin_less_us_row_is_classified_not_refused(evidence):
    for row in (
        fund(US_SURROGATE, name="Vanguard S&P 500 ETF", domicile="US", ucits=False),
        fund(None, surrogate_key=US_SURROGATE, domicile="US", ucits=False),
        fund(None, key=US_SURROGATE, domicile="US", ucits=False),
    ):
        pea = eligibility.classify_pea(row, evidence)
        cto = eligibility.classify_cto(row, evidence)
        assert pea.eligible is False
        assert pea.reason == "domicile_outside_eea"
        assert cto.accessible is False
        assert cto.reason == "no_priips_kid"
        assert cto.has_priips_kid is False


def test_an_isin_less_non_us_row_reaches_the_null_path(evidence):
    """No ISIN means no evidence can match, which is unknown -- never an error."""
    row = fund(None, surrogate_key="XX:XLON:ABC", domicile="IE", ucits=True)
    pea = eligibility.classify_pea(row, evidence)
    cto = eligibility.classify_cto(row, evidence)

    assert pea.eligible is None
    assert pea.eligible is not False
    assert pea.confidence == "none"
    assert pea.reason == "no_isin_to_match"
    assert cto.accessible is True  # an EEA UCITS is still sellable in France

    unknown_domicile = fund(None, surrogate_key="XX:XLON:ABC")
    assert eligibility.classify_pea(unknown_domicile, evidence).eligible is None
    assert eligibility.classify_cto(unknown_domicile, evidence).accessible is None


def test_a_surrogate_key_is_never_mistaken_for_an_isin(evidence):
    """Tickers carry dots and hyphens that ISIN normalisation would eat."""
    for key in ("US:XNAS:BRK.B", "US:ARCX:RDS-A", "US:XNYS:SPY"):
        row = fund(key, domicile="US")
        assert eligibility.classify_pea(row, evidence).eligible is False
    assert eligibility._SURROGATE_KEY.match(VOO) is None


def test_a_row_with_no_identifier_of_any_kind_is_still_refused(evidence):
    """A row that cannot be joined to anything cannot be stored either."""
    with pytest.raises(InvalidIsinError, match="surrogate"):
        eligibility.classify_pea(fund(None), evidence)
    with pytest.raises(InvalidIsinError):
        eligibility.classify_cto(fund(None, domicile="US"), evidence)


def test_classify_all_keeps_isin_less_rows_and_still_enforces_c3(evidence):
    frame = universe(
        [
            fund(US_SURROGATE, domicile="US", ucits=False),
            fund(None, surrogate_key="US:XNAS:QQQ", domicile="US", ucits=False),
            fund(AMUNDI_PEA_SP500, domicile="FR", ucits=True),
        ]
    )
    result = eligibility.classify_all(frame, evidence)

    assert len(result) == 3
    assert result.attrs["dropped_invalid_isin"] == 0
    without_isin = result[result["isin"].isna() | result["isin"].eq(US_SURROGATE)]
    assert len(without_isin) == 2
    assert (without_isin["pea_eligible"] == False).all()  # noqa: E712
    assert (without_isin["cto_accessible"] == False).all()  # noqa: E712
    assert (without_isin["pea_source"] == eligibility.LEGIFRANCE_L221_31).all()


# --------------------------------------------------------------------------- #
# The evidence corpus
# --------------------------------------------------------------------------- #


def test_seed_sections_are_parsed_apart(evidence):
    assert len(evidence.seed_verified) == 250
    assert len(evidence.seed_unverified) == 289
    assert not (evidence.seed_verified & evidence.seed_unverified)


def test_venue_and_issuer_sets_match_the_recon(evidence):
    assert len(evidence.sources["euronext"].isins) == 154
    assert evidence.sources["euronext"].kind == eligibility.VENUE
    assert evidence.sources["euronext"].as_of == DAY  # stamped inside the export
    assert len(evidence.sources["amundi"].isins) == 115
    assert len(evidence.sources["ishares"].isins) == 36
    assert len(evidence.sources["bnp-paribas-easy"].isins) == 24


def test_one_broker_publishing_two_lists_is_still_one_broker():
    """Bourse Direct and Boursorama each appear under two different URLs.

    Counting URLs instead of providers would manufacture "two independent
    brokers" out of one opinion and promote a `low` row to `medium`.
    """
    assert (
        eligibility._source_key(
            "https://www.boursedirect.fr/api/instrument/v3/search?nature=tracker&pea=true"
        )
        == eligibility._source_key("https://www.boursedirect.fr/fr/etf/offres/amundi-etf")
    )
    assert (
        eligibility._source_key(
            "https://www.boursorama.com/bourse/trackers/recherche/"
            "?beginnerEtfSearch%5Beligibility%5D%5B0%5D=taxation"
        )
        == eligibility._source_key(
            "https://www.boursorama.com/bourse/trackers/recherche/autres/"
            "?beginnerEtfSearch%5BisEtf%5D=1&beginnerEtfSearch%5Btaxation%5D=1"
        )
    )


def test_an_undeclared_source_cannot_vote():
    """A URL nobody has classified falls through to community, which never promotes."""
    key = eligibility._source_key("https://some-new-screener.example/etf-pea")
    assert eligibility._kind_of(key) not in eligibility.PROMOTING_KINDS


def test_the_stale_saxo_pdf_is_not_a_flag_source(evidence):
    """Seventeen months is long enough for a fund to have breached the 75% quota."""
    assert eligibility._KIND["saxo"] == eligibility.STALE
    assert evidence.sources["saxo"].as_of == date(2025, 3, 20)

    stale_only = "LU1681043599"
    verdict = eligibility.classify_pea(
        fund(stale_only),
        synthetic_evidence(sources=[source("saxo", eligibility.STALE, [stale_only])]),
    )
    assert verdict.eligible is None
    assert verdict.confidence == "hint"


# --------------------------------------------------------------------------- #
# The headline case: a synthetic tracker of a US index inside a PEA
# --------------------------------------------------------------------------- #


def test_synthetic_sp500_pea_etf_is_eligible(evidence):
    verdict = eligibility.classify_pea(
        fund(
            AMUNDI_PEA_SP500,
            name="Amundi PEA S&P 500 UCITS ETF Acc",
            domicile="FR",
            ucits=True,
            index_name="S&P 500 Net Total Return",
        ),
        evidence,
    )
    assert verdict.eligible is True
    assert verdict.confidence == "high"
    assert verdict.reason == "venue_flag"
    assert verdict.mechanism == "synthetic_swap"
    assert verdict.source.startswith("https://live.euronext.com")
    assert verdict.as_of == DAY


def test_the_swap_wrapper_of_the_same_index_is_eligible_too(evidence):
    verdict = eligibility.classify_pea(
        fund(
            ISHARES_SP500_SWAP_PEA,
            name="iShares S&P 500 Swap PEA UCITS ETF EUR (Acc)",
            domicile="IE",
            ucits=True,
        ),
        evidence,
    )
    assert verdict.eligible is True
    assert verdict.mechanism == "synthetic_swap"


def test_the_physical_twin_of_the_same_index_is_null_not_true(evidence):
    """Same benchmark, opposite answer. This is the pair the whole module exists for."""
    physical = eligibility.classify_pea(
        fund(
            ISHARES_CORE_SP500,
            name="iShares Core S&P 500 UCITS ETF",
            domicile="IE",
            ucits=True,
            replication="physical-full",
            index_name="S&P 500",
        ),
        evidence,
    )
    synthetic = eligibility.classify_pea(
        fund(AMUNDI_PEA_SP500, index_name="S&P 500"), evidence
    )

    assert synthetic.eligible is True
    assert physical.eligible is None
    assert physical.eligible is not False  # we did not establish "no", only "unknown"


# --------------------------------------------------------------------------- #
# Invariant C2 -- the tracked index is not an input
# --------------------------------------------------------------------------- #

_BENCHMARK_FIELDS = {
    "index",
    "indexes",
    "indices",
    "index_name",
    "index_tracked",
    "index_provider",
    "tracked_index",
    "benchmark",
    "benchmark_name",
    "underlying_index",
}


def _docstring_ids(tree: ast.AST) -> set[int]:
    holders = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
    found: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, holders):
            continue
        body = getattr(node, "body", [])
        first = body[0] if body else None
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            found.add(id(first.value))
    return found


def test_c2_the_classifier_never_reads_the_tracked_index():
    """Statically: no identifier or literal in the module names a benchmark field.

    Asserted over the whole module rather than over the True branches alone. The
    stronger form is the maintainable one -- "is this expression reachable from a
    branch that returns True?" is a question a future reader will get wrong, and
    getting it wrong here inverts the verdict for the most popular funds in the
    database. Docstrings and comments are exempt: they have to be able to explain
    the rule.
    """
    tree = ast.parse(Path(inspect.getfile(eligibility)).read_text(encoding="utf-8"))
    exempt = _docstring_ids(tree)

    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id.lower() in _BENCHMARK_FIELDS:
            offenders.append(f"name {node.id!r} on line {node.lineno}")
        elif isinstance(node, ast.Attribute) and node.attr.lower() in _BENCHMARK_FIELDS:
            offenders.append(f"attribute .{node.attr} on line {node.lineno}")
        elif isinstance(node, ast.keyword) and (node.arg or "").lower() in _BENCHMARK_FIELDS:
            offenders.append(f"keyword {node.arg!r} on line {node.lineno}")
        elif isinstance(node, ast.arg) and node.arg.lower() in _BENCHMARK_FIELDS:
            offenders.append(f"argument {node.arg!r} on line {node.lineno}")
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in exempt
            and node.value.strip().lower() in _BENCHMARK_FIELDS
        ):
            offenders.append(f"literal {node.value!r} on line {node.lineno}")

    assert not offenders, "C2 violated: " + "; ".join(offenders)


def test_c2_the_verdict_does_not_move_when_the_benchmark_does(evidence):
    """Behaviourally: sweep the benchmark fields, on an eligible and a null fund."""
    benchmarks = [
        None,
        "",
        "S&P 500",
        "MSCI World",
        "Nasdaq-100",
        "CAC 40",
        "EURO STOXX 50",
        "MSCI Emerging Markets",
    ]
    for code in (AMUNDI_PEA_SP500, ISHARES_CORE_SP500, ISHARES_CORE_WORLD, UNFLAGGED_ONLY):
        verdicts = {
            eligibility.classify_pea(
                fund(
                    code,
                    domicile="FR",
                    ucits=True,
                    index_name=benchmark,
                    index_provider=benchmark,
                ),
                evidence,
            )
            for benchmark in benchmarks
        }
        assert len(verdicts) == 1, f"{code}: benchmark changed the verdict"


def test_c2_an_eu_heavy_benchmark_does_not_promote_an_unevidenced_fund(evidence):
    """Rule 2b, deliberately not implemented: a CAC 40 tracker still has to be evidenced."""
    verdict = eligibility.classify_pea(
        fund(
            ISHARES_CORE_WORLD,
            name="Some CAC 40 UCITS ETF",
            domicile="FR",
            ucits=True,
            replication="physical-full",
            index_name="CAC 40",
        ),
        evidence,
    )
    assert verdict.eligible is None


# --------------------------------------------------------------------------- #
# Rule 0 -- the only branch allowed to say False
# --------------------------------------------------------------------------- #


def test_c3_us_domiciled_funds_fail_both_wrappers(evidence):
    for code in (VOO, SPY):
        row = fund(code, name="Vanguard S&P 500 ETF", domicile="US", ucits=False)
        pea = eligibility.classify_pea(row, evidence)
        cto = eligibility.classify_cto(row, evidence)

        assert pea.eligible is False
        assert pea.reason == "domicile_outside_eea"
        assert cto.accessible is False
        assert cto.reason == "no_priips_kid"
        assert cto.has_priips_kid is False


def test_c3_holds_without_a_declared_domicile(evidence):
    """A half-populated universe row must not let a 40-Act ETF through.

    The ISIN prefix is the fallback, and it is the only thing between an adapter
    that forgot to fill `domicile` and VOO being offered to a French holder.
    """
    pea = eligibility.classify_pea(fund(VOO), evidence)
    cto = eligibility.classify_cto(fund(VOO), evidence)
    assert pea.eligible is False
    assert cto.accessible is False


def test_c3_is_enforced_on_the_whole_frame(evidence):
    classified = eligibility.classify_all(
        universe([fund(VOO, domicile="US"), fund(AMUNDI_PEA_SP500, domicile="FR")]),
        evidence,
    )
    us_row = classified[classified["isin"] == VOO].iloc[0]
    assert bool(us_row["pea_eligible"]) is False
    assert bool(us_row["cto_accessible"]) is False
    assert not pd.isna(us_row["pea_eligible"])  # decided, not merely unknown
    assert not pd.isna(us_row["cto_accessible"])


def test_uk_funds_are_third_country_products(evidence):
    row = fund(UK_FUND, domicile="GB", ucits=True)
    assert eligibility.classify_pea(row, evidence).reason == "domicile_outside_eea"

    cto = eligibility.classify_cto(row, evidence)
    assert cto.accessible is False
    assert cto.reason == "uk_ucits_is_third_country"
    assert cto.authorised_fr is False


def test_etc_and_etn_are_not_funds(evidence):
    for row, label in (
        (fund(XETRA_GOLD, name="Xetra-Gold ETC", domicile="DE"), "name"),
        (fund(WISDOMTREE_GOLD, name="WisdomTree Physical Gold", legal_form="ETC"), "form"),
        (fund(XETRA_GOLD, name="Some Exchange Traded Note", domicile="DE"), "phrase"),
    ):
        verdict = eligibility.classify_pea(row, evidence)
        assert verdict.eligible is False, label
        assert verdict.reason == "not_a_fund", label
        assert verdict.source == eligibility.LEGIFRANCE_L221_31
        assert verdict.mechanism == "unknown"


def test_the_legal_form_test_runs_before_the_domicile_test(evidence):
    """An EEA-domiciled ETC is still not an OPCVM, and must say so precisely."""
    verdict = eligibility.classify_pea(
        fund(XETRA_GOLD, name="Xetra-Gold ETC", domicile="DE"), evidence
    )
    assert verdict.reason == "not_a_fund"


def test_an_etc_is_not_barred_from_a_cto(evidence):
    """Not being an OPCVM says nothing about PRIIPs: ETCs publish KIDs and trade."""
    cto = eligibility.classify_cto(
        fund(XETRA_GOLD, name="Xetra-Gold ETC", domicile="DE"), evidence
    )
    assert cto.accessible is not False


def test_an_unknown_domicile_is_not_a_disqualifier(evidence):
    verdict = eligibility.classify_pea(fund(ISHARES_CORE_WORLD), evidence)
    assert verdict.eligible is None
    assert verdict.eligible is not False


def test_a_placeholder_domicile_is_not_read_as_a_jurisdiction(evidence):
    """`XS` in a domicile column is an upstream's placeholder, not a country.

    Taken at face value it is "not in the EEA" and would fail Rule 0, turning a
    sloppy source into a published `False`.
    """
    verdict = eligibility.classify_pea(
        fund(ISHARES_CORE_WORLD, domicile="XS", ucits=True), evidence
    )
    assert verdict.eligible is None
    assert verdict.eligible is not False


# --------------------------------------------------------------------------- #
# Rule 2 -- negative inference is forbidden
# --------------------------------------------------------------------------- #


def test_a_fund_absent_from_every_list_is_null_not_false(evidence):
    verdict = eligibility.classify_pea(
        fund(ISHARES_CORE_WORLD, name="iShares Core MSCI World UCITS ETF",
             domicile="IE", ucits=True),
        evidence,
    )
    assert verdict.eligible is None
    assert verdict.eligible is not False
    assert verdict.confidence == "none"
    assert verdict.reason == "no_positive_evidence"
    assert verdict.mechanism == "unknown"


def test_a_catalogue_without_a_pea_flag_is_not_evidence_either_way(evidence):
    """Xtrackers publishes 428 products and no PEA flag, confirmed four ways.

    Seeing an ISIN there must not read as either answer.
    """
    assert UNFLAGGED_ONLY in evidence.unflagged
    verdict = eligibility.classify_pea(fund(UNFLAGGED_ONLY, domicile="DE"), evidence)
    assert verdict.eligible is None
    assert verdict.confidence == "none"


def test_absence_from_a_broker_catalogue_is_about_the_broker(evidence):
    cto = eligibility.classify_cto(
        fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True),
        evidence,
        broker="bourse-direct",
    )
    assert cto.accessible is None
    assert cto.accessible is not False
    assert cto.reason == "not_in_broker_catalogue"


def test_a_broker_we_hold_no_catalogue_for_falls_back_to_the_fund_level_answer(evidence):
    cto = eligibility.classify_cto(
        fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True),
        evidence,
        broker="trade-republic",
    )
    assert cto.accessible is True  # the fund-level question, answered honestly
    assert "trade-republic" in cto.note


# --------------------------------------------------------------------------- #
# Confidence tiers
# --------------------------------------------------------------------------- #


def test_tiers_are_ranked_by_evidence_quality():
    code = ISHARES_CORE_WORLD
    tiers = {
        "high": [source("euronext", eligibility.VENUE, [code])],
        "medium": [
            source("bourse-direct", eligibility.BROKER, [code]),
            source("fortuneo", eligibility.BROKER, [code]),
        ],
        "low": [source("bourse-direct", eligibility.BROKER, [code])],
    }
    for expected, sources in tiers.items():
        verdict = eligibility.classify_pea(
            fund(code), synthetic_evidence(sources=sources)
        )
        assert verdict.eligible is True
        assert verdict.confidence == expected

    issuer_only = eligibility.classify_pea(
        fund(code),
        synthetic_evidence(sources=[source("amundi", eligibility.ISSUER, [code])]),
    )
    assert issuer_only.confidence == "high"


def test_a_prospectus_reading_outranks_every_screener():
    code = ISHARES_CORE_WORLD
    document = eligibility.Source(
        key="prospectus",
        kind="prospectus",
        url="https://issuer.example/dic.pdf",
        as_of=date(2026, 3, 1),
        isins=frozenset({code}),
    )
    verdict = eligibility.classify_pea(
        fund(code),
        synthetic_evidence(
            sources=[source("euronext", eligibility.VENUE, [code])],
            prospectus={code: document},
        ),
    )
    assert verdict.confidence == "highest"
    assert verdict.source == "https://issuer.example/dic.pdf"
    assert verdict.as_of == date(2026, 3, 1)


def test_screeners_and_blogs_never_promote_on_their_own():
    code = ISHARES_CORE_WORLD
    for kind in (eligibility.SCREENER, eligibility.COMMUNITY):
        verdict = eligibility.classify_pea(
            fund(code), synthetic_evidence(sources=[source("x", kind, [code])])
        )
        assert verdict.eligible is None
        assert verdict.confidence == "hint"
        assert verdict.reason == "screener_or_community_only"


def test_two_independent_brokers_beat_one(evidence):
    """On the real corpus: the only two ISINs whose sole evidence is two brokers."""
    verdict = eligibility.classify_pea(fund(TWO_BROKERS), evidence)
    assert verdict.eligible is True
    assert verdict.confidence == "medium"
    assert verdict.reason == "brokers_corroborated"


def test_the_two_seed_sections_land_in_different_tiers(evidence):
    verified = eligibility.classify_pea(fund(SEED_VERIFIED_ONLY), evidence)
    unverified = eligibility.classify_pea(fund(SEED_UNVERIFIED_ONLY), evidence)

    assert verified.eligible is True
    assert verified.confidence == "low"
    assert verified.reason == "seed_verified"
    assert verified.source == eligibility.SEED_VERIFIED_CITATION

    assert unverified.eligible is None
    assert unverified.confidence == "hint"
    assert unverified.reason == "seed_unverified"
    assert unverified.source == eligibility.SEED_UNVERIFIED_CITATION

    assert verified.confidence != unverified.confidence


def test_the_unverified_section_never_produces_a_true_on_its_own(evidence):
    """Not one of the 285 unverified-only ISINs may be published as eligible."""
    promoting = set()
    for candidate in evidence.sources.values():
        if candidate.kind in eligibility.PROMOTING_KINDS:
            promoting |= candidate.isins

    alone = sorted(evidence.seed_unverified - promoting - evidence.seed_verified)
    assert len(alone) > 200  # the section is mostly a low-quality GitHub list
    for code in alone:
        verdict = eligibility.classify_pea(fund(code), evidence)
        assert verdict.eligible is None, code
        assert verdict.confidence == "hint", code


def test_a_promoting_source_still_wins_over_the_unverified_marking(evidence):
    """The unverified section is not counter-evidence -- only Rule 0 may demote."""
    both = evidence.seed_unverified & evidence.sources["euronext"].isins
    both |= evidence.seed_unverified & evidence.sources["bourse-direct"].isins
    assert both, "no unverified ISIN is independently flagged; test proves nothing"
    for code in sorted(both):
        verdict = eligibility.classify_pea(fund(code), evidence)
        assert verdict.eligible is True, code
        assert verdict.reason != "seed_unverified", code


# --------------------------------------------------------------------------- #
# Mechanism and the synthetic-swap political risk
# --------------------------------------------------------------------------- #


def test_mechanism_comes_from_replication_not_from_the_benchmark(evidence):
    swap = eligibility.mechanism_of(
        fund(AMUNDI_PEA_SP500, replication="synthetic-swap"), evidence
    )
    physical = eligibility.mechanism_of(
        fund(AMUNDI_PEA_SP500, replication="physical-full"), evidence
    )
    named = eligibility.mechanism_of(
        fund(ISHARES_SP500_SWAP_PEA, name="iShares S&P 500 Swap PEA UCITS ETF"), evidence
    )
    unknown = eligibility.mechanism_of(fund(ISHARES_CORE_WORLD), evidence)

    assert swap == "synthetic_swap"
    assert physical == "physical_eu"
    assert named == "synthetic_swap"
    assert unknown == "unknown"
    assert set(schema.PEA_MECHANISM) >= {swap, physical, named, unknown}


def test_a_source_stated_replication_fills_in_for_a_silent_universe_row(evidence):
    """The seed says FR0011871128 is swap-based; the universe row may not know yet."""
    assert evidence.replication[AMUNDI_PEA_SP500] == "synthetic-swap"
    assert eligibility.mechanism_of(fund(AMUNDI_PEA_SP500), evidence) == "synthetic_swap"


def test_every_synthetic_row_is_reachable_in_one_predicate(evidence):
    """Sénat QE n° 08783 is unanswered; this mask is the blast radius if it lands."""
    classified = eligibility.classify_all(
        universe(
            [
                fund(AMUNDI_PEA_SP500),
                fund(ISHARES_SP500_SWAP_PEA),
                fund(SEED_VERIFIED_ONLY),
                fund(ISHARES_CORE_WORLD),
            ]
        ),
        evidence,
    )
    at_risk = eligibility.synthetic_swap_mask(classified)
    assert set(classified.loc[at_risk, "isin"]) == {AMUNDI_PEA_SP500, ISHARES_SP500_SWAP_PEA}

    # The re-flag itself: one assignment, no re-derivation.
    classified.loc[at_risk, "pea_eligible"] = pd.NA
    assert classified.loc[at_risk, "pea_eligible"].isna().all()


# --------------------------------------------------------------------------- #
# CTO: three facts, kept apart
# --------------------------------------------------------------------------- #


def test_cto_keeps_the_kid_and_the_passport_as_separate_columns(evidence):
    row = fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True)
    cto = eligibility.classify_cto(row, evidence)
    assert cto.accessible is True
    assert cto.reason == "ucits_eea_with_kid"
    assert cto.has_priips_kid is True
    assert cto.authorised_fr is None  # GECO not consulted; not silently asserted


def test_a_missing_french_passport_blocks_a_fund_that_has_a_kid(evidence):
    cto = eligibility.classify_cto(
        fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True, authorised_fr=False),
        evidence,
    )
    assert cto.accessible is False
    assert cto.reason == "not_passported_to_france"
    assert cto.has_priips_kid is True
    assert cto.authorised_fr is False


def test_an_unconfirmed_kid_language_yields_unknown_not_false(evidence):
    cto = eligibility.classify_cto(
        fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True, kid_language_fr=False),
        evidence,
    )
    assert cto.accessible is None
    assert cto.reason == "kid_language_unconfirmed"


def test_a_non_eea_domicile_outside_the_us_and_uk_is_unknown_not_false(evidence):
    """Many Channel Islands ETCs publish a French KID and trade on Euronext Paris."""
    cto = eligibility.classify_cto(
        fund(WISDOMTREE_GOLD, name="WisdomTree Physical Gold", domicile="JE"), evidence
    )
    assert cto.accessible is None
    assert cto.reason == "unknown"


def test_being_in_a_broker_catalogue_answers_the_broker_question(evidence):
    cto = eligibility.classify_cto(fund(TWO_BROKERS), evidence, broker="fortuneo")
    assert cto.accessible is True
    assert cto.reason == "in_broker_catalogue"
    assert schema.CTO_REASON.count(cto.reason) == 1


def test_every_cto_reason_is_in_the_declared_vocabulary(evidence):
    rows = [
        fund(VOO, domicile="US"),
        fund(UK_FUND, domicile="GB"),
        fund(WISDOMTREE_GOLD, domicile="JE"),
        fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True),
        fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True, authorised_fr=False),
        fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True, kid_language_fr=False),
        fund(AMUNDI_PEA_SP500),
        fund(ISHARES_CORE_WORLD),
    ]
    for row in rows:
        for broker in (None, "fortuneo", "bourse-direct"):
            verdict = eligibility.classify_cto(row, evidence, broker=broker)
            assert verdict.reason in schema.CTO_REASON, (row["isin"], broker)


# --------------------------------------------------------------------------- #
# Invariants C1 and C4 over a whole frame
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def classified(evidence) -> pd.DataFrame:
    """A mixed frame: evidenced, disqualified, hinted and entirely unknown funds."""
    rows = [
        fund(AMUNDI_PEA_SP500, name="Amundi PEA S&P 500", domicile="FR", ucits=True),
        fund(ISHARES_SP500_SWAP_PEA, domicile="IE", ucits=True),
        fund(BNP_SP500, domicile="FR", ucits=True),
        fund(TWO_BROKERS, domicile="IE", ucits=True),
        fund(SEED_VERIFIED_ONLY, domicile="DE"),
        fund(SEED_UNVERIFIED_ONLY, domicile="BG"),
        fund(UNFLAGGED_ONLY, domicile="DE"),
        fund(ISHARES_CORE_SP500, domicile="IE", ucits=True),
        fund(ISHARES_CORE_WORLD, domicile="IE", ucits=True),
        fund(VOO, domicile="US", ucits=False),
        fund(SPY, domicile="US", ucits=False),
        fund(UK_FUND, domicile="GB", ucits=True),
        fund(XETRA_GOLD, name="Xetra-Gold ETC", domicile="DE"),
    ]
    return eligibility.classify_all(universe(rows), evidence)


def test_c1_pea_eligible_implies_cto_accessible(classified):
    eligible = classified["pea_eligible"].eq(True).fillna(False)
    assert eligible.any()
    assert (classified.loc[eligible, "cto_accessible"] == True).all()  # noqa: E712


def test_c1_is_enforced_at_runtime_not_merely_hoped_for(evidence):
    """A contradiction between two sources must fail the build, not ship quietly.

    Nothing populates `authorised_fr` today, so this can only be produced by hand
    -- but the day a GECO adapter says "not marketed in France" about a fund a
    French broker sells inside a PEA, somebody has to look at it.
    """
    frame = universe([fund(AMUNDI_PEA_SP500, domicile="FR", ucits=True, authorised_fr=False)])
    with pytest.raises(ValueError, match="C1 violated"):
        eligibility.classify_all(frame, evidence)


def test_c4_every_decided_row_is_attributed_and_dated(classified):
    decided = classified[classified["pea_eligible"].notna()]
    assert len(decided) >= 6
    assert decided["pea_source"].notna().all()
    assert decided["pea_as_of"].notna().all()
    assert all(isinstance(value, date) for value in decided["pea_as_of"])

    # And the converse: an undecided row is not dressed up with a date.
    undecided = classified[classified["pea_eligible"].isna()]
    assert undecided["pea_as_of"].isna().all()


def test_c4_a_false_verdict_cites_the_statute(classified):
    refused = classified[classified["pea_eligible"].eq(False).fillna(False)]
    assert len(refused) >= 3
    assert (refused["pea_source"] == eligibility.LEGIFRANCE_L221_31).all()


def test_classify_all_fills_the_declared_columns_with_storable_types(classified):
    for column in eligibility.ELIGIBILITY_COLUMNS:
        assert column in classified.columns

    assert classified["pea_eligible"].dtype == "boolean"
    assert classified["cto_accessible"].dtype == "boolean"
    assert classified["has_priips_kid"].dtype == "boolean"
    assert set(classified["pea_confidence"]) <= set(schema.PEA_CONFIDENCE)
    assert set(classified["pea_mechanism"]) <= set(schema.PEA_MECHANISM)
    assert set(classified["cto_reason"]) <= set(schema.CTO_REASON)

    stored = schema.conform(pa.Table.from_pandas(classified, preserve_index=False), "funds")
    assert stored.schema.equals(schema.FUNDS)
    assert stored.num_rows == len(classified)


def test_classify_all_does_not_mutate_its_input(evidence):
    frame = universe([fund(AMUNDI_PEA_SP500)])
    before = frame.copy()
    eligibility.classify_all(frame, evidence)
    pd.testing.assert_frame_equal(frame, before)


# --------------------------------------------------------------------------- #
# broker_availability
# --------------------------------------------------------------------------- #


def test_broker_availability_conforms_to_the_schema(evidence):
    table = eligibility.broker_availability(evidence)
    assert table.schema.equals(schema.BROKER_AVAILABILITY)
    assert table.num_rows > 400

    frame = table.to_pandas()
    assert frame["available"].all()  # never a False built out of absence
    assert set(frame["wrapper"]) == {"pea"}
    assert set(frame["broker"]) == {"bourse-direct", "boursorama", "fortuneo", "saxo"}
    assert frame["source_url"].notna().all()

    # Each catalogue keeps its own age: a live API and a PDF from March 2025 must
    # not be presented as equally current.
    ages = frame.groupby("broker")["as_of"].max().to_dict()
    assert ages["saxo"] == date(2025, 3, 20)
    assert ages["bourse-direct"] == DAY


def test_broker_availability_leaves_out_the_known_non_etfs(evidence):
    frame = eligibility.broker_availability(evidence).to_pandas()
    assert evidence.not_etf
    assert not set(frame["isin"]) & evidence.not_etf
    # The dropped rows are genuinely PEA-eligible, just not ETFs -- so they are
    # out of scope, never marked ineligible.
    for code in sorted(evidence.not_etf)[:20]:
        assert eligibility.classify_pea(fund(code), evidence).eligible is not False
