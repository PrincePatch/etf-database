# etf-database

An open database of worldwide ETFs — performance statistics, and whether each
one can actually be held in a French **PEA** or **CTO**.

Built for a French retail investor, which drives three decisions that make this
different from a generic ETF screener:

- **Everything is in euros.** A fund that gained 10% in dollars while the dollar
  fell 10% gained nothing for a euro-based holder. Reporting the local-currency
  figure would be the wrong number.
- **Total return, not price return.** Returns are computed on a
  distribution-adjusted series, so a distributing ETF is comparable with its
  accumulating twin instead of looking structurally worse.
- **Eligibility carries its evidence.** `pea_eligible` is `true`, `false`, or
  `null` — never a guess dressed up as a fact.

## Status

Live at **https://princepatch.github.io/etf-database/**, refreshed on a
schedule. The pipeline runs end to end.

| | |
| --- | --- |
| Funds | 11,296 |
| Listings | 24,428 |
| Funds with price history | 11,195 |
| Weekly index points | 3.3 M across 10,938 series |
| PEA: established / excluded / undetermined | 251 / 6,227 / 4,818 |
| Published payload | 26 MB |

Everything from the schema through the scheduled refresh is built and tested.
What is not finished is *coverage*, and it is worth being specific about where
the holes are rather than letting the totals imply completeness:

- **PEA eligibility is thin by construction** — 251 funds established against
  4,818 undetermined. See below; the fix is more first-party sources, not looser
  rules.
- **3,543 funds have no asset class.** Mostly single-stock leveraged products
  named after a ticker rather than an asset (`Tradr 2X Long UPST`), plus names
  the source truncated before the deciding word. The classifier leaves these
  null rather than guessing, since a wrong class silently returns the wrong set
  to anyone filtering on it.
- **101 funds have no price history**, generally because no venue we know of
  publishes a symbol we can resolve.
- **Two PEA-eligible ETFs were dropped as untradable** — an Amundi inverse
  EuroStoxx 50 and a BNP ESG Eurozone share class. Both are real funds; we
  simply have one listing each, with no ticker and no price bar, so nothing
  could be shown about them. Listing coverage, not the rule, is what is
  missing.
- **Only Chromium has been tested.** Firefox and Safari are unverified.

## The PEA question

There is no authoritative public list of PEA-eligible ETFs. The AMF holds a
per-fund commitment from each issuer and publishes no register, and Décret
2026-189 confirms none is planned. Ground truth for any single fund is the
"éligibilité PEA" line in its prospectus — a per-document read.

So eligibility is assembled from positive evidence of uneven quality, and the
quality is stored next to the answer. The three best public screeners agree on
only 88 of 243 contested ISINs; publishing a bare boolean would present a coin
flip and a prospectus reading as the same fact.

Two rules follow from the fact that the error cost is asymmetric — an ineligible
fund held in a PEA can force the plan closed and lose its tax clock, while
missing an eligible one merely means not buying it:

- **Positive evidence only.** Eligibility is never inferred from the index a
  fund tracks. A *synthetic* swap-based UCITS ETF on the S&P 500 **is**
  PEA-eligible; a physical one on the same index is not. Judging by the index
  inverts the answer for the most popular funds in the database.
- **No negative inference.** A fund's absence from a broker's list means that
  broker does not offer it, not that it is ineligible. Only structural
  disqualifiers — an ETC or ETN rather than a fund, or a domicile outside the
  EEA — produce `false`. Everything else unproven stays `null`.

The practical consequence is that **the PEA column is sparse and will stay
sparse** until more first-party sources are folded in. Expect a large majority
of `null`. That is the honest state of the evidence, not a bug, and it will not
be improved by loosening the rules.

`reference/PEA_CTO_RULES.md` documents the full decision procedure and its
sources. `reference/pea_eligible.csv` holds the seeded ISINs; every one was read
out of a fetched document and checked against the ISO 6166 check digit rather
than typed from memory.

## Data model

Seven tables, defined once in `pipeline/schema.py`, which is the single source
of truth — if a column is not declared there, it does not exist.

| Table | Grain | Holds |
| --- | --- | --- |
| `funds` | one row per ISIN | identity, costs, structure, PEA/CTO flags |
| `listings` | one row per (ISIN, MIC) | tickers, venues, quote currencies |
| `prices` | one row per (ISIN, date) | raw OHLCV plus a computed adjusted close |
| `corporate_actions` | one row per event | dividends and splits |
| `performance` | one row per ISIN | precomputed trailing returns, risk, drawdowns |
| `returns_yearly` / `returns_monthly` | calendar periods | per-year and per-month returns |
| `broker_availability` | one row per (broker, ISIN) | what each broker actually lists |

A fund and a listing are deliberately separate. One ISIN is usually listed on
several exchanges under different tickers — IE00B4L5Y983 trades as EUNL on
Xetra, IWDA in Amsterdam and SWDA in Milan. Statistics belong to the fund;
tickers, venues and currencies belong to the listing. A bare ticker is never a
key.

## Sources and what this repository publishes

Chosen for licence as much as for coverage.

Most of the pipeline draws on sources that invite reuse: ESMA FIRDS is public
regulatory filing data requiring only attribution, GLEIF is CC0, and the ECB
publishes its reference rates for exactly this purpose. The venue directories
are published for public consultation.

Two sources are not in that category, and the position taken here is deliberate
rather than convenient. **justETF is not used at all**: its robots.txt disallows
the endpoints its own screener calls, and its terms separately prohibit
"programs to carry out automated price inquiries" — an explicit contractual ban,
not merely a crawler preference. **Yahoo Finance is used, but its data is not
redistributed.** The chart host it serves from carries a blanket robots.txt
disallow, which is the same posture justETF takes, so treating the two
differently on coverage grounds alone would be a double standard.

The line drawn instead is between using data and republishing it:

- **Computed here, published in full** — performance statistics, calendar-period
  returns, and a weekly total-return index rebased to 100 in EUR. These are this
  project's own reconstructions, derived from raw bars combined with our
  dividend adjustment and ECB conversion. Facts about funds, not a copy of a
  feed.
- **Used here, never published** — raw daily OHLCV. No open, high, low, close or
  volume series appear in `docs/data/`. `data/processed/prices/` is local-only
  and git-ignored.

A weekly subset of raw closes would still be raw closes; a rebased index is a
transformation. That distinction is the whole policy, and `pipeline/export.py`
enforces it.

| Source | Contributes |
| --- | --- |

| Source | Contributes |
| --- | --- |
| ESMA FIRDS | the EU instrument backbone, ~7,800 ETF ISINs |
| Euronext, Xetra, LSE, SIX | listings, tickers, and TER |
| Nasdaq Trader symbol directory | ~5,600 US ETFs |
| OpenFIGI, GLEIF | identifier and issuer resolution |
| ECB reference rates | daily FX back to 1999 |
| Yahoo Finance | daily bars and corporate actions |

`reference/UNIVERSE_SOURCES.md` and `reference/PRICE_SOURCES.md` record what was
verified, what was measured, and what turned out to be a dead end.

## Two upstream defects worth knowing about

Both were found by measurement, and both would have silently corrupted the
database rather than failing loudly.

**The ECB's `eurofxref-hist.csv` is stale.** It serves a truncated file whose
last real observation is 2010-02-10, followed by placeholder rows carrying the
sequence 1..38, one of them dated a Sunday. The `.zip` of the nominally
identical file, on the same host, is complete and current. The loader validates
structural invariants — no weekend rows, no non-positive rates — so a repeat of
this fails the build instead of rewriting every figure in the database.

**Yahoo's adjusted close is wrong for some listings.** It publishes 40 dividend
events for `ISF.L` and applies none of them, understating that fund's 10-year
annualised return by 4.03 points — roughly 48% cumulative. The same fund in
Amsterdam and Milan is adjusted correctly, so it is a per-listing defect that
cannot be detected without recomputing. This project therefore stores raw bars
plus events and reconstructs the total-return series itself; the reconstruction
reproduces the vendor's figure to the basis point wherever the vendor is right.

## Layout

```
pipeline/
  schema.py       canonical table definitions — the data contract
  config.py       paths, base currency, risk-free rate, benchmark
  fx.py           ECB rates and conversion to EUR
  stats.py        performance, risk and calendar-period returns
  eligibility.py  the PEA / CTO classifier
  isin.py         ISO 6166 validation
  sources/        one adapter per upstream
reference/        seeded evidence and the reconnaissance reports
data/             raw cache (ignored) and processed tables
docs/             the static site
tests/
```

## Running it

```bash
python -m pip install -r requirements.txt
python -m pytest tests/ -q
```

## Licence and disclaimer

Code under MIT. The assembled data carries the terms of its upstreams; ESMA
FIRDS requires attribution and GLEIF is CC0.

**This is not investment advice, and the PEA and CTO flags are not a
substitute for checking a fund's prospectus.** Eligibility can lapse silently
between a fund's semi-annual reports, and the consequences fall on the holder.
Verify before you buy.
