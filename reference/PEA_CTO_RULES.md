# Recon: PEA eligibility & CTO accessibility for a French retail ETF database

**Repo:** PrincePatch/etf-database · **Date of research:** 2026-08-10
**Scope:** how to flag every ETF as (a) `pea_eligible` and (b) `cto_accessible` for a French retail investor.

Legend used throughout: **[V]** = I fetched the source and read it · **[S]** = read from a search-engine
snippet of the cited page because the fetch was blocked · **[G]** = general knowledge, unverified.

---

## PART 1 — THE RULES

### 1.1 PEA eligibility: the statutory test

The controlling text is **article L221-31 du Code monétaire et financier**.
[V] Légifrance, version **en vigueur depuis le 16 février 2025**, last modified by
**LOI n° 2025-127 du 14 février 2025 (loi de finances pour 2025), art. 92 et 93**.
<https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000049720217>

Two layers matter:

**Layer 1 — direct securities (I.-1°).** Eligible: shares (*actions*, excluding preference shares of
art. L228-11 C. com.), investment certificates, SARL parts, and preferential subscription rights, issued
by companies **having their registered office in the EU or in an EEA State** that has signed a tax
administrative-assistance convention with France (EU-27 + Norway, Iceland, Liechtenstein), and subject
to corporate income tax or an equivalent.

**Layer 2 — collective vehicles (I.-2°) — this is the ETF test.** SICAV, FCP, and
**"OPCVM établis dans d'autres États membres de l'UE ou de l'EEE"** are eligible **if and only if they
"emploient plus de 75 % de leurs actifs en titres mentionnés aux a et b du 1°"** — i.e. **more than 75%
of the fund's assets are invested in EU/EEA-headquartered equities.**

Critical framing for the database schema: **the 75% test is on the fund's balance sheet, not on the
index it tracks.** Nothing in L221-31 mentions the benchmark. This single fact is the whole mechanism
described in §1.2.

[V] BOFiP **BOI-RPPM-RCM-40-50-20-20** confirms the quota must be respected **on a permanent basis**
("de manière permanente"), and that a breach by the fund is a ground for **closure of the plan**
(BOI-RPPM-RCM-40-50-50). <https://bofip.impots.gouv.fr/bofip/1556-PGP.html>

### 1.2 How a synthetic ETF tracks the S&P 500 / MSCI World inside a PEA

The mechanism, precisely:

1. The fund **physically holds a basket of EU/EEA equities** (the *panier de substitution* — typically
   large liquid Eurozone names). This basket is what satisfies the **>75% L221-31 I-2° quota**.
2. The fund enters an **OTC total return swap** with one or more investment-bank counterparties:
   it **pays away the performance of the substitute basket** and **receives the performance of the
   target index** (S&P 500, Nasdaq-100, MSCI World, TOPIX, MSCI India…).
3. The investor's economic exposure is the target index; the fund's *legal* asset composition remains
   >75% European. The tax test looks at (3)'s numerator, so the fund is PEA-eligible.

**Counterparty risk is capped by UCITS law, and the commonly-repeated "swaps cannot exceed 10% of net
assets" is wrong.** [V] The real rule is **Directive 2009/65/EC (UCITS), Article 52(1), 2nd subpara.**,
consolidated text 02009L0065-20210802, quoted verbatim:

> "The risk exposure to a counterparty of the UCITS in an OTC derivative transaction shall not exceed
> either: (a) 10 % of its assets when the counterparty is a credit institution referred to in
> Article 50(1)(f); or (b) 5 % of its assets, in other cases."

That is a cap on the **net mark-to-market exposure** to a counterparty, not on the swap's notional. In
practice issuers reset/collateralise the swap so the exposure sits far below the cap. Art. 52(2) adds
that OTC exposure + securities + deposits with a single body may not exceed 20% of assets.
<https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:02009L0065-20210802>

**Consequence for the data model:** an ETF's `index_tracked` tells you *nothing* about PEA eligibility.
`Amundi PEA S&P 500` (FR0011871128) and `iShares S&P 500 Swap PEA` (IE000DQLYVB9) are PEA-eligible;
`iShares Core S&P 500` (IE00B5BMR087) is not. Same index, opposite answer. **Never infer PEA
eligibility from the benchmark.**

Note also that **domicile is not the test either**: LU- and IE-domiciled funds are eligible (see the
seed CSV — 60+ LU rows, plus IE rows from iShares/Vanguard/HSBC/First Trust), because L221-31 I-2° c)
explicitly admits OPCVM established in *any* EU/EEA Member State. What matters is the **asset quota**.

### 1.3 New in 2026: the declaration decree (this changes the sourcing story)

[V] **Décret n° 2026-189 du 16 mars 2026**, JORF, in force **20 March 2026**.
<https://www.legifrance.gouv.fr/jorf/id/JORFTEXT000053700580>

It moves the PEA reporting obligations out of the CGI (repealing **art. 91 quater L, annexe II CGI**)
and into the CMF as a new **article R. 221-111-2**. Substance:

- A fund wishing its units to be PEA-eligible must lodge an **engagement (commitment) with the AMF
  *before* marketing the securities in France**, undertaking to keep its assets permanently invested in
  the proportions of L221-31 I-2° a) to f).
- The fund must **disclose in its annual and semi-annual reports the actual proportion** of assets held
  in eligible securities over the period, and make those reports available to the tax authorities on
  request.
- The **unit-holder** must be able to produce that engagement document if the administration asks.

**Two implications for this project:**
1. There *is* now a formal, AMF-held artefact that is the ground truth for PEA eligibility. But
   **the decree creates no public register and no attestation system** — verification happens on demand.
   So there is still **no official public list** to scrape.
2. The **fund's annual/semi-annual report is a citable primary source** for a `pea_eligible` flag, and
   the **prospectus/DIC "éligibilité PEA" line** remains the practical per-ISIN proof.

### 1.4 Is anything about to change? (status as of 2026-08-10)

**Nothing has been adopted. Eligibility of synthetic ETFs is intact today.** The live thread:

| Date | Event | Status |
|---|---|---|
| 2025-11-03 | [V] PLF 2026 **amendement n° I-1338** (UDR/Ciotti +15) — multiple PEAs, merge PEA/PEA-PME/PEA-Jeunes, fractional shares, broader eligible universe. <https://www.assemblee-nationale.fr/dyn/17/amendements/1906A/AN/1338> | **Rejeté** |
| 2026-02 | [S] *Le Figaro* reports Bercy is examining synthetic-ETF PEA eligibility; ministry replies the matter *"fait l'objet d'analyses"* | Informal |
| 2026-05-14 | [V] **Question écrite Sénat n° 08783**, sén. **Hervé Maurey** (Eure, UC): extra-European ETFs (Dow Jones, S&P 500, world) inside the PEA channel French savings abroad while collecting the tax break; asks what the government intends to do. <https://www.senat.fr/questions/base/2026/qSEQ260508783.html> | **"En attente de réponse"** — still unanswered on the Sénat site |
| 2026-06-09 | [S] *Les Échos*: the tax administration considers the regulatory criteria met — reported as reassurance, **not** a doctrine change or a published rescrit | Informal |
| 2026-07-10 | [V] Meilleurtaux Placement: no ban; any prohibition would need primary legislation and "the current majority does not appear inclined" to support it. <https://placement.meilleurtaux.com/bourse/actualites/2026-juillet/impacts-interdiction-etf-synthetiques-pea.html> | Eligibility confirmed in practice |

**Also changed for 2026 and relevant to the DB's tax layer:** [S] the **LFSS 2026** raised CSG on
capital income by 1.4 pt (9.2% → 10.6%), taking **social levies on PEA gains from 17.2% to 18.6%**
(CSG 10.6 + CRDS 0.5 + prélèvement de solidarité 7.5). Applies on withdrawal, including to gains
accrued before 2026. Assurance-vie, PEL/CEL and real-estate income stay at 17.2%.

**Earlier reforms, for the record:** loi **PACTE** (2019) liberalised withdrawals (partial withdrawal
after 5 years without closing the plan; PEA-Jeunes created; PEA-PME ceiling raised). **LF 2025 art. 92-93**
excluded warrants / *bons de souscription* / rights other than listed preferential subscription rights,
and excluded securities acquired by employees and executives in consideration of their functions.

**Recommendation for the schema:** store `pea_eligible` **with an `as_of` date and a
`source_url`**, and treat the synthetic-ETF question as an open political risk — add a
`pea_eligibility_mechanism` enum (`physical_eu` | `synthetic_swap` | `unknown`) so that, if Bercy ever
acts, the affected rows can be re-flagged with one query instead of a re-scrape.

### 1.5 CTO accessibility: the PRIIPs/KID barrier

The blocker on US-domiciled ETFs is **Regulation (EU) No 1286/2014 (PRIIPs)**, and it is important to
state the mechanism correctly: **it is an obligation on the seller/distributor, not a ban on the product.**

- [V] **Art. 5(1)**: *"Before a PRIIP is made available to retail investors, the PRIIP manufacturer
  shall draw up … a key information document … and shall publish the document on its website."*
- [V] **Art. 13(1)**: *"A person advising on, or selling, a PRIIP shall provide retail investors with
  the key information document in good time before those retail investors are bound by any contract or
  offer relating to that PRIIP."*
- [V] **Art. 7**: the KID must be in an official language of the Member State of distribution
  (France → French, absent AMF acceptance of another).
- In force since **1 January 2018**.

US 40-Act sponsors (Vanguard, SSGA, Invesco US, BlackRock US) have no EU distribution interest and do
not produce KIDs. An EU broker therefore cannot lawfully **sell** VOO / SPY / VTI / QQQ / VT to a
retail client. Holding and selling are fine — brokers set these symbols **closing-only**.
<https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32014R1286>

**The UCITS carve-out is gone.** [V] PRIIPs Art. 32(1) exempted UCITS (which used the UCITS KIID)
until 31 Dec 2021; **Regulation (EU) 2021/2259 of 15 Dec 2021** replaced that date with
**31 December 2022**. From **1 January 2023** every UCITS sold to EEA retail must publish a PRIIPs KID;
the AMF confirmed this in its doctrine update of **16 Feb 2023** (DOC-2012-06 / DOC-2011-19).
As of 2026 **no exemption exists for anyone**. This tightened the regime; it did nothing for US ETFs,
which were never inside the UCITS carve-out.
<https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32021R2259>

**Nothing in the pipeline re-opens US ETFs.** The **Retail Investment Strategy (RIS)**: provisional
Council/EP agreement **18 Dec 2025**; Presidency final compromise **5 June 2026**; Member States
approved **12 June 2026**; OJ publication expected late Q3/Q4 2026. Directive → 24 months transposition
+ 6 months ⇒ effective **~mid-2029**. Substance is a "product at a glance" dashboard, machine-readable
KIDs via ESAP, standardised past performance. **There is no third-country / US-ETF carve-out anywhere
in the RIS**, nor in the Savings & Investments Union (March 2025). The retail block on US-domiciled
ETFs is stable through at least 2029.

### 1.6 The AMF layer: "commercialisé en France" is a *separate* condition

This is the part most write-ups miss, and it matters for `cto_accessible`.

[V] AMF, **17 July 2025**, *"L'AMF rappelle aux distributeurs leurs obligations en matière de
commercialisation de fonds d'investissement"*: before marketing a fund a distributor must **check that
it is authorised for marketing in France** — by AMF approval, by **European-passport notification to
the AMF**, or by a marketing authorisation; the AMF publishes the definitive list in **GECO**; and the
AMF states explicitly that **ETFs are funds and are subject to these rules**.
<https://www.amf-france.org/en/news-publications/news/amf-reminds-distributors-their-requirements-regarding-marketing-investment-fund>

So the **UCITS passport does not operate automatically** — it needs a host-State notification to the
AMF. Practically, "commercialisé en France" = present in GECO + French-language DIC available +
the **€2,000/year per-fund AMF contribution** paid. Because that fee is per fund, sponsors notify
**selectively**: a perfectly good LU/IE UCITS share class can simply be absent from France.

[V] **Execution-only is not an escape hatch.** French law reads *commercialisation* broadly — it arises
*"dès lors que le produit est présenté en France et que cette présentation vise à inciter les
investisseurs à souscrire"*, including *"la simple mise à disposition sur internet permettant une
souscription directe"*. **Reverse solicitation is not a defence**: the KID obligation applies *"même si
la souscription est faite à l'initiative du client et même si le produit provient de l'extérieur de
l'UE."* This is exactly why French brokers **curate** their ETF universes instead of exposing every
ISIN their venues can reach.

### 1.7 Broker reality for a French retail investor (2026)

Two widely-repeated premises turned out to be **out of date**. I re-verified both independently
because they invert the usual advice:

- [V] **Interactive Brokers DOES offer a PEA.** "PEA Classique" launched **20 Nov 2024** (IBKR Ireland),
  announced by Businesswire press release of that date and carried by ITespresso, ChannelBiz,
  Zonebourse and Fortuneo. French tax residents, 18+, EUR cash, €150k ceiling, no custody/transfer fee,
  commissions capped at 0.5%, free transfer-in of an existing PEA. **No PEA-PME.**
  <https://www.businesswire.com/news/home/20241120296817/fr> ·
  <https://www.interactivebrokers.ie/fr/accounts/plan-depargne-en-action-accounts.php> (403 to bots)
- [V] **Trade Republic DOES offer a PEA**, launched **9 Jan 2025** — confirmed by their own press
  release (`250109_TradeRepublic_PressRelease_BirthdayAnnouncement_FR_FR.pdf`). €1/order, DCA plans
  free, €150k legal ceiling, no custody fee, plus a PEA Jeunes variant. Full ECB banking licence since
  Dec 2023, BaFin-supervised, AMF-registered; securities custodied at HSBC Germany.

| Broker | PEA | CTO venues | US-domiciled ETFs | Notes |
|---|---|---|---|---|
| Interactive Brokers (IE) | **Yes** (since 20/11/2024) | 150+ markets | Blocked for retail; IBKR itself names UCITS equivalents, CFDs, or pro reclassification as alternatives | Deepest universe |
| Trade Republic | **Yes** (since 09/01/2025) | ~2,000 ETFs, **all UCITS** | None offered | **Major 2026 change:** LS Exchange + PFOF ended **30 Jun 2026** (EU PFOF ban); since 1–2 Jul 2026: "Best Price" (€1, TR as counterparty, smart-routed Xetra/Euronext/Nasdaq) and "Direct Price" (€2, ~30 venues). LSE appears absent — **uncertain**, venue list unpublished |
| Saxo Banque France | Yes (€150k) | 40+ exchanges, ~1,300 ETFs | Blocked, explicit KID message | |
| DEGIRO | **No** (site: "en cours de développement") | ~45–50 exchanges incl. Xetra, **LSE, Borsa Italiana**, SIX, Euronext Dublin | Blocked **since 02/01/2018** (hold/sell only) | Strict local-language-KID reading makes some LSE-only lines unbuyable |
| Boursorama / BoursoBank | Yes | Euronext Paris/Growth/Access, Amsterdam, Brussels, Lisbon, Madrid, Milan, SIX, **LSE, Xetra**, NYSE, Nasdaq | Blocked (US *stocks* are fine) | |
| Fortuneo | Yes | Euronext ×3, Xetra, LSE, Borsa Italiana, Madrid, Zurich, NYSE/Nasdaq, Canada | Blocked | |
| Bourse Direct | Yes | Euronext ×4, Xetra, Borsa Italiana, SIX, LSE, NYSE/Nasdaq/AMEX | Blocked | |

**Key correction to a common belief:** French incumbents are **not** confined to Euronext Paris — all
three route to Xetra, LSE and Milan on the CTO. The real deterrent is **cost** (~€12–15 minimum per
foreign order vs €1–4 on Paris), not access. And US-domiciled ETFs are blocked at *every* one of them,
because PRIIPs binds the **distributor**, not the venue.

**Workarounds, and how much to trust them:**

- **Elective professional (MiFID II, Annex II, Section II.1)** — needs **2 of 3**: ≥10 significant-size
  transactions per quarter over the last four quarters; portfolio (cash + instruments) **> €500,000**;
  ≥1 year in a relevant financial-sector role. Plus a firm competence assessment and written
  request/warning/acknowledgement. **No relaxation is in force.** The final RIS lowers the threshold to
  **€250,000 as a three-year average** and adds a training criterion — but that rides on the RIS
  Directive, i.e. **~mid-2029**.
- **Options-assignment route at IBKR** (buy deep-ITM call → exercise → physical delivery of the US ETF).
  Still reported working in 2026, but **never officially documented**, and IBKR support has told some
  clients that without a KID the assignment is **cash-settled** instead. Symbol- and desk-dependent;
  the resulting position becomes closing-only; also drags in **US estate-tax** exposure on a US-situs
  asset. **Do not encode this as a reliable path in the database.**

### 1.8 Edge cases the schema must survive

- **UK-domiciled funds** post-Brexit are "UK UCITS" = third-country products → **blocked** like US ETFs.
- **UCITS ETF listed only on LSE** (Irish-domiciled GBP/USD lines): the fund is fine; buyable at IBKR,
  DEGIRO, Saxo, Boursorama, Fortuneo, Bourse Direct — but **not** at Trade Republic. **Venue ≠ eligibility.**
- **Swiss-domiciled** (rare): non-EU, non-UCITS → no KID → blocked. But most "Swiss" ETFs are actually
  IE/LU-domiciled with a SIX listing and are perfectly fine.
- **Share class not notified in France**: a real and common gap, driven by the €2,000/year AMF fee.
- **ETCs / ETNs** are not funds. Many carry a KID and are tradeable, but they sit outside the UCITS
  test entirely — **model them separately** (Euronext's own directory mixes them in: of 4,174 listings,
  only 2,841 are `productType=ETF`).

---

## PART 1bis — THE DECISION PROCEDURE (pseudocode)

Design principle: **three independent booleans, not one.** Domicile is a *derived hint*, never the test.
Prefer `unknown` over a guess — a wrong `true` on `pea_eligible` costs the user a forced closure of
their plan (BOI-RPPM-RCM-40-50-50).

```pseudocode
# ============================================================
# A. PEA ELIGIBILITY
# input: etf {domicile, is_ucits, legal_form, replication, index,
#             issuer, name, pea_flag_sources[], prospectus_text}
# output: (pea_eligible: true|false|unknown, mechanism, confidence, evidence)
# ============================================================

function pea_eligible(etf):

    # --- Rule 0: hard structural disqualifiers (cheap, certain) -----------
    if etf.legal_form in {ETC, ETN, certificate, structured_note}:
        return (false, "not_a_fund", HIGH,
                "L221-31 I-2 admits only OPCVM/SICAV/FCP")

    if etf.is_ucits == false and etf.domicile not in EU_EEA:
        return (false, "non_eu_non_ucits", HIGH, "L221-31 I-2 c")

    if etf.domicile in {US, UK, CH, JP, CA, AU, KY, JE, GG, ...} - EU_EEA:
        return (false, "domicile_outside_eea", HIGH,
                "L221-31 I-2 c requires an OPCVM established in an EU/EEA State")

    # --- Rule 1: POSITIVE EVIDENCE ONLY. Never infer from the index. -----
    # Ranked, first match wins. Each tier carries its own confidence.

    if etf.isin in EURONEXT_PEA_YES_SET:                     # exchange flag
        return (true, mechanism_of(etf), HIGH,
                "Euronext product directory, pea=Yes, as_of <date>")

    if prospectus_or_dic_states_pea_eligibility(etf):        # primary source
        return (true, mechanism_of(etf), HIGHEST,
                "prospectus / DIC 'éligible au PEA' + décret 2026-189 engagement")

    if etf.isin in ISSUER_PEA_SET:                           # issuer screener
        return (true, mechanism_of(etf), HIGH, issuer_source_url)

    if count(distinct brokers listing etf.isin as PEA) >= 2: # corroboration
        return (true, mechanism_of(etf), MEDIUM, broker_source_urls)

    if count(distinct brokers listing etf.isin as PEA) == 1:
        return (true, mechanism_of(etf), LOW, broker_source_url)   # review queue

    # --- Rule 2: negative inference is NOT allowed --------------------
    # Absence from a broker list means "not offered by that broker",
    # not "not eligible". Only Rule 0 may return false.
    #
    # Rule 2b (optional, weak, flag for human review — do NOT auto-true):
    #   a physically-replicated UCITS ETF whose index is *by construction*
    #   >75% EU/EEA-domiciled issuers (CAC 40, EURO STOXX 50, MSCI EMU,
    #   STOXX Europe 600, MSCI Europe) is *probably* eligible.
    #   Emit (unknown, ..., confidence=HINT) and queue for verification.

    return (unknown, "no_positive_evidence", NONE, null)


function mechanism_of(etf):
    if "swap" in lower(etf.name) or etf.replication == synthetic:
        return "synthetic_swap"        # <- political risk: Bercy review, see 1.4
    if etf.replication == physical and index_is_eu_eea_heavy(etf.index):
        return "physical_eu"
    return "unknown"


# ============================================================
# B. CTO ACCESSIBILITY  (three independent facts, then AND them)
# ============================================================

function cto_accessible(etf, broker = null):

    # --- B1. Does a PRIIPs KID exist at all? (Reg. 1286/2014 art. 5) ----
    has_kid = etf.is_ucits and etf.domicile in EU_EEA
              or kid_document_found(etf)          # verified fetch

    if etf.domicile == US:                        # 40-Act ETF
        return (false, "no_priips_kid", HIGH,
                "Reg (EU) 1286/2014 art. 13(1) - distributor cannot sell")
    if etf.domicile == UK:
        return (false, "uk_ucits_is_third_country", HIGH, ...)
    if etf.domicile not in EU_EEA and not kid_document_found(etf):
        return (false, "no_priips_kid", MEDIUM, ...)

    # --- B2. KID in a language France accepts (art. 7) ------------------
    kid_fr = kid_available_in(etf, "fr")          # AMF may accept EN case-by-case
    if kid_fr == false:
        return (unknown, "kid_language_unconfirmed", LOW, ...)

    # --- B3. Authorised for marketing in France (AMF, host-State notif) --
    # Authoritative: AMF GECO. NOTE: GECO does NOT expose a PEA flag
    # (verified: no "PEA"/"eligib" token anywhere in its 2.4 MB front bundle).
    authorised_fr = present_in_geco(etf.isin)     # true | false | unknown

    if authorised_fr == false:
        return (false, "not_passported_to_france", MEDIUM, amf_geco_url)

    # --- B4. Broker layer: catalogue inclusion != venue reachability -----
    if broker != null:
        if etf.isin in broker.catalogue:  return (true,  "in_catalogue", HIGH, ...)
        if broker == TRADE_REPUBLIC and etf.primary_venue == LSE:
                                          return (false, "venue_not_served", MEDIUM, ...)
        return (unknown, "not_in_broker_catalogue", LOW, ...)

    # generic (no broker specified)
    if has_kid and authorised_fr in {true, unknown}:
        return (true, "ucits_eea_with_kid", MEDIUM, ...)
    return (unknown, ...)


# ============================================================
# C. INVARIANTS — enforce these in tests
# ============================================================
#  C1. pea_eligible == true  IMPLIES  cto_accessible == true
#      (a PEA-eligible fund is an EEA UCITS marketed in France by construction)
#  C2. index_tracked MUST NOT appear in any branch that returns
#      pea_eligible = true.  Assert this statically.
#  C3. domicile == US  IMPLIES  pea_eligible == false
#                       AND     cto_accessible == false
#  C4. every row with pea_eligible != unknown carries (source_url, as_of)
#  C5. ISIN must pass the ISO 6166 Luhn checksum before insert
#      (see scratchpad/validate_isin.py)
```

**Suggested columns:** `pea_eligible` (bool/null), `pea_confidence` (enum),
`pea_eligibility_mechanism` (enum), `pea_source_url`, `pea_as_of`,
`has_priips_kid`, `kid_language_fr`, `authorised_fr` (GECO), `cto_accessible`,
plus a `broker_availability` join table (broker × isin) because catalogue inclusion and
venue reachability genuinely diverge at DEGIRO and Trade Republic.

---

## PART 2 — THE DATA SOURCES (every one fetch-verified on 2026-08-10)

Ranked by usefulness for building and maintaining a `pea_eligible` column.

### Tier 1 — build the pipeline on these

**1. Euronext product directory — the exchange's own PEA flag. BEST FIRST-PARTY SOURCE.**

Found by reverse-engineering the Drupal `drupal-settings-json` blob on the ETF list page. The filter
field is `pea`, rendered as checkboxes `pea[Yes][Yes]` / `pea[No][No]`, and it must be passed to the
backend as a flat **URL query parameter** `&pea=Yes` — putting it in the POST body silently does
nothing and you get all 4,174 rows back.

- Bulk CSV: `POST https://live.euronext.com/product_directory/data/etf-all-markets/download?mics=<MICS>&pea=Yes`
- Paged JSON (DataTables): `POST https://live.euronext.com/en/product_directory/data/etf-all-markets?mics=<MICS>&pea=Yes`
  with body `draw=1&start=0&length=100`
- `<MICS>` is a 40-MIC string read from `drupalSettings.jsongateway` on
  <https://live.euronext.com/en/products/etfs/list>
- **Status: 200, `text/csv`, no auth, no Cloudflare, no rate limiting encountered.**
- **Yield: 156 listings, 154 unique ISINs.** Full directory = 4,174 listings (2,841 `productType=ETF`).
- Identifiers: **ISIN, ticker/symbol, full instrument name, market MIC, currency**, plus prices/volume.
  No TER, no index, no replication.
- Freshness: the file self-stamps **"10 Aug 2026"** — regenerated every trading day.
- **Counter-check I ran:** of the 35 ETFs carrying the literal word "PEA" in their Euronext instrument
  name, **35/35 are flagged `pea=Yes`** — zero false negatives on that probe. And the 154 are a strict
  subset of the broker union, i.e. zero contradictions against independent sources.
- Caveat: **Euronext-listed only.** Genuinely PEA-eligible German-domiciled ETFs traded on Xetra
  (iShares DivDAX, MDAX, TecDAX, ATX, Core EURO STOXX 50 (DE)…) are simply absent. Cover them with a
  broker source.

**2. Bourse Direct — undocumented JSON API with a `pea` facet. Widest broker coverage.**

- `GET https://www.boursedirect.fr/api/instrument/v3/search?nature=tracker&pea=true&size=100&page=N`
  (**`page` is 0-indexed, `size` caps at 100**). Discovered in
  `/apps/instrument-search/production/instrument-search.bundle.js`.
- **Status: 200, no auth, no bot wall.** Declares 402 quotations; 390 fetched → **295 unique ISINs**.
- Identifiers: ISIN, ticker/mnemo, **name truncated to 30 chars**, market MIC, currency, and a
  Morningstar DICI URL embedding a Morningstar `investmentid` — a useful external join key. No TER,
  no issuer field.
- Other confirmed facets: `mic=`, `peapme=true`, `nature=`.
- **Contamination warning:** this endpoint returns Bourse Direct's whole PEA *fund* universe. About 70
  rows are **actively managed SICAV/FCP, not ETFs** (Carmignac, Fidelity Funds, Schroder ISF, JPM
  Euroland, MainFirst, Deutsche Invest, HSBC GIF…). You must intersect with an ETF universe — see #4.

**3. ScanETF — Supabase PostgREST, explicit `is_pea_eligible` boolean. Richest schema anywhere.**

- `GET https://hpdovfdcdqqhqtrxysel.supabase.co/rest/v1/etf_listing?is_pea_eligible=eq.true`
  (public anon key extracted from the site's JS chunk).
- **Yield: 189 PEA rows out of 4,162 total.** Fields: `isin, ticker, nom_du_fonds, fournisseur,
  index_name, replication, frais (TER), devise, date_creation, is_pea_eligible`.
- It also carries an `etf_bank_availability` table with **`boursorama_pea` and `trade_republic`
  flags** — 117 rows `boursorama_pea=true`, which matches an independent live Boursorama scrape of 114.
  That is a ready-made `broker_availability` join table.
- **The only source giving PEA + index + replication + TER in one call**, so it is the best
  *enrichment* source even though it is third-party for the flag itself.
- Freshness: rows created up to 2026-07-17; sitemap `lastmod` 2026-08-06.
- ⚠️ It is a commercial site's exposed anon key. **Check their ToS before productionising.**

**4. ESMA FIRDS — the ETF universe. No PEA flag, but indispensable.**

`etf_master.csv` = **7,810 ETFs** assembled from ESMA FIRDS reference data, keyed by ISIN with CFI
code, currency, fund LEI, manager LEI and venue MICs. **This is what separates real ETFs from the
actively managed funds that leak out of broker "tracker" screens** — it removed 79 non-ETFs from the
seed. Regulatory-grade, free, machine-readable. Use it as the universe table and join everything to it.

### Tier 2 — needed for coverage

**5. justETF FR — has a PEA filter after all (213 ETFs).**
- `https://www.justetf.com/fr/search.html?search=ETFS&pea=true` — the facet is labelled
  **"Éligible au PEA (213)"**.
- The underlying DataTables POST (`...-etfsTablePanel&search=ETFS&pea=true&_wicket=1`) **returns 0 rows
  via curl** — it is Wicket session-stateful. Extraction needed a **real browser** driving the
  DataTables API. Budget for headless-browser automation, not plain HTTP.
- Fields: ISIN, ticker, name, TER, `replicationMethod`, domicile, distribution policy, fund size. Live.

**6. Fortuneo — JSON API with a PEA facet.**
- `GET https://bourse.fortuneo.fr/api/trackers/search/?page=N&additionalParams={"pea":"true"}`
  (URL-encoded; other spellings of the param return 500). 20 rows/page.
- **Status 200, no auth. Yield: total 154 → 152 unique ISINs.**
- Identifiers: **full untruncated name** (better than Bourse Direct), ticker, ISIN, market code,
  PEA + SRD flags, live price. No TER. Issuer list separately at `/api/trackers/search-list`.
- Same mutual-fund contamination as Bourse Direct.

**7. Boursorama / BoursoBank — clean HTML, but no ISIN in the listing.**
- List: `https://www.boursorama.com/bourse/trackers/recherche/?beginnerEtfSearch%5Beligibility%5D%5B0%5D=taxation`
  (the checkbox `eligibility[]=taxation` is the one labelled **PEA**; `taxationPEAPME` is PEA-PME),
  paginated `/page-N`, 15/page → **114–136 trackers** depending on the variant used.
- The listing exposes only name + a Boursorama symbol (`1rTCW8`), **no ISIN**. Resolve by fetching each
  `/bourse/trackers/cours/<symbol>/` and reading
  `<h2 class="c-faceplate__isin">ISIN - Issuer - ETF</h2>` → 136/136 resolved. Needs a browser UA.
- ~136 extra fetches per refresh; fine for a nightly job. Agent C's note stands: this is **the cleanest
  legitimate PEA-flag source** (no ToS grey area, no scraped anon key).

**8. Issuer sites — authoritative for TER / index / replication.**
- **BlackRock / iShares France**, `https://www.blackrock.com/fr/intermediaries/products/<id>/...` —
  ISIN, ticker, index name, "Physique / Réplication totale" vs swap, and TER. Verified working.
- **Saxo × Amundi PDF**:
  `https://www.home.saxo/-/media/documents/campaigns/amundi/liste-etf-amundi-x-saxo.pdf` — 7 pages,
  text-extractable, **richest single schema**: name, ISIN, ticker, C/D, SFDR, **TER**, **PEA Oui/Non**,
  KID URL. 153 rows, **42 flagged PEA=Oui**. **But stale: "Sélection valable au 20 mars 2025"**
  (~17 months) and Amundi-only. Use for TER backfill, never for the flag.
- **Bourse Direct partner tables**: `https://www.boursedirect.fr/fr/etf/offres/amundi-etf` — clean HTML
  table Catégorie / Nom / ISIN / Éligible PEA ✓, 118 rows of which **51 PEA**; ticker in the buy link.
  Siblings `/ishares`, `/wisdomtree`, `/vaneck` have ISIN tables but **no PEA column**. No date shown.

### Issuer endpoints that carry a real first-party PEA flag

| Issuer | Endpoint | PEA field | PEA rows |
|---|---|---|---|
| **Amundi** | `POST https://www.amundietf.fr/mapi/ProductAPI/getProductsData` | `characteristics.FUND_PEA` (bool) | **115** |
| **iShares** | `GET blackrock.com/fr/intermediaries/products/{portfolioId}/fund/1499538099375.ajax` | `div.col-peaFlag` "Régime fiscal PEA" = Oui | **36** |
| **BNP Paribas Easy** | `GET api.bnpparibas-am.com/push/sharesearchv2/PV_FR-FSE/4149` | `flags[]` contains `pea_flag` | **24** |
| **Ossiam** | `GET api.ossiam.net/front.shareClass/byCountry/France` | `isPea` (bool) | **6** |
| **State Street SPDR** | per-fund pages under `ssga.com/fr` (finder `/bin/v1/ssmp/fund/fundfinder`) | "Eligible PEA" table row | **4** |
| Vanguard / HSBC AM | prospectus & annual-report PDFs only | none — prose only | 3 / 2 |

**Amundi is the single most valuable endpoint in this whole recon, and it is fully solved.** One
**unauthenticated POST, JSON, 200, ~1.4 MB** returns all 633 products; the site's PEA filter is purely
**client-side** on `FUND_PEA`, so no filter arguments are needed — fetch everything, filter locally. It
ships **TER + `BENCHMARK_NAME` + replication (`Direct(Physical)` vs `Indirect(Swap Based)`) + ticker +
real product-page URLs**, plus AUM/NAV/SRRI/SFDR/domicile and `OLD_ISINS` (the Lyxor→Amundi rename
history — useful for reconciling stale lists). Wire `issuer` is `AMUNDI` or `LYXOR` on legacy lines;
normalise both. Cross-checks passed: identical 115 under RETAIL and INSTIT profiles, and all 5 ISINs
hard-linked on Amundi's own `/etf-pea` page are present.

This matters because Amundi absorbed Lyxor and **dominates the French PEA market — 115 of the 250
verified seed rows (46%) are Amundi**, now sourced issuer-first rather than by third-party agreement.

**Negative results, recorded so nobody re-hunts them:**
**Xtrackers/DWS publishes no PEA flag in any machine-readable form** — confirmed four ways (datatable
columns, both JS bundles, rendered finder UI, rendered product page) via
`POST etf.dws.com/api/fundfinder/fr-fr/datatable` (needs `client-id: passive-frontend` plus an
`audiences_fr-fr` cookie). It *does* return all **428 products with ISIN/name/TER/currency** — good for
**TER backfill only**. Same negative for **Invesco** (`dng-api.invesco.com/product/search` works, no
PEA field, zero "PEA" hits site-wide or in annual reports), **VanEck** and **Franklin Templeton**. For
those issuers eligibility can only come from Euronext `pea=Yes`, justETF or brokers. UBS/CS unverified
(JS-only site, fundgate 403).

**Caveats for the schema:** iShares' `peaFlag` is **share-class-level and under-populated on recent
launches**, so 36 is a *lower bound*; 34 of the 36 are flag-sourced, 2 (`IE000DQLYVB9`/SPEA,
`IE00055B2JD3`/WPEH) come from product-page prose — keep provenance per row if you need strict
flag-only sourcing. **BNP's 24 rows carry ISIN + name only** — their API exposes no TER, index or
replication at all; that is the one remaining backfill gap in the issuer layer.

### Tier 3 — corroboration only, never promotes on its own

**9. GitHub — the only two open PEA lists that exist.**
- `https://raw.githubusercontent.com/majordomef-sudo/pea-comparator/main/etf-data.js` →
  `window.PEA_ETFS` JSON, **429 rows (418 unique)**, pushed 2026-07-17. Largest PEA list found
  anywhere — but **low quality**: names like `"PROSPECTUS OF…"`, many `N/A`, and only 144/429 overlap
  justETF.
- `https://raw.githubusercontent.com/corentinbouton/ETF-scrapper/main/data/ETFs_20-07-24.xlsx` —
  95 rows, a Boursorama PEA scrape. **Filename self-dates to 2024-07-20: two years stale**, and it
  includes 9 leveraged products from an unfiltered sub-scrape.

**10. French finance blogs** — ramify.fr (28), investissements-faciles (16), finref (15), sinvestir
(13), Finary (10), lenouvelinvestisseur (6), prosper-conseil (6); 53 unique, 42 of which are confirmed
by core sources. Editorial "best of" articles, and several **mix PEA and CTO products in one table**.
Useful only for TER backfill.

### Verified dead ends — recorded so nobody re-checks them

| Source | Result |
|---|---|
| **AMF GECO** <https://geco.amf-france.org> | Authoritative for *"authorised for marketing in France"*, but **carries NO PEA flag**. I downloaded the whole 2.4 MB Angular bundle (`/main.js`) and grepped it: **zero occurrences of "PEA" or "eligib"** in the entire front end. API base is `https://geco.amf-france.org/back-office/` (rewrites to `/back-office/v0/…`; endpoints `funds/search`, `funds/share/{id}` are keyed on an internal numeric id, **not** ISIN). Use it for `authorised_fr`; useless for `pea_eligible`. |
| **Décret 2026-189** | Creates a per-fund *engagement* lodged with the AMF, but **explicitly no public register and no attestation system**. There is still **no official public list to scrape** — this is the single biggest structural gap. |
| **TrackInsight** | PEA facet *exists* in the JS bundle (`n.PEA="pea"`) but `search-api` returns 202 / 0 bytes (Cloudflare). |
| **Trade Republic** | `/fr-fr/pea` is marketing copy; support article 302s to `/support`; universe browser is a login-gated SPA; `api.traderepublic.com/api/v1/...` → `No such path`. No public list. |
| **BforBank** | `/fr/bourse/pea` returns a ~4.4 KB Next.js shell, nothing server-rendered. |
| **Crédit Agricole Investore** | Product URL 302s to a generic page; `ca-sicavetfcp.fr` "fonds éligibles au PEA" → 200 with **0 ISINs** (JS-rendered). |
| **DEGIRO FR** | `/tarifs/etf-gratuits` → **503** (bot block). Moot anyway: DEGIRO offers no PEA. |
| **data.gouv.fr** | **Nothing.** `q=ETF` → 0 datasets; `q=PEA` → 1 ("Webstat PEA", Banque de France aggregates only). |
| **Reddit r/vosfinances wiki** | Hard **403** from this IP on www / old / api hosts and via r.jina.ai. Worth a manual look from a browser. |
| Morningstar FR (202/0), Zonebourse (403), investir.lesechos (403), abcbourse (404), Quantalys (JS wall, no PEA token), ETFbook (no PEA), Yomoni `/pea` (404), easyBourse `/etf` (404), devenir-rentier (403) | all dead |

### Recommended pipeline

```
universe      = ESMA FIRDS etf_master              # is it an ETF at all?
flag_issuer   = Amundi FUND_PEA + iShares peaFlag + BNP pea_flag + Ossiam isPea
                                                   # first-party, best quality
flag_exchange = Euronext pea=Yes                   # daily, exchange-level
flag_breadth  = Bourse Direct + Fortuneo + Boursorama + justETF
                                                   # catches Xtrackers/Invesco,
                                                   # which publish no PEA flag
enrichment    = Amundi API (TER+benchmark+replication) -> ScanETF supabase
                -> Xtrackers datatable (TER only) -> blogs, last
authorised_fr = AMF GECO                           # separate, CTO-side field
corroboration = GitHub lists, blogs                # never promote alone
```

Cadence: Amundi + Euronext + broker APIs nightly; justETF (needs a browser) and the other issuer
pages weekly.

**Important operational rule:** treat an ISIN *leaving* the Euronext `pea=Yes` set as an **alert, not a
silent delete**. It may mean the fund breached the 75% quota — which under BOI-RPPM-RCM-40-50-50 is a
**plan-closure event for the holder**, the single most damaging thing this database could fail to surface.

**The three sources genuinely disagree.** Agent C measured it: justETF ∩ ScanETF = 166,
justETF ∩ Boursorama = 107, all three = 88. So **model `pea_eligible` as a voted field with provenance,
not a single-source boolean.**

---

## PART 3 — SEED DATA

**File:** `pea_eligible_seed.csv` — columns `isin,ticker,name,issuer,index_tracked,replication,ter,source_url`

One file, two sections, separated by a fenced `# ===== UNVERIFIED =====` comment block followed by a
repeated header row.

**Inclusion rule for the VERIFIED body.** An ISIN must be confirmed an ETF by ESMA FIRDS **and** either
be in the Euronext `pea=Yes` set, **or** be asserted PEA-eligible by **≥1 first-party source**
(the exchange, a regulated broker's own PEA filter, or the fund issuer), **or** by **≥2 independent
source families** of any tier. Rationale: a regulated broker carries regulatory risk for getting this
wrong, so its own filter counts singly; screeners, GitHub and blogs are corroboration only.

**Integrity controls actually applied — not just asserted:**

- Every ISIN was **read out of a fetched file**. None was typed from memory or reconstructed.
- Every ISIN passes the **ISO 6166 Luhn check digit** (`validate_isin.py`): **0 invalid, 0 duplicates**
  across all 539 rows (**250 verified + 289 unverified**). A parallel run rejected 5 blog ISINs this way
  (`FR0014002CG7`, `FR0014002CH5`, `FR0013412345` — typos/placeholders) and they were never written.
- **79 rows that were PEA-eligible but not ETFs** (actively managed SICAV/FCP leaking out of broker
  "tracker" screens) were **dropped**, and listed in `dropped_not_etf.txt` rather than silently binned.
  Note these are genuinely PEA-eligible — they are simply out of scope for an *ETF* database.
- TER normalised to a plain percent number (`0,19 %` → `0.19`); values outside 0–5% are **rejected
  rather than guessed**. `replication` normalised to `physical` / `synthetic` **only where a source
  stated it** — never inferred from the index.
- **Blank beats guessed, everywhere.**

**Reproduce with:** `python merge_seed.py && python validate_isin.py pea_eligible_seed.csv`

### Working files left in the scratchpad

| File | What it is |
|---|---|
| `pea_eligible_seed.csv` | **deliverable** — verified body + fenced unverified section |
| `recon_eligibility.md` | **deliverable** — this document |
| `euronext_pea_yes.csv` | raw Euronext `pea=Yes` export, 156 listings, 10 Aug 2026 |
| `euronext_etf_all.csv` | raw Euronext full directory, 4,174 listings |
| `etf_master.csv` | ESMA FIRDS ETF universe, 7,810 ISINs |
| `broker_lists.csv` | Bourse Direct / Fortuneo / Boursorama / Saxo, 675 rows → 301 ISINs |
| `community_lists.csv` | justETF / ScanETF / GitHub / blogs, 1,134 rows → 533 ISINs |
| `issuer_lists.csv` | **first-party issuer flags**, 190 rows: Amundi 115, iShares 36, BNP 24, Ossiam 6, SPDR 4, Vanguard 3, HSBC 2 |
| `raw/amundi_products2.json` + `raw/amundi_body2.json` | the full 633-product Amundi payload and the exact POST body that fetches it |
| `xtrackers_full_universe_no_pea_flag.csv` | all 428 Xtrackers products w/ TER — **TER backfill only, no PEA determination** |
| `ishares_pea.csv` | BlackRock product pages: TER, index, replication |
| `dropped_not_etf.txt` | the 79 PEA-eligible non-ETFs, with their sources |
| `validate_isin.py` | ISO 6166 checksum + duplicate checker |
| `merge_seed.py` | the merge with the trust tiers encoded |
| `build_seed.py` | earlier Euronext-only builder, kept for reference |

---

## What is genuinely unknowable and needs manual curation

1. **There is no authoritative public list, and Décret 2026-189 confirms there will not be one.** The
   AMF holds a per-fund *engagement* but publishes no register. Ground truth for any single ISIN is the
   **prospectus / DIC "éligibilité PEA" line** plus the fund's annual report disclosure of its actual
   quota. That is a per-fund PDF read — irreducibly manual, or an LLM-extraction job.
2. **The three best screeners materially disagree** (all-three overlap is only 88 of 243). Someone has
   to adjudicate the ~150 contested ISINs by hand against prospectuses.
2b. **Four issuers publish no PEA flag at all** — Xtrackers/DWS, Invesco, VanEck, Franklin Templeton
   (each confirmed negative by multiple methods, not merely "not found"). Their products can *never*
   be issuer-verified; they will always rest on Euronext/broker/screener evidence, so they are
   permanently a confidence tier below the Amundi/iShares/BNP/Ossiam rows. iShares' own flag is
   additionally **under-populated on recent launches**, so its 36 is a floor, not a count.
3. **Whether the 75% quota is *currently* met** is unobservable from outside. Eligibility can lapse
   silently between the semi-annual reports, and the consequence falls on the *holder*.
4. **The synthetic-ETF political risk is live and unresolved.** Sénat QE n° 08783 (14 May 2026) is
   still marked "en attente de réponse"; the only reassurance is an informal *Les Échos* report of
   9 June 2026. This could invalidate the single most popular category in the seed (every
   `mechanism = synthetic_swap` row) on a future finance law. Not predictable — just make it
   one-query re-flaggable.
5. **Per-broker catalogue inclusion cannot be derived from any rule.** DEGIRO and Trade Republic
   diverge from venue reachability in ways only their own catalogues reveal. ScanETF's
   `etf_bank_availability` table is the only structured start; the rest is per-broker scraping.
7. **`authorised_fr` (marketing passport to France)** requires GECO lookups keyed on an internal
   numeric id, not ISIN — the ISIN→id mapping still has to be solved before that field can be automated.
8. **The €500k elective-professional threshold** drops to €250k (3-year average) under the RIS, but only
   around **mid-2029**. Anything encoding the CTO workaround needs a dated rule, not a constant.
