# ETF Universe — Source Reconnaissance Report

**Repo:** PrincePatch/etf-database
**Date of verification:** 2026-08-10 (all live fetches performed this day)
**Verifier environment:** Windows 10, curl 8.21.0, Python 3.12.7

> Every source marked **VERIFIED** was actually fetched during this recon and the sample rows below
> are real bytes returned by the endpoint. Anything not fetched is marked **UNVERIFIED**.

---

## 1. Recommended primary sources — ranked

| # | Source | What it gives | ISIN | Ticker | Issuer | Exch | Ccy | Rows | Key? | Status |
|---|--------|---------------|------|--------|--------|------|-----|------|------|--------|
| **1** | **ESMA FIRDS `FULINS_C`** | EU-wide regulator reference data — the backbone | YES | no | LEI | YES (MIC) | YES | **7,837 ETF ISINs / 102,660 ISIN×venue** | no | **VERIFIED** |
| 2 | **Euronext product_directory CSV** | Paris/Amsterdam/Brussels/Lisbon/Oslo **+ Borsa Italiana ETFplus** | YES | YES | no | YES | YES | 4,174 listings / 3,240 ISIN | no | VERIFIED |
| 3 | **Deutsche Börse Xetra t7 allTradableInstruments CSV** | Xetra ETF/ETC/ETN + German ticker & WKN | YES | YES | no | YES | YES | 3,676 ETP / 3,522 ISIN | no | VERIFIED |
| 4 | **Nasdaq Trader `nasdaqtraded.txt`** | ALL US listings w/ ETF flag + listing exchange | no | YES | no | YES | (USD) | 5,587 ETFs | no | VERIFIED |
| 5 | **LSE Price Explorer API** + monthly XLSX | UK ETFs/ETCs/ETNs with issuer | YES | YES | YES | YES | YES | 5,281 lines / 4,563 in XLSX | no | VERIFIED |
| 6 | **SIX Swiss `fqs/ref.csv`** | Swiss ETFs **with TER** | YES | YES | YES | YES | YES | 2,277 ETF + 283 ETP | no | VERIFIED |
| 7 | **OpenFIGI `/v3/mapping`** | ISIN → tickers/exchanges; **also ISIN→US ticker** | in | YES | no | YES | no | n/a (lookup) | optional free | VERIFIED |
| 8 | **GLEIF `rr` relationship + ISIN→LEI** | LEI → fund manager / issuer brand | YES | no | YES | no | no | 9.1M ISIN-LEI; 149k fund-manager | no | VERIFIED |
| 9 | **SEC Investment Company Series & Class CSV** | US issuer (trust) + fund name per ticker | no | YES | YES | no | no | 28,845 tickers; 4,330 US ETF matches (77.5%) | no | VERIFIED |
| 10 | **Yahoo Finance screener (POST + crumb)** | Global; **TER + AUM** — few free sources have these | no | YES | weak | YES | YES | ~53k listings across regions | no (crumb) | VERIFIED |
| 11 | **Borsa Italiana `infoproviders.xlsx`** | **TER + issuer group + ESG** for ETFplus | YES | YES | YES | no | no | 2,221 | no | VERIFIED |
| 12 | **HKEX `ListOfSecurities.xlsx`** | Hong Kong ETPs, daily | YES | YES | no | YES | YES | 412 | no | VERIFIED |
| 13 | **JerBouma/FinanceDatabase (MIT)** | cross-check tickers + `family` issuer brand | partial | YES | YES | YES | YES | 36,480 rows (22% w/ ISIN) | no | VERIFIED |
| 14 | **Twelve Data `/etf`** | 58,014 global listings, FIGI-keyed — ISIN paywalled | **no** | YES | no | YES | YES | 58,014 | no key | VERIFIED |
| — | **justETF** | 3,567 rows w/ TER+AUM — **robots-disallowed, ToS 3.1** | YES | YES | YES | no | YES | 3,567 | no | **EXCLUDE** |

**Bottom line:** sources #1 + #2 + #3 alone, fetched with plain `curl` and **no API key at all**,
produce a measured **8,692 unique European ETP ISINs**. Add `nasdaqtraded.txt` for 5,587 US
listings and you are at the target universe in **four HTTP requests**.

### The single most important finding

**ESMA FIRDS `FULINS_C` is a 3.5 MB zip that contains the entire EU ETF universe** — 7,837 ETF
ISINs with issuer LEI, currency and every venue MIC — published weekly by the regulator under an
explicitly reuse-permitted licence. It alone covers **80.9%** of everything Euronext and Xetra
return, and adds **4,224 ISINs neither of them has**. Build on this, not on exchange scraping.

**Second most important:** FIRDS also carries **2,333 US-domiciled ETF ISINs**, and feeding those
to OpenFIGI with `exchCode:"US"` resolved **40/40 to correct, live US ETF tickers** in my test.
That recovers ~42% of the otherwise-unsolvable US ISIN gap (§4.1) for free.

**Third:** FIRDS is an *EU* register — **the UK is outside it post-Brexit**, so the LSE's 4,563 ETPs
are largely additive rather than redundant. LSE is therefore the highest-value venue to add after
FIRDS, not an afterthought.

**One thing to deliberately walk away from:** justETF would hand you 3,567 European funds with TER
and AUM in a **single** request — but its robots.txt names both data endpoints under `Disallow` and
its T&C clause 3.1 bans automated querying outright. For a project whose point is an *open*
database, it has to be excluded; §2.13 lists the legitimate TER sources that replace it.

---

## 2. Source-by-source detail

### 2.0 ESMA FIRDS — `FULINS_C`  ★★ THE BACKBONE — **VERIFIED (independently, twice)**

FIRDS is the Financial Instruments Reference Data System that every EU trading venue is legally
obliged to report to under MiFIR Art. 27. It is the authoritative EU instrument registry, it is
free, it needs no key, and **the ETF slice is one small file**.

**Step 1 — discover the current file (Apache Solr, public):**
```
GET https://registers.esma.europa.eu/solr/esma_registers_firds_files/select
    ?q=*
    &fq=publication_date:[2026-08-08T00:00:00Z TO 2026-08-09T00:00:00Z]
    &fq=file_type:FULINS
    &wt=json&rows=100
```
HTTP 200, no key, no headers. I got `numFound: 39` for that one week. Real document returned:

```json
{"file_name":"FULINS_C_20260808_01of01.zip","file_type":"FULINS",
 "publication_date":"2026-08-08T00:00:00Z",
 "download_link":"https://firds.esma.europa.eu/firds/FULINS_C_20260808_01of01.zip",
 "checksum":"4c6875eedb76c683d823c6c0568f3218"}
```

**Step 2 — the key insight: only ONE of the ~39 weekly files matters.**
`FULINS` is split by **CFI category letter**. `C` = Collective Investment Vehicles = all ETFs.
The other letters (`D` debt, `E` equity, `F` futures, `H`, `I`, `J`, `O`, `R` rights — 17 parts
alone, `S` swaps) are irrelevant to the fund universe.

**Step 3 — download and parse:**
```
GET https://firds.esma.europa.eu/firds/FULINS_C_20260808_01of01.zip
-> 3,566,018 bytes zip  ->  FULINS_C_20260808_01of01.xml  87,231,560 bytes
```
ISO 20022 schema `urn:iso:std:iso:20022:tech:xsd:auth.017.001.02`. Iterate `<RefData>` elements.

**My own measured parse of the 2026-08-08 file (not the subagent's — I re-ran it):**

```
TOTAL RefData records             : 144,469
Unique ISINs (all CIV)            :  18,297
CFI CE* (= ETF) records           : 102,660
ETF unique ISINs                  :   7,837
ETF unique (ISIN, MIC) pairs      : 102,660
distinct issuer LEIs              :   5,518
CFI split: CE 102,660 | CI 33,621 | CM 5,232 | CB 1,557 | CF 1,206 | CH 142 | CP 38 | CS 13
```
**Filter is simply `ClssfctnTp` starts with `CE`.**

**Fields present** (this is the complete useful schema):
`FinInstrmGnlAttrbts/Id` = **ISIN** · `FullNm` = full name · `ShrtNm` (ISO 18774 FISN) ·
`ClssfctnTp` = **CFI** · `NtnlCcy` = **currency** · `Issr` = **issuer LEI** ·
`TradgVnRltdAttrbts/Id` = **venue MIC** · `FrstTradDt` · `TermntnDt` · `ReqForAdmssnDt` ·
`RlvntCmptntAuthrty` · `RlvntTradgVn`.

**Real extracted rows (verbatim from my parse):**
```
('LU1681043599', 'AMUNDI MSCI WORLD UCITS ETF', 'CECGMS', 'EUR', '5493003BFED2MWDBYH64', 'AQEA')
('LU1681043599', 'AMUNDI MSCI WORLD UCITS ETF', 'CECGMS', 'EUR', '5493003BFED2MWDBYH64', 'AQED')
('LU1681043599', 'AMUNDI MSCI WORLD UCITS ETF', 'CECGMS', 'EUR', '5493003BFED2MWDBYH64', 'AQEU')
```
(tuple = ISIN, FullNm, CFI, currency, issuer LEI, venue MIC)

ISIN domiciles: **IE 3,211 · US 2,333 · LU 1,240 · DK 459 · DE 133 · FR 124 · CA 82 · CH 68 · HK 30 · JP 28.**
Currencies: USD 48,463 · EUR 47,740 · GBP 2,548 · CHF 1,086 · JPY 892 · DKK 877 · SEK 290 · MXN 187.

**Measured overlap with the exchange sources:**
```
FIRDS ETF ISINs            : 7,837
Euronext u Xetra           : 4,468
  of which FIRDS covers    : 3,613  (80.9%)
FIRDS-only (brand new)     : 4,224
Euronext/Xetra not in FIRDS:   855   <-- see caveat 3 below
GRAND UNION (European)     : 8,692
```

**Cadence:** full snapshot **every Saturday** (verified 15 consecutive weeks 2026-05-02 → 2026-08-08),
plus daily `DLTINS` delta files (`auth.036.001.03`, with `NewRcrd`/`ModfdRcrd`/`TermntdRcrd`
envelopes) if you want incremental updates.

**Licensing — the best of any source here.** ESMA legal notice: *"Reproduction of all information
on this site is authorised except as otherwise stated, provided the source is acknowledged."*
Commercial republication is permitted provided you disclose it is freely available from ESMA.
No rate limits observed.

#### Three caveats you must design around

1. **FIRDS has NO ticker/symbol.** I scanned all 144,469 records — there is no such element.
   `ShrtNm` is a FISN abbreviation, not a tradeable symbol. Fill from OpenFIGI / Euronext / Xetra.
2. **The 102,660 (ISIN, MIC) pairs are NOT 102,660 real exchange listings.** They are massively
   inflated by MTFs, systematic internalisers and OTC reporting venues. Top MICs by distinct ISIN:
   ```
   6550 BTFE (Bloomberg MTF)   5558 TWEM (Tradeweb)   4354 XPAC   4354 XPOS   4111 XGAT
   3637 HAMN (Hamburg)  3445 HAND  3247 MUND (Munich)  3184 DUSB  3182 DUSD (Düsseldorf)
   2958 XETA  2958 XETU  2920 MUNB  2912 FRAA  2912 FRAU  2851 STUB (Stuttgart)
   ```
   **You must whitelist regulated-market MICs.** I built and measured the fix — see below.
   **The fix — ISO 10383 MIC register (VERIFIED, free, no key):**
   ```
   GET https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv
   -> 587,384 bytes, 2,875 MICs
   Columns: MIC, OPERATING MIC, OPRT/SGMT, MARKET NAME-INSTITUTION DESCRIPTION,
            LEGAL ENTITY NAME, LEI, MARKET CATEGORY CODE, ACRONYM, ISO COUNTRY CODE,
            CITY, WEBSITE, STATUS, CREATION DATE, LAST UPDATE DATE, EXPIRY DATE, COMMENTS
   ```
   Category codes: NSPD 1,309 · MLTF 379 · SINT 349 · **RMKT 302** · ATSS 142 · OTFS 128 · OTHR 127.

   **Measured effect of joining FIRDS pairs to `MARKET CATEGORY CODE`:**
   ```
   Raw FIRDS ETF (ISIN, MIC) pairs : 102,660
     MLTF (MTFs — Bloomberg, Tradeweb, …) :  84,483   <-- 82% is MTF noise
     RMKT (regulated markets)             :  18,150
     OTFS                                 :      27

   After filtering to RMKT: 18,150 listing rows across 4,735 distinct ISINs
   ```
   So of 7,837 ETF ISINs, **4,735 have a genuine regulated-market listing** and the remaining 3,102
   appear only on MTFs/SIs (mostly US ETFs OTC-reported into the EU). **18,150 — not 102,660 — is
   the realistic European listing-table size.** Keep the MTF rows in a separate `venue_eligibility`
   table if you want them at all.

3. **ETCs and ETNs are NOT in `FULINS_C`.** They are debt instruments (CFI starts `D`), so they live
   in `FULINS_D`. That is exactly why 855 Euronext/Xetra ISINs are "missing" from FIRDS — they are
   the Leverage Shares / WisdomTree / Xtrackers ETC/ETN lines. If you want ETPs and not just ETFs,
   either also ingest `FULINS_D` (much larger, 17+ parts) or take ETC/ETN from Xetra
   (`Instrument Type ∈ {ETC, ETN}`, 592 rows) and Euronext, which is far cheaper.

---

### 2.1 Euronext — `live.euronext.com` product directory  ★ BEST TICKER SOURCE FOR EU — **VERIFIED**

> Note: a parallel agent reported Euronext as "returns HTML, real export likely behind JS".
> **That is wrong** — I verified a working keyless CSV download below. Trust this section.

The public ETF page is a Drupal app. The real data endpoints are exposed in the
`drupal-settings-json` blob on `https://live.euronext.com/en/products/etfs/list` under the keys
`jsongateway`, `jsongateway_download` and `filter_jsongateway`. (My first guess,
`/en/pd/data/etf?...`, returned `{"iTotalRecords":null,"aaData":[]}` — it is the wrong path.)

**CSV bulk download (recommended):**

```
GET https://live.euronext.com/en/product_directory/data/etf-all-markets/download?mics=<MIC list>
```

* Method: **GET works with no headers at all** (POST with `format=csv` returns the identical bytes).
  No cookie, no User-Agent, no Referer, no token required. Confirmed with bare `curl`.
* Response: `text/csv; charset=UTF-8`, `;`-delimited, UTF-8 **BOM**, 707,412 bytes.
* Structure: line 1 = header, lines 2–4 = junk preamble ("European Trackers" / date /
  disclaimer) — **skip lines 2–4**, data starts line 5.
* Full MIC list (from the page config, 41 MICs):
  `ALXA,ALXB,ALXL,ALXP,ATFX,BGEM,ENXB,ENXL,ETFP,ETLX,EXGM,MERK,MIVX,MLXB,MOTX,MTAA,MTAH,MTCH,SEDX,TNLA,TNLB,VPXB,WOMF,XACD,XAMC,XAMS,XATL,XBRU,XDUB,XESM,XLDN,XLIS,XMLI,XMOT,XMSM,XOAM,XOAS,XOBD,XOSL,XPAR,XPMC`
  (URL-encode the commas as `%2C`.)

**Columns:** `Instrument Fullname; ESG Classification; Name; ISIN; Symbol; Market; Currency;
Open Price; High Price; low Price; last Price; last Trade MIC Time; Time Zone; Volume; Turnover;
Closing Price; Closing Price DateTime`

**Real sample (first 3 data rows, verbatim):**

```
"iShares $ Asia Investment Grade Corp Bond UCITS ETF USD (Acc)";"ESG ETF art. 8";"$Asia IG Corp US A";IE0007G78AC4;ASIG;"Euronext Amsterdam - Multi-currency Trading";USD;5.488;5.488;5.462;5.462;" 17:04";CET;23996;131434.674;5.465;
"Leverage Shares -1x Short Disney ETP Securities";-;"-1X SHORT DIS";XS2337085422;SDIS;"Euronext Amsterdam";EUR;5.148;5.148;5.148;5.148;" 09:04";CET;0;0.00;5.26;
"Leverage Shares -1x Short Palantir ETP Securities";-;"-1X SHORT PLTR";XS3172423520;SPLR;"Euronext Amsterdam";EUR;19.852;19.852;19.852;19.852;" 09:04";CET;0;0.00;19.154;
```

**Measured counts:** 4,174 data rows, **3,240 unique ISINs**.
Breakdown by `Market`:

```
2452  ETF Plus                                   <-- Borsa Italiana (Milan)!
 780  Euronext Paris
 485  Euronext Amsterdam
 334  Euronext Amsterdam - Multi-currency Trading
  69  Euronext Paris - Multi-currency Trading
  34  Euronext Amsterdam, Paris
  11  Euronext Amsterdam, Brussels
   4  Euronext Paris, Amsterdam
   2  Euronext Brussels
   2  Oslo Børs
   1  Euronext Paris, Brussels
```

ISIN domiciles: IE 2259, LU 976, XS 376, CH 143, FR 140, JE 116, GB 74, DE 67, NL 16.
Currencies: EUR 3768, USD 376, JPY 7, GBP 5, CHF 5, CZK 4, HUF 4, HKD 3, NOK 2.

> **Big finding:** because Euronext owns Borsa Italiana, the `ETFP`/`ETLX`/`MTAA` MICs are served by
> this same endpoint. **2,452 of the 4,174 rows are Borsa Italiana ETFplus.** You do **not** need to
> scrape borsaitaliana.it separately for the listing universe.

**JSON alternative** (if you want paging / the DataTables shape):
```
POST https://live.euronext.com/en/product_directory/data/etf-all-markets?mics=<...>
Headers: X-Requested-With: XMLHttpRequest, Content-Type: application/x-www-form-urlencoded
Body: draw=1&start=0&length=100&iDisplayLength=100&iDisplayStart=0
-> {"iTotalRecords":4174,"iTotalDisplayRecords":4174,"aaData":[[...]]}
```
The first cell is an `<a href="/en/product/etfs/{ISIN}-{MIC}">` — **ISIN and MIC are parseable
straight out of the href**, which is handy but the CSV is strictly easier.

**Not available here:** issuer/management company, TER, AUM, inception date. The `filter_jsongateway`
path (`/en/product_directory/filter/etf-all-markets`) has an `issuerGroup` facet but **returned
HTTP 500** on every call I made — treat as **UNVERIFIED/broken**. The per-ETF product page
(`/en/product/etfs/IE00B4L5Y983-XAMS`) contains only an opaque `"issuer_code":"153127"` in its JS
settings, not an issuer name. Issuer must come from elsewhere (see §3).

**ToS / robots:** `live.euronext.com/robots.txt` contains only sitemaps plus
`User-agent: GPTBot / Disallow: /product/`. The `/product_directory/` download path is **not**
disallowed for ordinary clients. Data is delayed/EOD ("All datapoints provided as of end of last
active trading day"), which is the free tier by design. Real-time redistribution would need a
licence; a static reference universe (ISIN/name/ticker/MIC/ccy) is the low-risk use.

---

### 2.2 Deutsche Börse / Xetra — T7 all tradable instruments  — **VERIFIED**

Discovery: the download page `https://www.cashmarket.deutsche-boerse.com/cash-en/trading/Tradable-Instruments-Xetra/Downloads`
exposes the direct blob links. (The `xetra.com/resource/blob/1528/...` guess 404s.)

```
GET https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/a31c10e3183f4c5dd721f9c7f9eaaaea/data/t7-xetr-allTradableInstruments.csv
```

* No key, no headers required. `text/csv; charset=UTF-8`, `;`-delimited, **4,508,731 bytes**.
* Line 1 `Market:;XETR`, line 2 `Date Last Update:;10.08.2026` (updated daily — it was today's
  date), line 3 = header, data from line 4. **5,101 instruments total.**
* ~150 columns (mostly T7 microstructure: tick tables, multicast addresses…). The useful ones:
  `Instrument, ISIN, WKN, Mnemonic, MIC Code, Currency, Instrument Type,
  Product Assignment Group Description, Primary Market MIC Code, First Trading Date,
  Country Of Issue, Product Status, Instrument Status`.

**`Instrument Type` is a clean ETP classifier:**

```
3084  ETF
1425  CS   (common stock - discard)
 387  ETN
 205  ETC
```
→ **3,676 ETP rows, 3,522 unique ISINs.**

**Real sample (3 ETF rows, selected columns):**

```json
{"Instrument":"EXPAT BUL.SOFIX UCITS ETF","ISIN":"BG9000011163","WKN":"000A2ARPV","Mnemonic":"BGX","MIC Code":"XETR","Currency":"EUR","Instrument Type":"ETF","Product Assignment Group Description":"EXCHANGE TRADED FUNDS - PASSIV","Primary Market MIC Code":"XBUL","First Trading Date":"2018-01-10"}
{"Instrument":"EXPAT CROAT.CROBEX UCITS","ISIN":"BGCROEX03189","WKN":"000A2JB7C","Mnemonic":"ECDC","MIC Code":"XETR","Currency":"EUR","Instrument Type":"ETF","Product Assignment Group Description":"EXCHANGE TRADED FUNDS - PASSIV","Primary Market MIC Code":"XBUL","First Trading Date":"2024-07-25"}
{"Instrument":"EXPAT CZECH PX UCITS ETF","ISIN":"BGCZPX003174","WKN":"000A2JAG6","Mnemonic":"CZX","MIC Code":"XETR","Currency":"EUR","Instrument Type":"ETF","Product Assignment Group Description":"EXCHANGE TRADED FUNDS - PASSIV","Primary Market MIC Code":"XBUL","First Trading Date":"2024-07-25"}
```

ISIN domiciles: IE 2139, LU 791, DE 265, XS 214, JE 69, GB 67, CH 55, FR 46.
Currency: EUR 3414, USD 228, GBP 21, CHF 6, SEK 5.
`Primary Market MIC Code` distribution is a **bonus multi-listing hint**: XFRA 1206, XETR 743,
XLON 536, XDUB 535, XPAR 221, XSWX 182, XAMS 69, XBRN 39, XMIL 30 — i.e. Xetra tells you where each
ETP is primary-listed even when that venue is not Xetra.

**Caveat:** `Instrument` is a **truncated ~25-char name** ("EXPAT BUL.SOFIX UCITS ETF"), not the
legal fund name. Use Euronext/LSE for the long name and Xetra only for the German ticker+WKN.

**Also verified:** `RDF_StaticData_xetr.zip`
(`.../resource/blob/1542/1cc3f1330393238478e1c967099d3e31/data/RDF_StaticData_xetr.zip`,
7,025,830 bytes) — contains only order profiles / trading schedules / TES profiles
(`20260810_orderProfiles.csv` etc.), **no instrument list**. Not useful for the universe.

**Note:** the blob URL is content-addressed by the hash, **not by the filename** — swapping the
filename to `t7-xfra-...` or `t7-xeur-...` returns the identical XETR file (byte-identical, 4,508,731).
So there is **no Frankfurt (XFRA) equivalent at this URL**; only XETR. The hash may rotate, so
scrape the Downloads page for the current link rather than hardcoding it.

**ToS / robots:** `cashmarket.deutsche-boerse.com/robots.txt` contains a sitemap line and **no
`Disallow` at all**. These are the officially published public reference files.

---

### 2.3 US listings — Nasdaq Trader symbol directory  — **VERIFIED, preferred over the screener API**

```
GET https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt
```
(also mirrored at `ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt`)

* No key, no headers. 990,012 bytes, pipe-delimited, 13,119 lines.
* Header: `Nasdaq Traded|Symbol|Security Name|Listing Exchange|Market Category|ETF|Round Lot Size|Test Issue|Financial Status|CQS Symbol|NASDAQ Symbol|NextShares`
* **Column 6 (`ETF`) = `Y`** → **5,587 ETFs.** (Careful: it is field index 6 1-based / 5 0-based,
  *not* 8 — my first `awk $8` gave a nonsense 33.)

**Real sample rows (verbatim):**

```
Y|AAA|Alternative Access First Priority CLO Bond ETF|P| |Y|100|N||AAA|AAA|N
Y|AAAA|Amplius Aggressive Asset Allocation ETF|Z| |Y|100|N||AAAA|AAAA|N
Y|AAAC|Columbia AAA CLO ETF|P| |Y|100|N||AAAC|AAAC|N
```

Listing exchange breakdown of the 5,587: **P** = NYSE Arca 2,680, **Z** = Cboe BZX 1,574,
**Q** = Nasdaq 1,257, **N** = NYSE 76.

This single file therefore already covers **NYSE + NYSE Arca + Nasdaq + Cboe**, which removes the
need for separate NYSE and Cboe listed-fund files.

**Fields present:** ticker, full security name, listing exchange, ETF flag. **No ISIN, no issuer,
no currency** (all USD), no TER/AUM.

**ToS:** the SymbolDirectory files are Nasdaq's official free public distribution (same files as the
public FTP). No robots.txt on the host (returns a 404 page).

---

### 2.4 Nasdaq screener API `api.nasdaq.com` — **VERIFIED but NOT recommended**

```
GET https://api.nasdaq.com/api/screener/etf?download=true
GET https://api.nasdaq.com/api/screener/etf?tableonly=true&limit=25&offset=0
Header required: User-Agent: <a browser UA>   (a default curl UA is rejected)
```
* Works, HTTP 200, 1,153,466 bytes for the full download. `totalrecords: 5263`.
* JSON path to rows: `data.records.data.rows` (note: for `download=true` the shape is
  `data.data.rows` — **`data.records` does not exist**, my first parse KeyError'd on it).
* **Fields are only:** `oneYearPercentage, symbol, companyName, lastSalePrice, netChange,
  percentageChange, deltaIndicator`. **No ISIN, no issuer, no exchange, no currency, no TER, no AUM.**
* `tableonly=false` adds a `data.filters` block (8 filter definitions) but the row schema is identical.

**Real sample:**
```json
{"oneYearPercentage":"21.86%","symbol":"ABFL","companyName":"Abacus FCF Leaders ETF","lastSalePrice":"$83.86","netChange":"+0.06","percentageChange":"+0.07%","deltaIndicator":"up"}
{"oneYearPercentage":"12.28%","symbol":"ABEQ","companyName":"Absolute Select Value ETF","lastSalePrice":"$38.85","netChange":"+0.06","percentageChange":"+0.15%","deltaIndicator":"up"}
{"oneYearPercentage":"4.36%","symbol":"AADR","companyName":"AdvisorShares Dorsey Wright ADR ETF","lastSalePrice":"$83.89","netChange":"-0.359","percentageChange":"-0.43%","deltaIndicator":"down"}
```

**Why not recommended:** `https://api.nasdaq.com/robots.txt` is explicitly
```
User-agent: *
Disallow: /
```
It is an undocumented internal API, robots-disallowed, requires UA spoofing, has fewer rows (5,263)
and strictly fewer fields than `nasdaqtraded.txt` (5,587). **Use §2.3 instead.**
`https://api.nasdaq.com/api/quote/SPY/info?assetclass=etf` also works but returns **no ISIN**.

---

### 2.5 SEC — Investment Company Series and Class Information  — **VERIFIED (issuer source for US)**

```
GET https://www.sec.gov/files/investment/data/other/investment-company-series-class-information/investment-company-series-class-2026.csv
GET https://www.sec.gov/files/investment/data/other/investment-company-series-and-class-information/investment_company_series_class.csv   (rolling "current")
Header REQUIRED: User-Agent: "<app name> <contact email>"
```
Note the two path spellings differ (`-series-class-information` for 2023+ vs
`-series-and-class-information` for the older/rolling files). The year-file path I first guessed
(`.../investment_company_series_class_2026.csv`) **404s**.

* 8,051,163 bytes, 43,123 rows, comma CSV with UTF-8 BOM.
* Columns: `Reporting File Number, CIK Number, Entity Name, Entity Org Type, Series ID, Series Name,
  Class ID, Class Name, Class Ticker, Address_1, Address_2, City, State, Zip Code`
  (the rolling file uses lowercase names: `rep_file_num, CIK, entity_name, …, class_ticker_symbol`).
* 28,845 rows carry a ticker.

**Measured value: joins to 4,330 of the 5,587 US ETF tickers = 77.5% issuer coverage.**

**Real sample (join output, ticker | Entity Name | Series Name | CIK):**
```
AAA  | Investment Managers Series Trust II   | Alternative Access First Priority CLO Bond ETF | 0001587982
AAAA | EA Series Trust                       | Amplius Aggressive Asset Allocation ETF        | 0001592900
AAAC | Columbia ETF Trust I                  | Columbia AAA CLO ETF                           | 0001551950
```
Top trusts among matched ETFs: iSHARES TRUST 368, PROSHARES TRUST 151, Tidal Trust II 146,
FIRST TRUST EXCHANGE-TRADED FUND VIII 136, Direxion Shares ETF Trust 125, Global X Funds 114.

The missing ~22.5% are the ETPs that are **not** registered investment companies — commodity grantor
trusts (GLD, SLV), commodity pools, and all ETNs. Those need a fallback (name-prefix mapping).

**Caveat:** `Entity Name` is the *legal trust*, not the brand. "Tidal Trust II" is a white-label
platform hosting dozens of unrelated brands; "Investment Managers Series Trust II" likewise. So this
gives a legally correct issuer but you will still want a brand-name normalisation layer.

**SEC rate limits (important):** SEC enforces **max 10 requests/second** and requires a declared
`User-Agent` containing a contact address. I actually tripped it during this recon —
`https://www.sec.gov/robots.txt` returned the *"Request Rate Threshold Exceeded"* HTML page rather
than a robots file. Throttle and back off.

---

### 2.6 SEC `company_tickers.json` — **VERIFIED DEAD END for ETFs**

```
GET https://www.sec.gov/files/company_tickers.json     (795,627 bytes, 10,398 entries)
```
Sample: `{"0":{"cik_str":1045810,"ticker":"NVDA","title":"NVIDIA CORP"}, …}`

**Only 179 of the 5,587 US ETF tickers appear (3.2%).** ETFs are *series/classes* of a trust, not
registrants, so they are almost entirely absent from this registrant-level file. Use §2.5 instead.

---

### 2.7 Yahoo Finance screener — **VERIFIED (best global enrichment, no ISIN)**

Two endpoints. The predefined one is easy but capped; the POST one is the real tool.

**(a) Predefined (no auth):**
```
GET https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=top_etfs_us&count=250&start=0
```
Works, but `total = 519` and it hard-stops (offset 900 returns 0 rows). US-only. Not sufficient.

**(b) Full screener — requires a crumb + cookie:**
```
1. GET https://fc.yahoo.com                                  -> sets the A1/A3 cookies (returns an HTTP error; that's fine, keep the cookies)
2. GET https://query1.finance.yahoo.com/v1/test/getcrumb     -> e.g. "X4Exi94.YCz"
3. POST https://query2.finance.yahoo.com/v1/finance/screener?crumb=<crumb>&lang=en-US&region=US&formatted=false
   Content-Type: application/json
   {"size":250,"offset":0,"sortField":"fundnetassets","sortType":"DESC","quoteType":"ETF",
    "query":{"operator":"AND","operands":[{"operator":"eq","operands":["region","us"]}]},
    "userId":"","userIdType":"guid"}
```
Without the crumb you get `{"finance":{"error":{"code":"Unauthorized","description":"Invalid Crumb"}}}`.

**Verified totals per region (`quoteType:ETF`):**
```
de 27124 | us 5913 | gb 5752 | ca 3246 | it 2297 | kr 1956 | nl 1691 | ch 1482
fr 836 | au 793 | jp 476 | tw 377 | hk 344 | se 28 | in 0 | br 0
```
Pagination verified to offset 5000 (250 rows each); returns 0 at offset 9000 — so it pages to about
the stated `total`, capped near ~8–9k per query. Split by exchange to go deeper.

**Real sample rows:**
```json
{"symbol":"VTI","longName":"Vanguard Morningstar Total Stock Market ETF","fullExchangeName":"NYSEArca","exchange":"PCX","currency":"USD","quoteType":"ETF","netAssets":2289978570000.0,"netExpenseRatio":0.03}
{"symbol":"VOO","longName":"Vanguard S&P 500 ETF","fullExchangeName":"NYSEArca","exchange":"PCX","currency":"USD","quoteType":"ETF","netAssets":1686884320000.0,"netExpenseRatio":0.03}
{"symbol":"IEO","longName":"iShares U.S. Oil & Gas Exploration & Production ETF","fullExchangeName":"Cboe US","exchange":"BTS","currency":"USD","quoteType":"ETF","netAssets":573560450.0}
```

**Why it matters:** it is the only free, global, single-schema source I verified that carries
**`netExpenseRatio` (TER)** and **`netAssets` (AUM)** — the two fields no exchange file has.
**But there is no ISIN anywhere in the response**, and `fundFamily` was `null` on the rows I sampled,
so issuer is unreliable here.

**Caveats:** the `de = 27,124` figure is *listing* count — Germany multiplies each ETF across
XETRA/Frankfurt/Stuttgart/Munich/Berlin/Düsseldorf/Hamburg/Hanover with `.DE/.F/.SG/.MU/.BE/.DU/.HM/.HA`
suffixes. Do **not** treat these as distinct funds. Unofficial/undocumented API, no SLA, crumb
rotates, aggressive polling gets rate-limited (HTTP 429). Yahoo's ToS prohibits redistribution of
their data — safe as an internal enrichment/cross-check, risky to republish verbatim.

---

### 2.8 OpenFIGI `/v3/mapping` — **VERIFIED (the multi-listing expander)**

```
POST https://api.openfigi.com/v3/mapping
Content-Type: application/json
[{"idType":"ID_ISIN","idValue":"IE00B4L5Y983"}, …]
X-OPENFIGI-APIKEY: <key>   (optional, free)
```

**Verified to work with NO API key.** Real response for `IE00B4L5Y983` (iShares Core MSCI World):

```json
{"figi":"BBG000P71QK5","name":"ISHARES CORE MSCI WORLD","ticker":"IWDA","exchCode":"NA","compositeFIGI":"BBG000P71PV5","securityType":"ETP","marketSector":"Equity","shareClassFIGI":"BBG001T5K109","securityType2":"Mutual Fund","securityDescription":"IWDA"}
{"figi":"BBG000PH98P0","name":"ISHARES CORE MSCI WORLD","ticker":"SWDA","exchCode":"LN","compositeFIGI":"BBG000PH98M3","securityType":"ETP","marketSector":"Equity","shareClassFIGI":"BBG001T5K109","securityType2":"Mutual Fund","securityDescription":"SWDA"}
{"figi":"BBG000PJSYB3","name":"ISHARES CORE MSCI WORLD","ticker":"IWDA","exchCode":"EO","compositeFIGI":"BBG000PJSYB3","securityType":"ETP","marketSector":"Equity","shareClassFIGI":"BBG001T5K109","securityType2":"Mutual Fund","securityDescription":"SWDA"}
```

One ISIN fanned out to **20+ venue-level FIGIs** (exchCode NA/LN/EO/XH/XF/XE/XJ/XL/XG/XO/XA…).

**Rate limits (from openfigi.com/api/documentation):**

| | requests | jobs per request | effective |
|---|---|---|---|
| `/v3/mapping` **no key** | 25 / minute | 10 | 250 ISIN/min → 15k/hour |
| `/v3/mapping` **with free key** | 25 / **6 seconds** | 100 | 25,000 ISIN/min |
| `/v3/search`, `/v3/filter` no key | 5 / minute | — | — |
| `/v3/search`, `/v3/filter` with key | 20 / minute | — | — |

A full 8,000-ISIN universe maps in **~20 seconds with a free key**, or ~32 min without one. Get the key.

**Critical limitation:** OpenFIGI **accepts** `ID_ISIN` as input but **never returns an ISIN** in the
output (Bloomberg does not redistribute ISIN). So it cannot solve the US-ISIN gap (§4.1). It is
excellent for: ISIN → every ticker+exchange it trades on, plus `securityType:"ETP"` as an ETF filter,
plus `shareClassFIGI` which is a genuinely useful **share-class grouping key**.

**Licensing:** FIGI is issued under an open licence (OMG standard); the API is free with no
daily/weekly/monthly volume cap per their own page.

---

### 2.9 GLEIF — ISIN→LEI mapping + LEI golden copy — **VERIFIED (issuer LEI; weak for US ISIN)**

**ISIN→LEI file (the useful one):**
```
GET https://mapping.gleif.org/api/v2/isin-lei?page[size]=2
    Header: Accept: */*        <-- MUST NOT send "Accept: application/json" (returns HTTP 406)
-> {"data":[{"attributes":{"fileName":"isin-lei-20260810T071513.zip",
     "downloadLink":"https://mapping.gleif.org/api/v2/isin-lei/f1805508-…/download"}}], …}
GET <downloadLink>   -> 32,054,786 byte zip
```
* Regenerated **daily** (2,669 historical files listed). Each file is a **full cumulative snapshot**,
  not a delta.
* Inner file `lei-isin-20260810T071513.csv`, 310,032,953 bytes, **9,118,616 rows**, columns `LEI,ISIN`.

**Real sample (verbatim first rows):**
```
LEI,ISIN
00EHHQ2ZHDCFXJCPCL46,US92204Q1031
00KLB2PFTM3060S2N216,US4138382027
029200038B4L4ZI1E579,NGSDCBANCO00
```
ISIN prefix distribution: DE 4,193,272 | US 2,281,092 | GB 647,483 | CH 615,499 | NL 484,295 |
FR 211,584 | IT 145,677 | SE 115,607 | ES 105,681 | **IE only 23,205**.

**Golden copy files (LEI → names and relationships):**
```
GET https://goldencopy.gleif.org/api/v2/golden-copies/publishes?format=json&page[size]=1
```
returns today's three full files (verified 2026-08-10):

| file | records | zip size | contains |
|---|---|---|---|
| `lei2` | 3,398,502 | 475.37 MB | every LEI's legal name + address |
| **`rr`** | **484,003** | **23.08 MB** | **relationships — the one you want** |
| `repex` | 6,289,463 | 58.18 MB | reporting exceptions |

**`rr` is the issuer solution — I downloaded and joined it myself.**
`.../2026/08/10/1262225/20260810-1600-gleif-goldencopy-rr-golden-copy.csv.zip`
(24,202,828 bytes → 241,128,968 byte CSV). Relationship types measured:
```
IS_FUND-MANAGED_BY            148,992   <-- ETF -> asset manager
IS_ULTIMATELY_CONSOLIDATED_BY 132,427
IS_DIRECTLY_CONSOLIDATED_BY   126,271
IS_SUBFUND_OF                  72,991
IS_INTERNATIONAL_BRANCH_OF      1,941
IS_FEEDER_TO                    1,381
```
Join `Relationship.StartNode.NodeID` (the fund LEI from FIRDS `Issr`) →
`Relationship.EndNode.NodeID` (the manager LEI), then `lei2` for the manager's legal name.

**Measured result: 7,378 of 7,837 FIRDS ETFs (94.1%) resolve to a fund manager — entirely offline,
from one 23 MB download.** Do **not** make 7,800 individual `api.gleif.org` calls.

Note: the FIRDS `Issr` LEI is the **sub-fund's own LEI** (5,518 distinct LEIs for 7,837 ETFs), so
`IS_FUND-MANAGED_BY` is what yields the real issuer brand. The REST equivalent
`GET https://api.gleif.org/api/v1/lei-records/{lei}/fund-manager` works
(e.g. `549300QS4Q1IT6XCA514` → `5493004330BCAPB3GT42` BLACKROCK ASSET MANAGEMENT IRELAND LIMITED),
but `/direct-parent` returns **404** for funds — use `fund-manager`.

**Licensing:** GLEIF LEI data is published **CC0 / free of charge, no restriction on use** — the most
permissive licence of any source here. `mapping.gleif.org/robots.txt` = `User-agent: * / Disallow:`
(i.e. everything allowed).

**Verified coverage test on US ETFs:** searched all 9.1M rows for known ETF ISINs —
found **SPY (`US78462F1030` → `549300NZAMSJ8FXPQQ63`)** and **QQQ (`US46090E1038` → `549300VY6FEJBCIMET58`)**,
but **IVV and VOO are absent**. Coverage is issuer-dependent and incomplete. See §4.1.

---

### 2.10 Third-party open datasets — **VERIFIED**

| Source | Rows | ISIN | Ticker | Issuer | Exch | Ccy | Licence |
|---|---|---|---|---|---|---|---|
| **JerBouma/FinanceDatabase** (⭐8.3k) | 36,480 across 52 per-exchange CSVs | 7,931 (22%) | YES | YES (`family`) | YES + MIC | YES | **MIT** |
| **albertored/etfdb** | 4,488 | **100%** | YES | YES | no | YES | ⚠️ **NO LICENCE FILE** |
| **adanosorg/…ticker-database** (HuggingFace) | 18,175 ETF | 16,153 | YES | no | label only | no | **MIT** |

**FinanceDatabase** — `https://raw.githubusercontent.com/JerBouma/FinanceDatabase/main/database/etfs/LSE.csv`
(last pushed 2026-08-09). Header:
`symbol,name,currency,summary,category_group,category,family,exchange,mic,isin`. Real row:
```
ACWD.L,SPDR MSCI ACWI UCITS ETF,USD,"…",Alternatives,Blend,State Street Global Advisors,LSE,XLON,IE00BF1B7389
```
Best use: **issuer-brand cross-check** (`family` is the marketing brand, which SEC's legal trust
name is not) and US-listed coverage. Caveats: ~19k German regional-venue rows have almost no ISIN;
the `summary` column has mojibake.

**albertored/etfdb** — `https://raw.githubusercontent.com/albertored/etfdb/main/csv/basic_info.csv`
(1,099,937 bytes). Richest free European UCITS metadata: **TER, replication method, acc/dist, AUM**,
100% ISIN coverage. **But it ships no LICENSE file** → treat as internal reference/validation only,
do not redistribute in the published database until licensing is clarified.

---

### 2.11 Commercial API free tiers — **VERIFIED**

| API | Works free? | Result |
|---|---|---|
| **Twelve Data** | ✅ **no key needed** | `GET https://api.twelvedata.com/etf` → 15.3 MB, **58,014 ETFs**. Fields: `symbol, name, currency, exchange, mic_code, country, figi_code, cfi_code`. **ISIN is paywalled** — every one of the 58,014 rows returns `"request_access_via_add_ons"`. Free plan 8 credits/min, 800/day. ToS: non-commercial. |
| **Alpha Vantage** | ✅ works with `apikey=demo` | `LISTING_STATUS` → 14,279 rows, **5,703 ETFs**. `symbol,name,exchange,assetType,ipoDate,delistingDate,status`. **US-only, no ISIN.** Real row: `AAAU,Goldman Sachs Physical Gold ETF,BATS,ETF,2018-08-15,null,Active`. Own free key = 25 req/day. |
| **FMP** | ❌ key required | `/api/v3/etf/list` and `/stable/etf-list` → `"Invalid API KEY"` with no key and with `demo`. Free tier = 250 calls/day, US-only. Whether `/etf/list` is inside the free tier is **UNVERIFIED**. |
| **EODHD** | ❌ blocked | `exchange-symbol-list/US` and `exchanges-list` → **HTTP 403 Forbidden** with the demo token (whitelisted to a handful of tickers). |

**Twelve Data is the interesting one, and I re-verified it myself.**
`GET https://api.twelvedata.com/etf` → HTTP 200, 15,294,764 bytes, `count: 58014`, no key, no headers.
Fields: `symbol, name, currency, exchange, mic_code, country, figi_code, cfi_code, isin, cusip`.

Real rows (verbatim):
```json
{"symbol":"0000D0","name":"Mirae Synthetic Navi Tcall Balance ETF","currency":"KRW","exchange":"KRX","mic_code":"XKRX","country":"South Korea","figi_code":"BBG01R8V5G47","cfi_code":"CECJLU","isin":"request_access_via_add_ons","cusip":"request_access_via_add_ons"}
{"symbol":"0000H0","name":"Samsung Kodex India Nifty Mutual Fund","currency":"KRW","exchange":"KRX","mic_code":"XKRX","country":"South Korea","figi_code":"BBG01T8D72X0","cfi_code":"CECJEU","isin":"request_access_via_add_ons","cusip":"request_access_via_add_ons"}
```
Country spread: Germany 18,072 · US 11,341 · UK 6,743 · Canada 3,672 · Italy 3,544 · Switzerland 2,695
· China 1,653 · South Korea 1,584 · France 1,251 · Netherlands 1,210 · Mexico 1,054 · Australia 923.

**Confirmed: `isin` and `cusip` are `"request_access_via_add_ons"` on all 58,014 rows** — paywalled.

> **But `figi_code` is populated on every row, and it is free.** That is the workaround: OpenFIGI
> (§2.8) turns our ISINs into FIGIs, and Twelve Data keys every one of its 58,014 global listings by
> FIGI. **Join on FIGI** and Twelve Data becomes a free ISIN-linked source of APAC, Canadian, Chinese,
> Korean and Mexican listings — exactly the regions no exchange file above reaches. This is the
> cheapest route to non-European, non-US coverage.

---

### 2.12 Other exchanges — **VERIFIED**

#### London Stock Exchange — ✅ two working free sources

**(a) Price Explorer JSON API — best:**
```
POST https://api.londonstockexchange.com/api/v1/components/refresh
Content-Type: application/json      (no key, no cookie, no referer)
{"path":"live-markets/market-data-dashboard/price-explorer",
 "parameters":"categories%3DETFS",
 "components":[{"componentId":"block_content:9524a5dd-7053-4f7a-ac75-71d12db796b4",
                "parameters":"categories%3DETFS%26size%3D1000%26page%3D0"}]}
```
`totalElements = 5,281` ETP lines (one row per TIDM/currency). Page size honoured to **1000** →
**6 requests for everything**. Fields: `isin, tidm, name, issuername, issuercode, currency, category,
lastprice, marketcapitalization`. **No TER, no AUM.** Real rows:
```json
{"name":"21SHARES BITCOIN CORE ETP","tidm":"CBTC","isin":"CH1199067674","currency":"GBP","issuername":"21SHARES AG","midPrice":11.3275}
{"name":"21SHARES BITCOIN CORE ETP","tidm":"CBTU","isin":"CH1199067674","currency":"USD","issuername":"21SHARES AG","midPrice":16.06}
```
Companion: `GET https://api.londonstockexchange.com/api/gw/lse/instruments/alldata/{TIDM}` → adds
`sedol, segment, lipperId, instrumenttype, listingadmissiondate`
(`IUSA` → `IE0031442068`, sedol `3144206`, `ISHARES`, GBX, segment `EUET`).

**(b) Monthly Instrument list XLSX — reference-data grade:**
`GET https://docs.londonstockexchange.com/sites/default/files/reports/Instrument%20list_79.xlsx`
(8.4 MB). Sheets `1.3 ETFs` (3,413) + `2.2 ETCs` (307) + `2.3 ETNs` (843) = **4,563 ETPs**.
Header on row 9: `TIDM | Issuer Name | Instrument Name | ISIN | MiFIR Identifier Code | Start date |
Country of Incorporation | Trading Currency | LSE Market | FCA Listing Category | Market Segment Code`
```
ARAW | ABRDN III ICAV          | ABRDN ARAW UCITS ETF - GBX | IE000J7QYHD8 | ETFS | Ireland     | GBX
GLDA | AMUNDI PHYSICAL METALS  | AMUNDI PHYSICAL GOLD ETC   | FR0013416716 | ETCS | France      | GBX
ABTC | 21SHARES AG             | 21SHARES BITCOIN ETP       | CH0454664001 | ETNS | Switzerland | GBP
```
⚠️ **No stable "latest" URL** — the `_N` index increments monthly (`_79` = Jun 2026; `_81`+ → 404).
A loader must HEAD-walk the index. The older `list_of_etfs_and_etps_securities_N.xls` is **frozen at
Sep 2020 — dead**.

#### SIX Swiss Exchange — ✅ fully open, and it carries TER

```
GET https://www.six-group.com/fqs/ref.json      (or ref.csv — same params, ';'-delimited)
    ?select=<cols>&where=ProductLine=ET&page=N
```
No key, no cookie, no referer. `ProductLine=ET` → **2,277 ETFs**; `ProductLine=EP` → **283 ETPs**.
Schema is **334 columns** (`select=*` reveals them); useful ones: `ISIN, ValorSymbol, ShortName,
FundLongName, IssuerNameFull, TradingCurrency, FundCurrency, **ManagementFee** (= TER %),
MarketCode (MIC), ListingSegmentDesc, AmountInIssue, ValorNumber`.
```
ISIN;ValorSymbol;ShortName;FundLongName;IssuerNameFull;TradingCurrency;ManagementFee;MarketCode
CH0008899764;CSSMI;iSh SMI (CH) CHF D;iShares SMI ETF (CH);BlackRock Asset Management Schweiz AG;CHF;0.35;XSWX
IE00B4L5Y983;SWDA;iSh Cor MSCI Wld USD A;iShares Core MSCI World UCITS ETF (Acc);iShares III plc;USD;0.2;XSWX
```
⚠️ **`pageSize` is hard-capped at 50** and larger values are silently ignored; the pagination param is
**`page=N`** (`pageNumber`/`offset` silently return page 1). Full pull = 46 pages in **12 s**, no
rate limiting. **Encoding is ISO-8859-1, not UTF-8.** Responses carry
`"copyRight":"(c) Copyright by SIX Group Ltd 2026. All rights reserved."`, `delayMinutes: 15`.

> This resolves the "UNVERIFIED / totalRows: 0" state both agents initially hit — the missing piece
> was `where=ProductLine=ET`.

#### Borsa Italiana direct — ✅ worth it *only* as a TER/issuer enrichment

`https://www.borsaitaliana.it/borsa/etf/lista.html` → **404** (all `/borsa/etf/*` list URLs 404).
**Working:** `GET https://www.borsaitaliana.it/etf/etf/infoproviders.xlsx` (264 KB, no auth,
`Last-Modified: 05 Jan 2026` — **~7 months stale**). Sheet `ETFplus instrument list`: 4,005 rows,
**2,221 populated** (ETF 1,576 · ACTIVE ETF 261 · ETN 202 · ETC 155 · STRUCTURED ETF 27).
Columns: `ISIN | Name | Mnemonic | Benchmark | **Annual fee** | Dividend frequency | Benchmark area |
Sub category | **Issuer group name** | Tick size | ESG classification`
```
IE0003IT72N9 | AXA ACT BIODIVERSITY EQ UCITS | ABIE | — | 0.53 | Capitalization | EQUITY THEMATIC | ACTIVE ETF | AXA IM | ESG art.8
IE00018U4PN8 | AXA IM Emerg Mkts Credit PAB U | AICU | ICE EMRG MARKETS CORP PARIS AL | 0.34 | Capitalization | CORPORATE BOND - EMERGING | ETF | AXA IM | ESG art.8
```
**Adds over the Euronext ETFplus rows:** `Annual fee` (TER) on all 2,221, `Issuer group name`,
benchmark index name, ESG/SFDR classification, and ETF/ACTIVE/ETC/ETN segmentation.
Lacks currency and MIC. **Join on ISIN over Euronext; do not use as a source of truth.**

#### Rest of world

| Venue | Verdict | Detail |
|---|---|---|
| **HKEX** | ✅ best-in-class | `GET https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx` (1.38 MB, **daily**, no auth). `Category = "Exchange Traded Products"` → **412 rows** (ETF 373, L&I 39). Has **ISIN** + trading currency. Real: `02800 \| TRACKER FUND \| HK2800008867 \| HKD`, `02801 \| ISHARES CHINA \| HK2801040828 \| HKD`. No issuer/TER. |
| **Nasdaq Nordic** | ✅ tiny | `nasdaqomxnordic.com` **retired** (301); `DataFeedProxy.aspx` dead. Replacement: `GET https://api.nasdaq.com/api/nordic/screener/etp?category=ETF` → **32 rows** only (+6 AIF). Has ISIN + **TER**. Real: `{"fullName":"XACT OMXS30 ESG (UCITS ETF)","isin":"SE0000693293","currency":"SEK","totalExpenseRatio":"0.10"}`. Latin-1 mojibake. |
| **Wiener Börse** | ⚠️ HTML only | `/en/market-data/etfs-etps/` → 404. Working: `https://www.wienerborse.at/en/exchange-traded-funds/` — server-rendered table, 50 rows/page × 8 ≈ **400 ETFs** with **ISIN**. `per-page=500` ignored. No JSON/CSV/XLS. Real: `AMUNDI 0-6M EUR INV UCITS ETF \| FR0010754200`. |
| **JPX (Japan)** | ⚠️ **no ISIN** | (a) `data_j.xls` → 4,444 rows, **476** with `ETF・ETN`; Japanese names, 4-digit code, no ISIN. (b) better: `https://www.jpx.co.jp/english/equities/products/etfs/issues/01.html` — **408 rows** with English name, **Management Company** and **Trust Fee**. Real: `1308 \| Listed Index Fund TOPIX \| Amova Asset Management \| 0.046`. Still **no ISIN**. |
| **TSX (Canada)** | ⚠️ thin | `GET https://www.tsx.com/json/company-directory/search/tsx/%5E*` → **2,266 results**, but only `symbol` + `name`. **No ISIN, no issuer, no ETF flag** — ETFs identifiable only by name heuristics. A dedicated TSX ETF file was **not found — UNVERIFIED**. |
| **ASX (Australia)** | ❌ | The markitdigital `companies/directory/file` token URL returns 1,839 rows but it is the **company** directory, not ETPs. `/funds/directory/file` and `/etp/directory/file` → **404**. **No ETP list found — UNVERIFIED.** |
| **BME (Spain)** | ❌ | `bolsamadrid.es/esp/aspx/ETFs/ETFs.aspx` and `/ing/` both **301 → marketing pages**, 0 rows. `bmerf.es` likewise. **No endpoint located.** BME is SIX-owned and the SIX `fqs` schema exposes `BMEInstrumentTypeCode`/`BMEProductLine`, so BME may be reachable via SIX — **UNVERIFIED**. |

---

### 2.13 justETF — ✅ technically trivial, ❌ **explicitly disallowed. Do not use.**

**The endpoint works.** The old `POST /servlet/etfs-table` is **dead** (301 → page-not-found).
Current path:
```
1. GET  https://www.justetf.com/en/search.html?search=ETFS      -> JSESSIONID/AWSALB cookies,
                                                                   scrape `fetchCallbackUrl` from HTML
2. POST https://www.justetf.com/en/search.html?0-1.0-container-…-etfsTablePanel&search=ETFS&_wicket=1
   Content-Type: application/x-www-form-urlencoded; charset=UTF-8
   X-Requested-With: XMLHttpRequest
   draw=1&start=0&length=5000&lang=en&country=DE&universeType=private&defaultCurrency=EUR&etfsParams=search%3DETFS%26query%3D
```
HTTP 200, `recordsFiltered: 3567`, and **`length=5000` returns all 3,567 rows in ONE 4.2 MB request.**
**42 fields** including `isin, ticker, wkn, valorNumber, name, **ter**, **fundSize** (AUM €m),
fundCurrency, domicileCountry, distributionPolicy, replicationMethod, inceptionDate,
numberOfHoldings, sustainable, savingsPlanReady`, plus full return/volatility series.
```json
{"isin":"IE00B5BMR087","ticker":"SXR8","wkn":"A0YEDG","name":"iShares Core S&P 500 UCITS ETF USD (Acc)","ter":"0.07%","fundSize":"135,504","domicileCountry":"Ireland","distributionPolicy":"Accumulating","replicationMethod":"Full replication","numberOfHoldings":"503"}
{"isin":"IE00B4L5Y983","ticker":"EUNL","wkn":"A0RPWH","name":"iShares Core MSCI World UCITS ETF USD (Acc)","ter":"0.20%","fundSize":"128,828","distributionPolicy":"Accumulating","replicationMethod":"Optimized sampling","numberOfHoldings":"1,281"}
```

**But the ToS position is unambiguous.** `https://www.justetf.com/robots.txt`, verbatim:
```
User-agent: *
Allow: /
Disallow: /servlet/
Disallow: /*/search.html*_wicket=1*
```
→ **Both data endpoints are named explicitly.** This is not incidental blanket blocking.

`/en/terms.html` is 404; the real terms are at `/en/about/legal-terms.html` → PDF
`justETF_general_terms_and_conditions.pdf`. **Operative clause 3.1, verbatim:**

> **"3.1 The user undertakes to refrain from anything that could impair the operability or one or more
> functionalities or the infrastructure of justETF. This includes, in particular, putting justETF
> under excessive strain as well as using programs to carry out automated price inquiries."**

Also relevant: the T&Cs contain **no** separate database-rights/redistribution section (the only
copyright clause, 3.2, covers *user-uploaded* content), so the prohibition rests on 3.1 plus
robots.txt. And justETF's own data is **licensed third-party** — the legal notice credits
"Xignite, Inc., etfinfo and justETF GmbH", with an MSCI clause: *"Without prior written permission of
MSCI, this information … may only be used for your internal use, may not be reproduced or
re-disseminated in any form."*

**Verdict: EXCLUDE from the pipeline.** One request would give 3,567 rows with TER and AUM, which is
exactly what the database is missing — which is precisely why it is tempting and why it must be
resisted for an *open* database. Take TER from **SIX** (2,277), **Borsa Italiana** (2,221),
**Nasdaq Nordic** (32), **JPX** (408) and **Yahoo** instead; between them they cover most of the
same funds legitimately.

---

## 3. Proposed ingestion strategy

### 3.1 Order of ingestion

**Stage A — the spine (4 requests, no key, no auth, ~12 MB total):**
1. **ESMA FIRDS `FULINS_C`** → 7,837 ETF ISINs + issuer LEI + currency + venue MICs. *This is the
   authoritative fund list.* Whitelist regulated-market MICs (caveat §2.0.2).
2. **Euronext CSV** → 4,174 EU listings; supplies the **ticker/symbol** and full names FIRDS lacks,
   plus Borsa Italiana.
3. **Xetra CSV** → 3,676 ETPs; supplies German ticker + WKN, and the **ETC/ETN** lines FIRDS
   `FULINS_C` omits (caveat §2.0.3).
4. **`nasdaqtraded.txt`** → 5,587 US ETF listings (ticker, name, listing exchange).

**Stage B — identifier expansion (OpenFIGI, free key):**
* Feed every distinct ISIN → `(ticker, exchCode)` for all venues, plus `shareClassFIGI`.
* Feed the **2,333 US-prefixed FIRDS ISINs with `exchCode:"US"`** → recovers 42% of US ISINs (§4.1).
* This also back-fills venues you never scraped (LSE, SIX, BME, Vienna) without touching them.

**Stage C — additional venues (all verified working, all keyless):**
* **LSE** Price Explorer API (6 requests → 5,281 ETP lines with ISIN + issuer) — the single biggest
  addition after Stage A, since FIRDS covers UK poorly post-Brexit.
* **SIX Swiss** `fqs/ref.csv?where=ProductLine=ET` (46 pages, 12 s → 2,277 ETFs **with TER**).
* **HKEX** `ListOfSecurities.xlsx` (412 ETPs with ISIN), **Wiener Börse** (~400, HTML),
  **Nasdaq Nordic** (32).
* **Twelve Data `/etf` joined on FIGI** for APAC/Canada/LatAm — cheaper than JPX/TSX/ASX scraping,
  since TSX and ASX have **no verified ISIN-bearing endpoint** at all.

**Stage D — attribute enrichment:**
* issuer, EU → FIRDS `Issr` LEI → **GLEIF `rr` relationship file** (`IS_FUND-MANAGED_BY`), which
  resolved **7,351 / 7,810 ETFs (94%)** to an asset manager **offline, in one 24 MB download**.
  Do *not* make 7,800 individual `api.gleif.org` calls.
* issuer, US → SEC series/class CSV (77.5% hit) + name-prefix fallback for grantor trusts/ETNs.
* issuer brand cross-check → FinanceDatabase `family` column (MIT licensed).
* **TER** → SIX `ManagementFee` (2,277) ∪ Borsa Italiana `Annual fee` (2,221) ∪ JPX `Trust Fee` (408)
  ∪ Nasdaq Nordic `totalExpenseRatio` (32) ∪ Yahoo `netExpenseRatio` (global fallback).
* **AUM** → Yahoo `netAssets`; SIX `AmountInIssue` as a cross-check.
  (justETF would give TER+AUM for 3,567 funds in one request but is **excluded** — see §2.13.)

### 3.2 Deduplication model

The single most important design decision: **an ETF is not a row, it is three nested things.**
Model them as three tables, not one.

```
fund          (share class)   PK = ISIN                  <- the real fund identity
  |                            + share_class_figi (OpenFIGI) groups ACC/DIST/hedged classes
  |                            + name, issuer, domicile, currency_base, TER, AUM, inception
  |
  +--< listing (venue line)   PK = (ISIN, MIC)           <- the multi-listing grain
                               + ticker, MIC, exchange_name, trading_currency, figi
```

**Rules:**

1. **Primary key = ISIN.** Every EU source gives it directly. Merge Euronext ∪ Xetra ∪ LSE ∪ SIX ∪
   FIRDS on ISIN.
2. **Listing key = `(ISIN, MIC)`.** The same ISIN legitimately appears on many venues
   (IE00B4L5Y983 = IWDA on Amsterdam, SWDA on London, plus ~18 more per OpenFIGI). These are
   **not duplicates** — they are distinct listing rows pointing at one fund row.
   Beware: Euronext's `Market` column sometimes holds a *multi-venue* string
   ("Euronext Amsterdam, Paris", 34 rows) — split these into two listing rows.
   Also treat XETR vs XFRA vs XSTU as distinct MICs but the same fund.
3. **Ticker is NOT a key.** The same ticker means different funds on different venues
   (IWDA is Amsterdam *and* London). Only `(ticker, MIC)` is unique.
4. **US rows have no ISIN** → key them on `(ticker, MIC)` and carry `isin = NULL`, with a
   `us_synthetic_id` = `US:{listing_exchange}:{ticker}`. Do **not** invent an ISIN.
5. **Currency:** distinguish `trading_currency` (per listing — the same ISIN trades EUR in Paris and
   USD in Amsterdam multi-currency) from `base_currency` (per fund). The exchange files give the
   former only.
6. **Conflict precedence for shared fields** (measured quality order):
   name → Euronext (full legal name) > LSE > Xetra (truncated to ~25 chars, use last);
   currency → the venue file for that listing;
   issuer → SEC (US) / GLEIF LEI (EU) > name-prefix heuristic.
7. **Deletions:** all these files are full snapshots. Diff on ISIN each run and mark
   `delisted_at` rather than hard-deleting — ETFs close constantly.

### 3.3 Measured overlap and realistic final count

These are **measured**, not estimated:

```
Euronext unique ISIN        : 3,240
Xetra    unique ISIN        : 3,522
  overlap                   : 2,294   (71% of Euronext is also on Xetra)
  union                     : 4,468
FIRDS ETF ISINs             : 7,837
  covers of Euronext u Xetra: 3,613   (80.9%)
  FIRDS-only                : 4,224
  Euronext/Xetra not in FIRDS:  855   (= ETC/ETN, CFI "D", live in FULINS_D)
GRAND UNION (European)      : 8,692
```

| Layer | Unique fund IDs | Running total | Basis |
|---|---|---|---|
| FIRDS ∪ Euronext ∪ Xetra | 8,692 | **8,692** | **measured** |
| + LSE (4,563 ETPs; **UK is post-Brexit, NOT in ESMA FIRDS**) | +600–1,200 net new | ~9,300–9,900 | source count measured, overlap estimated |
| + SIX Swiss (2,277 ETF + 283 ETP) | +150–400 net new | ~9,450–10,300 | source count measured |
| + Wiener (~400) / Nordic (32) | +50–150 | ~9,500–10,450 | source counts measured |
| + US ETFs (keyed by ticker; 2,333 gain a real ISIN) | +5,587 | **~15,100–16,000** | **measured** |
| + HKEX (412) / JPX (408) / TSX / KRX / Twelve Data tail | +1,500–3,000 | ~16,600–19,000 | partly measured |

> **Note on LSE:** the UK left the ESMA regime, so `FULINS_C` does **not** cover LSE-only listings —
> the LSE's 4,563 ETPs are largely additive, not redundant. (The FCA runs its own UK FIRDS at
> `data.fca.org.uk` — **UNVERIFIED**, but worth testing as a direct LSE alternative.)

**Realistic target: ~14,500 fund-level rows for Europe + US** (already at the top of the requested
10,000–15,000 band), rising to **~16,000–18,000** if you add APAC and Canada.

At **listing** grain, measured rather than guessed:
```
FIRDS regulated-market listings (RMKT only)  : 18,150
US listings (nasdaqtraded)                   :  5,587
                                               ------
European + US listing rows                   : ~23,700
```
(vs. **102,660 + 5,587** if you naively keep every FIRDS venue MIC — don't, see §2.0.2.)

Sanity check: justETF tracks ~3,500 *European UCITS ETFs*, but that counts **funds**, not share
classes. FIRDS's 7,837 counts **ISINs** — ACC/DIST/EUR-hedged/USD-hedged are separate ISINs — so
the two numbers are consistent, and 7,837 is the right grain for a database keyed on ISIN.

Sanity check against a known benchmark: justETF tracks ~3,500 *European UCITS ETFs*, but that counts
**funds**, not share classes. Counting ISINs (ACC/DIST/EUR-hedged/USD-hedged are separate ISINs) and
including ETCs/ETNs, ~6,500–8,000 European ISINs is the consistent number — and my measured
Euronext ∪ Xetra = 4,468 from only two venues supports that.

---

## 4. Dead ends and partial failures

### 4.1 US ETF ISINs — **partially solved (~42%); no free source for the rest**

US ISIN = `US` + CUSIP + check digit, and **CUSIP is licensed by CUSIP Global Services (paid)**.

#### What DOES work — FIRDS → OpenFIGI (**VERIFIED, ~42% coverage, free**)

Any US ETF that is admitted to trading or reported on *any* EU venue gets a FIRDS record carrying
its real US ISIN. `FULINS_C` contains **2,333 US-prefixed ETF ISINs**. Feed them to OpenFIGI
constrained to the US exchange and you get the US ticker back exactly:

```
POST https://api.openfigi.com/v3/mapping
[{"idType":"ID_ISIN","idValue":"US78467Y1073","exchCode":"US"}, …]
```

**Measured on a 40-ISIN sample: 40/40 resolved, and 40/40 of the returned tickers were real
entries in `nasdaqtraded.txt`. 100% precision.** Real output:

```
US78467Y1073 -> MDY    State Street SPDR S&P MIDCAP 400 ETF Trust
US00326A1043 -> SGOL   abrdn Physical Gold Shares ETF
US33737M3007 -> FYC    First Trust Small Cap Growth AlphaDEX Fund
US92189F8251 -> BRF    VanEck Brazil Small-Cap ETF
US46138B1035 -> DBC    Invesco DB Commodity Index Tracking Fund
US78464A3674 -> SPLB   State Street SPDR Portfolio Long Term Corporate Bond ETF
```

2,333 ISINs = 24 requests with a free OpenFIGI key (~10 s), or 234 requests / ~10 min without.
**This covers roughly 2,333 of 5,587 US ETFs (≈42%)** — the large, internationally-distributed ones.
The remainder are US-domestic-only ETFs that no EU venue reports, and for those there is genuinely
no free ISIN source.

(Naive name-matching FIRDS `FullNm` against `nasdaqtraded` security names only achieved
**85/5,587 = 1.5%** — FIRDS names are too abbreviated. Use OpenFIGI, not fuzzy names.)

#### What does NOT work — the SEC → GLEIF chain (**measured non-viable**)

I attempted a four-hop free workaround and **measured it to be non-viable**:

```
nasdaqtraded ticker
  -> SEC series/class CSV      (ticker -> SERIES_ID)
  -> SEC N-CEN FUND_REPORTED_INFO (SERIES_ID -> LEI)
  -> GLEIF ISIN-LEI            (LEI -> ISIN)
```
Verified numbers at each hop (N-CEN 2026q2,
`https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2026q2_ncen.zip`, 8,404,110 bytes):
* `FUND_REPORTED_INFO.tsv`: 2,335 fund rows, **696 with `IS_ETF = Y`**, 2,055 SERIES_ID→LEI pairs.
  (Useful columns confirmed present: `FUND_NAME, SERIES_ID, LEI, IS_ETF, IS_INDEX,
  MONTHLY_AVG_NET_ASSETS, MANAGEMENT_FEE, NET_OPERATING_EXPENSES`.)
* GLEIF has an ISIN for **only 160 of those 2,048 LEIs (7.8%)**.
* **End-to-end result: 42 of 5,587 US ETFs resolved = 0.8%.**

Sample of the 42 that did resolve:
```
AAAA | Amplius Aggressive Asset Allocation ETF   | S000090747 | 529900BBEQVFL3E7I651 | US02072Q6897
BBAG | JPMorgan BetaBuilders U.S. Aggregate Bond | S000063668 | 549300Y7I30NRICF3S33 | US46641Q2416
BCHI | GMO Beyond China ETF                      | S000088547 | 52990010I07PLVDP2C96 | US90139K2096
```

Even running all four quarters of N-CEN (each fund files annually, so 4 quarters ≈ 2,800 ETFs) the
7.8% GLEIF hit rate caps this at roughly **200–400 US ISINs out of 5,587**. **Abandon this chain.**
Accept `isin = NULL` for US listings, or budget for a commercial identifier feed.

### 4.2 Other confirmed dead ends

| Attempt | Result |
|---|---|
| `https://live.euronext.com/en/pd/data/etf?mics=…` | HTTP 200 but `{"iTotalRecords":null,"aaData":[]}` — **wrong path**, use `/en/product_directory/data/etf-all-markets/` |
| Euronext `filter_jsongateway` (`/en/product_directory/filter/etf-all-markets`) | **HTTP 500** every attempt — cannot harvest the `issuerGroup` facet |
| Euronext per-ETF product page for issuer/TER | JS-rendered; HTML exposes only `"issuer_code":"153127"`, no issuer name, no TER |
| Xetra `RDF_StaticData_xetr.zip` | 7 MB but contains **only** order profiles / trading schedules / TES profiles — **no instrument list** |
| Xetra `t7-xfra-…` / `t7-xeur-…` filename variants | Blob is hash-addressed; returns the **byte-identical XETR file**. No Frankfurt equivalent. |
| `xetra.com/resource/blob/1528/8a2b0d9c/xetra-instruments.csv` | **404** — guessed URL, does not exist |
| SEC `company_tickers.json` for ETFs | **179 / 5,587 = 3.2%** — ETFs are series, not registrants. Dead end. |
| SEC `investment_company_series_class_2026.csv` (the "and" path) | **404** — the 2026 file lives under `-series-class-information` (no "and") |
| SEC `/files/structureddata/data/…nport.zip` | **404** — real path is `/files/dera/data/form-n-port-data-sets/…` |
| Cboe `listed_symbols/csv/` | HTTP 200 but it is a **volume/quote statistics** file (`Name,Volume,Ask Size,…`), no ETF flag, no ISIN. Superseded by `nasdaqtraded.txt` (Cboe = `Z`, 1,574 ETFs). |
| `cdn.cboe.com/api/global/us_equities/listed_symbols.csv` | **403** |
| Cboe `symbol_book`, `etp/etp_products/csv` | **404** |
| `api.boerse-frankfurt.de/v1/search/…` | **403 "Invalid CORS request"** — needs computed `X-Client-TraceId`/`X-Security` headers |
| `stockanalysis.com/api/screener/e/f?…` | **404** — endpoint shape wrong/changed |
| Yahoo predefined screener `top_etfs_us` | Hard cap at **519** rows, US-only. Use the crumb-authenticated POST instead. |
| Yahoo POST screener without crumb | `{"code":"Unauthorized","description":"Invalid Crumb"}` |
| OpenFIGI as an ISIN *source* | Accepts ISIN as input, **never returns ISIN**. Cannot fix §4.1. |
| `goldencopy.gleif.org/api/v2/isin-lei-files` | **404** — ISIN mapping lives on `mapping.gleif.org`, not `goldencopy` |
| `mapping.gleif.org` with `Accept: application/json` | **HTTP 406** — must send `Accept: */*` |
| `nasdaqtraded.txt` field 8 as the ETF flag | Wrong column — gives 33. **ETF flag is field 6.** |
| **HuggingFace `search=etf`** | **100% noise** — podcast transcripts, price time-series, VHDL homework. `paperswithbacktest/ETFs-Daily-Price` is **gated**. The one real hit (`adanosorg/…`) was findable only by *full-text* search, not name search. |
| **data.gouv.fr / AMF** | AMF publishes exactly **5 datasets**, none a fund universe. ⚠️ Trap: the v1 API `?q=` silently returns `{"total":0}` for *every* query — use `/api/2/datasets/search/?q=`. AMF **GECO** is an Angular SPA over an undocumented `/back-office/v0/` API with client-side SheetJS export only — no bulk export (**UNVERIFIED**). |
| **data.europa.eu** | "ETF" resolves to *European Training Foundation*. Its FIRDS record has `license: null`, `modified: 2017-12-21`, and **only HTML pointers, no `download_url`** — a catalogue, not a host. Go direct to `registers.esma.europa.eu`. |
| **GitHub search** | `q=ucits+etf+list` → **0 repos**. `q=etf+isin` → 12, all tiny. `hstsethi/in-isin-db` `/data` holds only `.gitkeep`; `JustETF-Scraper-API` `/resources` only `etf.html`. Every justETF repo is scraper code with **no committed data**. |
| **Twelve Data / EODHD for ISIN** | Twelve Data returns `"request_access_via_add_ons"` for `isin` and `cusip` on all 58,014 rows. EODHD demo token → **403**. |
| **FMP `/etf/list`** | `"Invalid API KEY"` with no key and with `demo`. Free tier is 250 calls/day and US-only. |
| **justETF `POST /servlet/etfs-table`** | **Dead** — 301 → `/en/page-not-found.html`. Current endpoint works but is robots-disallowed (§2.13). |
| **`borsaitaliana.it/borsa/etf/lista.html`** | **404**; all `/borsa/etf/*` list URLs 404. Use `/etf/etf/infoproviders.xlsx`. |
| **LSE `list_of_etfs_and_etps_securities_N.xls`** | Exists but **frozen at Sep 2020** — dead. Use `Instrument list_N.xlsx`. |
| **`nasdaqomxnordic.com` / `DataFeedProxy.aspx`** | Site **retired** (301 → nasdaq.com); proxy dead. Use `api.nasdaq.com/api/nordic/screener/etp`. |
| **ASX ETP directory** | `/funds/directory/file` and `/etp/directory/file` → **404**. The token URL that does work returns the **company** directory, not ETPs. |
| **BME / bolsamadrid.es ETF pages** | Both `/esp/` and `/ing/` **301 → marketing pages**, 0 rows. No endpoint located. |
| **TSX company-directory JSON** | 2,266 results but **only symbol + name** — no ISIN, no issuer, no ETF flag. |
| **Wiener Börse `/en/market-data/etfs-etps/`** | **404**; the working page is `/en/exchange-traded-funds/`, HTML-only, 50 rows/page hard cap. |
| **SIX `fqs` with no `ProductLine` filter** | Returns `totalRows: 0` silently. The working filter is `where=ProductLine=ET`. |
| **SIX `pageSize=5000`** | Silently ignored — hard-capped at 50. Paginate with `page=N` (`pageNumber`/`offset` silently return page 1). |
| **`six-group.com/.../etf-explorer.html`** | **404** — the ETF Explorer page path has moved; go straight to `/fqs/ref.json`. |

### 4.3 Top risks (ranked)

**1. Venue-MIC explosion in FIRDS (highest impact, certain to bite).**
FIRDS returns 102,660 (ISIN, MIC) pairs for 7,837 funds — ~13 venues each — because it includes
Bloomberg MTF (BTFE, 6,550 ISINs), Tradeweb (TWEM, 5,558), systematic internalisers and every German
regional exchange. If you load these as "listings" your table is ~10× too large and mostly not
tradeable venues. **Mitigation:** whitelist regulated-market MICs from the ISO 10383 register
(keep `OPRT` operating MICs / drop `APPA` SI segments), and treat FIRDS MICs as *eligibility*
evidence rather than as listing rows.

**2. US ETFs have no free ISIN for ~58% of the universe (structural, unfixable for free).**
CUSIP is a paid licence. FIRDS→OpenFIGI recovers ~2,333 of 5,587 (42%); the SEC→GLEIF chain measured
0.8% and is not worth building. **Mitigation:** design the schema so `isin` is nullable and the fund
key is a surrogate ID, with `(ticker, MIC)` as the US natural key. Do **not** synthesise ISINs.
Budget for a commercial identifier feed if full US ISIN coverage is a hard requirement.

**3. Undocumented endpoints will break without notice (medium impact, high likelihood).**
The Xetra blob URL is content-hash-addressed and will rotate. Euronext's endpoint lives in a Drupal
settings blob that a redesign would move. Yahoo needs a rotating crumb and its ToS forbids
redistribution. `api.nasdaq.com` is `robots: Disallow: /`. **Mitigation:** anchor the pipeline on
the two sources with stable contracts and explicit reuse licences — **ESMA FIRDS** (regulator,
weekly, versioned filenames, reuse authorised) and **GLEIF** (CC0) — and treat every exchange
scrape as a best-effort enrichment layer that is allowed to fail without breaking the build.
Checksum-verify FIRDS downloads (the Solr doc ships an MD5).

### 4.4 Licensing summary — what you may actually republish

For an **open** ETF database this matters as much as coverage.

| Source | Licence position | Safe to republish? |
|---|---|---|
| **ESMA FIRDS** | *"Reproduction … authorised … provided the source is acknowledged"* | ✅ **Yes**, with attribution |
| **GLEIF** (LEI, ISIN-LEI, rr) | **CC0**, explicitly free of charge, no restriction | ✅ **Yes**, unconditionally |
| **JerBouma/FinanceDatabase** | **MIT** | ✅ Yes, with licence notice |
| **adanosorg ticker-database** | **MIT** | ✅ Yes |
| **OpenFIGI** | Open FIGI standard, free API, no volume cap | ✅ Yes (FIGI/ticker; it returns no ISIN) |
| **SEC** files | US Government work, public domain | ✅ Yes |
| **Nasdaq Trader SymbolDirectory** | Official free public distribution | ⚠️ Likely yes; no explicit grant found |
| **Euronext** CSV | No explicit reuse grant; EOD/delayed by design; robots blocks only GPTBot on `/product/` | ⚠️ Reference fields low-risk; do not republish prices |
| **Deutsche Börse Xetra** | No explicit reuse grant; robots.txt has no Disallow | ⚠️ Same posture as Euronext |
| **LSE / LSEG** | Delayed ≥15 min; LSEG ToS applies; no explicit reuse grant | ⚠️ Reference fields low-risk; no prices |
| **SIX Swiss** | Every response stamps *"(c) Copyright by SIX Group Ltd 2026. All rights reserved."* | ⚠️ Undocumented public endpoint; attribute, no prices |
| **Borsa Italiana / HKEX / Wiener / JPX** | Public files, no explicit grant | ⚠️ Reference fields only |
| **Yahoo Finance** | ToS **prohibits redistribution** | ❌ Internal enrichment only |
| **Twelve Data** | ToS says free tier is **non-commercial** | ❌ Internal only |
| **albertored/etfdb** | **No LICENSE file at all** | ❌ Internal only until clarified |
| **justETF** | **robots.txt `Disallow`s both data endpoints**; T&C 3.1 bans "programs to carry out automated price inquiries"; underlying data licensed from Xignite/etfinfo/MSCI | ❌ **Do not scrape at all** |

**Recommended publishable core:** FIRDS + GLEIF + OpenFIGI + SEC + FinanceDatabase. That combination
is fully attributable and reusable, and on its own delivers ISIN, name, issuer, currency, venue MICs
and tickers for the entire European universe plus the internationally-distributed US ETFs.

### 4.5 Operational cautions

* **SEC**: hard 10 req/s limit and a mandatory contact-bearing `User-Agent`; I tripped it during
  this recon (robots.txt returned the throttle page).
* **`api.nasdaq.com`**: `robots.txt` = `Disallow: /`. Avoid; `nasdaqtrader.com` is the sanctioned path.
* **Xetra blob hash** may rotate — scrape the Downloads page for the current link, don't hardcode.
* **Yahoo**: undocumented, crumb rotates, ToS forbids redistribution. Internal enrichment only.
* **Euronext/Xetra/Yahoo prices are EOD/delayed.** Store reference data; do not present as real-time.
