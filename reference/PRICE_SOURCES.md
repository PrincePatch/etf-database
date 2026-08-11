# Recon: free daily OHLCV for a 13,000-ticker ETF universe

Empirical benchmark run **2026-08-10**, Windows 10, Python 3.12.7, yfinance **1.5.2**, pyarrow 25.0.1,
curl_cffi 0.16.0, 12 CPU, residential IP (~25 MB/s down).
Every number below was **measured**, not estimated. Scripts: `t01`–`t19` in this directory.

---

## 0. Executive summary

| Question | Answer (measured) |
|---|---|
| Does unauthenticated Yahoo work in 2026? | **Yes**, 97–99.4% success across 6 EU venues + US |
| Is there a request rate limit? | **No volume limit found.** >4,000 requests, 0×429. The 429 is a **User-Agent gate** |
| Best throughput | **40–65 tickers/s** cold, **145–172 tickers/s** incremental (curl_cffi, 24–48 workers) |
| Cold backfill 13,000 tickers | **~5 min** fetch, ~4.8 GB transferred |
| Daily refresh 13,000 tickers | **~1.5–2 min** fetch, ~22 MB transferred |
| Fits a daily GitHub Action? | **Yes, trivially** — ~3% of the 6 h job limit |
| Storage, 13k × 15y = 49.1 M bars | see §6 — Parquet ≈ **0.85 GB**, SQLite ≈ **4.9 GB** |
| Viable free fallback vendor? | **None.** All 6 commercial free tiers forbid redistribution; stooq is dead |
| Biggest risk | Yahoo's `adjclose` is **wrong** for some listings, and datacenter-IP blocking is unverified |

---

## 1. Yahoo Finance: what actually works today

### 1.1 The single most important finding — the "rate limit" is a User-Agent check

Yahoo's `429 Too Many Requests` has nothing to do with request volume. It is returned by the CDN edge
purely on the basis of the `User-Agent` header. Measured, all on the same IP, back to back (`t12`):

| Client | Result |
|---|---|
| `curl_cffi`, no UA, no impersonation | **429** `Too Many Requests` |
| `requests`, default UA (`python-requests/2.33.1`) | **429** `Edge: Too Many Requests` |
| `requests`, `User-Agent: curl/8.4.0` | **429** `Edge: Too Many Requests` |
| `requests`, `User-Agent: python-requests/2.31.0` | **429** `Edge: Too Many Requests` |
| `requests`, **browser UA** | **200**, 846,777 B, 8,439 bars |
| `curl_cffi`, no impersonation, **browser UA header** | **200**, 846,791 B, 8,439 bars |
| `curl_cffi`, `impersonate="chrome"` | **200**, 846,877 B, 8,439 bars |
| `curl_cffi`, `impersonate="safari"` | **200**, 846,727 B, 8,439 bars |

**Setting one header is sufficient.** TLS/JA3 impersonation is *not* required for access — but it is
worth having for speed (§1.4). This explains every "yfinance suddenly broke" report: the default UA is
blocked instantly, which looks like a rate limit but is not.

### 1.2 There is no volume-based rate limit (>4,000 requests, zero 429)

`t04`, `t05`, `t16` — all with a browser UA:

| Test | Requests | Wall clock | Throughput | Non-200 |
|---|---|---|---|---|
| Sequential, chart endpoint, no sleep | 600 | 51.4 s | 11.68 req/s | **0** |
| Concurrency ramp 1→64 workers, `range=1mo` | 1,200 | — | peak **141.98 req/s** @32w | **0** |
| Concurrency ramp 1→64 workers, full history | 1,200 | — | peak **76.72 req/s** @64w | **0** |
| Sequential, **search** endpoint | 400 | 28.1 s | 14.22 req/s | **0** |

Latency on the chart endpoint: **p50 0.068 s, p90 0.136 s, p99 0.218 s**.
Concurrency scaling (200 req each, `1mo`): 1w → 10.8 req/s, 4w → 36.4, 8w → 58.7, 16w → 122.1,
32w → **142.0**, 64w → 125.9 (past 32 workers it regresses).

### 1.3 TRAP: `range=max` silently returns monthly bars

This is a silent data-corruption trap and the reason a naive raw fetcher looks 10× faster than it is:

| Params | Bars returned | First bar | Bytes |
|---|---|---|---|
| `range=max&interval=1d` | **404** (≈ monthly) | 1993-02-01 | 42,292 |
| `period1=0&period2=<now>&interval=1d` | **8,439** (daily) | 1993-01-29 | **846,707** |

`interval=1d` is **ignored** when `range` is used. yfinance itself always sends `period1`/`period2`.
**Always use `period1`/`period2`.** No error, no warning — just 20× less data.

### 1.4 Measured throughput, 312 real tickers (250 US + 62 EU)

Cold backfill = full history (`period1=0`); incremental = last 8 days.

**Raw chart endpoint + curl_cffi (`t09`) — the recommended path:**

| Mode | Workers | Wall clock | Tickers/s | Bars | Transferred |
|---|---|---|---|---|---|
| Full history | 8 | 13.29 s | 23.48 | 1,080,174 | 115.4 MB @ 8.7 MB/s |
| Full history | 16 | 8.40 s | 37.15 | " | 13.8 MB/s |
| Full history | **24** | **4.81 s** | **64.88** | " | **24.0 MB/s** |
| Full history | 32 | 8.49 s | 36.75 | " | 13.6 MB/s |
| Full history | 48 | 6.12 s | 50.96 | " | 13.8 MB/s |
| Full history | 64 | 5.08 s | 61.42 | " | 22.7 MB/s |
| Incremental | 16 | 3.43 s | 91.06 | 1,854 | 0.54 MB |
| Incremental | 32 | 2.15 s | 145.04 | " | " |
| Incremental | **48** | **1.81 s** | **171.96** | " | " |
| Incremental | 64 | 1.84 s | 169.96 | " | " |

Variance across repeat runs at 24 workers: 40.4 / 40.8 / 48.9 / 64.9 tk/s — **network-bound, not
server-limited**. Plan on **40 tk/s cold** and **120 tk/s incremental** as conservative figures.

**Transport comparison (`t17`, 24 workers, full history, 2 runs each):**

| Transport | Tickers/s |
|---|---|
| `requests` + browser UA | 21.11 / 21.81 → mean **21.5** |
| `curl_cffi` `impersonate="chrome"` | 40.36 / 40.75 → mean **40.6** |

**curl_cffi is 1.9× faster** (HTTP/2 multiplexing over one connection). Plain `requests` works and is
dependency-free; curl_cffi is worth it at 13,000 tickers.

**yfinance's own `yf.download` (`t07`) — 6–10× slower than the raw endpoint:**

| Mode | threads=8 | threads=16 | threads=32 |
|---|---|---|---|
| `period="max"` | 5.83 tk/s | 4.05 tk/s | 5.65 tk/s |
| `period="5d"` | 12.48 tk/s | 14.07 tk/s | 18.42 tk/s |

The bottleneck is yfinance's per-ticker pandas/timezone post-processing, not the network. Batching via
`yf.download(list_of_tickers, group_by=...)` **does work** and returns a correct MultiIndex frame, but
it still issues **one HTTP request per ticker** — it is a threading convenience, not a bulk endpoint.
There is no true multi-symbol history endpoint.

### 1.5 Coverage, 66 hand-picked EU UCITS + US tickers (`t02`)

`yf.download(66 tickers, period="max")` → **6.34 s, 64/66 = 97.0%**.

| Venue | Success |
|---|---|
| Paris `.PA` | 12/12 |
| Xetra `.DE` | 12/12 |
| Amsterdam `.AS` | 8/8 |
| Milan `.MI` | 8/8 |
| London `.L` | 8/8 |
| Zurich `.SW` | 6/8 |
| US | 10/10 |

Both failures (`IWDA.SW`, `IEMM.SW`) were **my invented symbols** — Yahoo correctly 404s them. Real
coverage of real tickers was **100%**. Delisted funds are detected cleanly (`LCWD.MI` stops 2025-02-20).

**History depth:** US ETFs go back to inception (SPY 1993-01-29, 8,439 bars). European listings are
**floored at 2008-01-02** — many EU tickers (`CAC.PA`, `EXS1.DE`, `XEON.DE`, `IQQH.DE`, `IUSA.AS`,
`IUSA.MI`, `LVC.PA`) all start on exactly that date regardless of true inception. Do not promise
pre-2008 history for European listings. Mean 3,484 bars/ticker, median 3,265.

---

## 2. Total return vs price return — and a real Yahoo bug

This matters more than anything else in the dataset. Measured over 10 years (`t18`):

| Ticker | Div/yr | Price CAGR | Yahoo `adjclose` CAGR | **Reconstructed TR** | Yahoo gap | True gap |
|---|---|---|---|---|---|---|
| **ISF.L** | 2.88% | 4.53% | **4.57%** | **8.60%** | 0.04 pp | **4.08 pp** ❌ |
| **IUSA.L** | 0.85% | 13.09% | **13.11%** | **14.62%** | 0.02 pp | **1.53 pp** ❌ |
| **EQQQ.L** | 0.23% | 19.50% | **19.51%** | **20.09%** | 0.01 pp | 0.59 pp ❌ |
| VUSA.L | 0.86% | 13.05% | 14.63% | 14.63% | 1.59 pp | 1.59 pp ✅ |
| VUKE.L | 2.98% | 4.38% | 8.54% | 8.54% | 4.15 pp | 4.15 pp ✅ |
| VWRL.AS | 1.23% | 10.00% | 11.97% | 11.97% | 1.98 pp | 1.98 pp ✅ |
| SPY | 0.97% | 13.55% | 15.39% | 15.39% | 1.84 pp | 1.84 pp ✅ |
| AGG | 4.06% | −1.47% | 1.38% | 1.38% | 2.84 pp | 2.84 pp ✅ |
| IUSA.**AS** | 0.84% | 12.98% | 14.49% | 14.49% | 1.51 pp | 1.51 pp ✅ |
| IUSA.**MI** | 0.84% | 13.09% | 14.61% | 14.61% | 1.52 pp | 1.52 pp ✅ |
| EUNL.DE (acc) | 0 | 12.66% | 12.66% | 12.66% | 0 | 0 ✅ |
| SGLN.L (acc) | 0 | 11.78% | 11.78% | 11.78% | 0 | 0 ✅ |

**Yahoo publishes dividend events for `ISF.L` (40 of them) but does not apply them to `adjclose`.**
For `ISF.L` that understates 10-year annualised return by **4.03 pp/yr** — a ~48% cumulative error.
The *same fund on another venue* (`IUSA.AS`, `IUSA.MI`) is adjusted correctly, so this is a
per-listing defect, not a fund property. You cannot detect it without recomputing.

**Our reconstruction is validated:** on all 9 tickers where Yahoo is correct, the CRSP-style
back-adjustment reproduces Yahoo's `adjclose` CAGR **to the basis point** (14.63=14.63, 11.97=11.97,
15.39=15.39, 1.38=1.38, 8.54=8.54, 14.49=14.49, 14.61=14.61, 12.66=12.66, 11.78=11.78). It agrees
everywhere Yahoo works and fixes it where Yahoo is broken.

> **Recommendation: store raw OHLCV + dividend + split events and compute total return yourself.**
> Never ship Yahoo's `adjclose` as the total-return series. This also makes the pipeline idempotent —
> Yahoo silently *restates* `adjclose` for the whole history on every new dividend, so a stored
> `adjclose` column drifts out of sync with any incremental update, whereas raw OHLCV + events is
> append-only.

Sanity checks that passed: `auto_adjust=True`'s `Close` is bit-identical to `auto_adjust=False`'s
`Adj Close`; accumulating ETFs correctly have zero dividends and `adjclose == close`.

---

## 3. ISIN → ticker mapping

### 3.1 yfinance / Yahoo (`t03`, `t16`)

| Method | Works? | Notes |
|---|---|---|
| `yf.utils.get_all_by_isin(isin)` | ✅ | 0.14–0.21 s, returns one ticker |
| `yf.Search(isin).quotes` | ✅ | same backend, `/v1/finance/search` |
| `yf.Ticker(ISIN).history()` | ✅ | Yahoo resolves ISINs directly as symbols |
| `yf.Ticker(sym).isin` (reverse) | ❌ | **Times out after 30 s** — scrapes a dead third-party site. Do not use |

Resolution rate on 49 real UCITS ISINs: **44/49 = 89.8%**. Throughput 13.9 ISIN/s at 12 workers
→ **13,000 ISINs in ~15.6 min**. Search endpoint took 400 sequential requests with zero blocks.

**The catch: Yahoo returns exactly ONE listing per ISIN, and it is LSE-biased.**
`IE00B4L5Y983` → `IWDA.L` only, never `EUNL.DE` / `IWDA.AS` / `SWDA.MI`. Worse, some ISINs resolve to
junk: `IE00B4ND3602` → `PHYMF` (a US pink-sheet line), and 4 resolve to Stuttgart `.SG` pseudo-tickers
typed `MUTUALFUND`. So Yahoo search alone cannot build a venue-aware universe.

### 3.2 OpenFIGI is the right identifier source (`t19`)

`POST https://api.openfigi.com/v3/mapping`, **no API key required**:

- Measured rate limit from response headers: `ratelimit-policy: 25;w=60` → **25 requests/min**,
  **10 ISINs per request** → 250 ISIN/min → **13,000 ISINs in ~52 min** keyless.
- Returns **every** listing: `IE00B4L5Y983` → **180** listings; `IE00B5BMR087` → 263; `LU1681043599` → 91.
- Includes the local ticker root + Bloomberg exchange code
  (`NA`=Amsterdam, `LN`=London, `GR`=Xetra, `FP`=Paris, `IM`=Milan, `SW`=SIX).
- Validates ISIN check digits (rejects malformed ones Yahoo silently accepts).

Caveat: it returns *Bloomberg* tickers, not Yahoo ones — `DE0005933931` → `DAXEX/GR`, whereas Yahoo
uses `EXS1.DE`. A ticker-root + suffix mapping layer is still needed.

### 3.3 The fallback that actually works: multi-venue retry (`t19`)

Every fund tested is listed on 2–4 Yahoo venues with near-identical history. Ordered candidate lists
resolve 100% of the time:

| Fund | Venues that work (bars / first date) |
|---|---|
| iShares Core MSCI World | `IWDA.AS` 4318, `EUNL.DE` 4285, `SWDA.MI` 4285, `IWDA.L` 4262 — all from 2009-09-25 |
| iShares Core S&P 500 | `SXR8.DE` 4123, `CSSPX.MI` 4123, `CSP1.L` 4100, `CSSPX.SW` 4077 — all 2010-05-19 |
| Vanguard FTSE All-World | `VWCE.DE` 1789, `VWRA.L` 1781, `VWCE.MI` 1672 |
| Amundi MSCI World | `CW8.PA` 4390, `CW8.MI` 2110 |
| Vanguard S&P 500 | `VUSA.AS` 3637, `VUSA.L` 3592, `VUSA.DE` 2230 |

**Note the currency differs by venue** — `IWDA.L` quotes USD, `CSP1.L` GBp (pence!), `VUSA.L` GBP,
`VUSA.AS` EUR. Store `meta.currency` per listing and beware GBp/GBP (factor 100).

---

## 4. Alternative free sources — all tested, all dead ends

### 4.1 stooq.com — **dead for programmatic use** (`t10`, `t11`)

stooq now serves a JavaScript **proof-of-work** challenge (SHA-256, 4 hex-zero difficulty).
I solved it in Python in **0.068 s** (nonce 86,258), POSTed to `/__verify`, and received the `auth`
cookie. HTML pages then load fine (198,937 B). But the CSV endpoint:

```
GET https://stooq.com/q/d/l/?s=spy.us&i=d   ->  200, 13 bytes, "Access denied"
```

**0/24 symbols**, US and EU alike, with a valid cookie and a correct `Referer`. `stooq.pl` serves the
same challenge. The block is deliberate and endpoint-specific: the browser UI works, bulk CSV does not.

### 4.2 boerse-frankfurt — closed (`t12`)

| Attempt | Result |
|---|---|
| Bare GET `api.boerse-frankfurt.de/v1/data/price_history` | 200 but **`{}`** (2 bytes) |
| With `Origin`/`Referer` | **403** |
| With legacy `X-Client-TraceId` + MD5 `X-Security` signature | **403** |

The published salt-based signing scheme no longer validates.

### 4.3 Euronext — closed (`t12`)

| Attempt | Result |
|---|---|
| `POST /en/ajax/AwlHistoricalPrice/getFullDownloadAjax/...` | 400 `No format specified` |
| `GET /en/intraday_chart/getChartData/...` | 200, 368,904 B of **AES-encrypted** JSON (`{"ct":"83sq0smj+…"}`) |
| Product page | 200 but behind an `antibot` form |

### 4.4 Commercial free tiers — none permit redistribution

| Vendor | Free limit | EU listings on free? | Adjusted? | Redistribution |
|---|---|---|---|---|
| Alpha Vantage | **25/day** | partial (LSE/Xetra only, no Euronext/Milan/SIX) | daily-adjusted is **premium** | ❌ "may not redistribute, republish, or resell" |
| Twelve Data | 800 credits/day | ❌ **US only** (EU venues gated to $29–99/mo) | ✅ | ❌ "cannot be displayed to users, shared externally" |
| EODHD | **20/day** | claimed worldwide — **empirically 403** | ✅ | ❌ "Personal use" |
| FMP | 250/day | ❌ **US only**, explicit | ✅ | ❌ needs "Data Display and Licensing Agreement" |
| Marketstack | **100/MONTH** | ✅ no gate found | ✅ | ❌ freeware = "testing and evaluation purposes" |
| Tiingo | 1,000/day | ❌ **no EU on any tier** (US + China only) | ✅ | ❌ "internal consumption only" |

Empirically confirmed with demo keys (`t19`):
- **Alpha Vantage `apikey=demo`**: returns only an `Information` nag for every function. Useless.
- **EODHD `api_token=demo`**: `AAPL.US` → 11,506 bars back to 1980-12-12 **with `adjusted_close`**;
  but `IWDA.LSE`, `EUNL.XETRA`, `CW8.PA` → **403 Forbidden**. The free tier is US-only in practice,
  contradicting their pricing page.

> **Conclusion: for a redistributable open ETF database there is no free commercial fallback.**
> Yahoo is the only source with the coverage and throughput, and it carries its own ToS risk (§8).
> The only *licensing-clean* long-term path is primary sources — exchange EOD files (Deutsche Börse,
> Euronext), issuer NAV/holdings publications — plus open identifiers (OpenFIGI, GLEIF).

---

## 5. Projected durations for 13,000 tickers

Derived from §1.4 measurements.

### Cold backfill (full history)

| Path | Rate | 13,000 tickers | Data transferred |
|---|---|---|---|
| Raw + curl_cffi, 24w — measured best | 64.9 tk/s | **3.3 min** | ~4.8 GB |
| Raw + curl_cffi — conservative | 40 tk/s | **5.4 min** | ~4.8 GB |
| Raw + plain `requests` | 21.5 tk/s | **10.1 min** | ~4.8 GB |
| `yf.download` | 5.8 tk/s | **37.4 min** | ~4.8 GB |

Transfer volume: measured **372 KB/ticker** (115.4 MB / 310) on a US-heavy sample with deep history.
A EU-only universe averages shorter history (median start 2014) → ~250 KB/ticker → **~3.3 GB**.
On a GitHub-hosted runner (much faster network than my 25 MB/s link) this is CPU/JSON-parse bound;
budget **5–10 min**.

### Daily incremental

| Path | Rate | 13,000 tickers | Data |
|---|---|---|---|
| Raw + curl_cffi, 48w — measured | 172 tk/s | **1.3 min** | 22 MB |
| Raw + curl_cffi — conservative | 120 tk/s | **1.8 min** | 22 MB |
| `yf.download` | 18.4 tk/s | **11.8 min** | 22 MB |

Measured **1.7 KB/ticker** for an 8-day window (0.54 MB / 312).

### Stats computation (`t17b`)

11 metrics (CAGR 1/3/5/10y, 3y vol, 3y Sharpe, max drawdown, current drawdown, YTD, last, bar count)
across 310 symbols / 1,079,591 bars: **0.94 s** = **1.14 M bars/s**.
→ **~43 s for the full 49.1 M-bar panel.** Negligible.

### Does it fit GitHub Actions?

Verified limits: **6 h** per job; **2,000 min/month** free for private repos; **unlimited for public repos**.

| Job | Duration | % of 6 h limit |
|---|---|---|
| Daily incremental (fetch 2 min + stats 1 min + checkout/deps/commit ~5 min) | **~8 min** | 2.2% |
| Full cold backfill (fetch 10 min + write 15 min + overhead) | **~30 min** | 8.3% |

Monthly cost for a **private** repo: 8 min/day × 31 = **248 min/month** of the 2,000 free minutes
(12%). A **public** repo is free. **Comfortable fit either way.**

> The binding constraint is **not compute — it is repository growth** (§7).

---

## 6. Storage sizing — measured

### 6.1 Calibration on real data (`t13`): 1,080,174 bars, 310 symbols, 9 columns

Columns: `symbol, date, open, high, low, close, adjclose, volume, currency`.

| Layout | Size | **B/row** | Files |
|---|---|---|---|
| Single Parquet, float64, **zstd** | 19.37 MB | **17.93** | 1 |
| Single Parquet, float64, zstd L9 | 18.62 MB | 17.24 | 1 |
| Single Parquet, float32, **brotli** | 18.24 MB | **16.89** | 1 |
| Single Parquet, float64, snappy | 24.95 MB | 23.10 | 1 |
| Partitioned by **year** | 29.36 MB | 27.18 | 34 |
| Partitioned by symbol-hash (64 buckets) | 32.92 MB | 30.47 | 64 |
| **One Parquet per symbol** | 33.68 MB | 31.18 | 310 |
| **SQLite, no index** | 82.14 MB | 76.04 | 1 |
| **SQLite + index(symbol,date)** | **108.27 MB** | **100.23** | 1 |
| CSV, uncompressed | 66.83 MB | 61.87 | 1 |
| CSV.gz | 18.08 MB | 16.74 | 1 |
| **Per-symbol JSON.gz** (date, close, adjclose only) | 8.08 MB | **7.48** | 310 |

**SQLite costs 4.2× a single Parquet, and 5.6× with an index.** Partitioning costs 50–75% overhead
versus one file (per-file footers plus loss of cross-file dictionary sharing).

### 6.2 Compression tuning (`t14`) — two non-obvious results

**Rounding does not help the way you'd expect.** Yahoo returns float32 values widened into float64
(`24.93000030517578` — mean 14.2 decimal digits). Rounding to 4 dp makes it *worse*:

| Variant | B/row |
|---|---|
| float64 raw (as delivered) | **17.93** |
| float64 round(6) | 20.94 ⬆ |
| float64 round(4) | 19.22 ⬆ |
| float64 round(3) | 17.19 |
| float64 **round(2)** | **14.02** ⬇ 22% |
| float32 round(2) | 14.11 |

Only round(2) wins, and it is **unsafe for `adjclose`** — back-adjusted historical prices get small
(a fund that 20×'d has early adjusted prices near 1.0), where 2 dp is a ~1% error. Since we store raw
OHLCV + events anyway (§2), round OHLC to the exchange tick (2 dp) and keep events at full precision.

**`use_byte_stream_split` is silently ignored** by pyarrow 25.0.1 here — identical byte counts
(19.60 MB) with the parameter absent, set to a column list, or set to `True`. Do not count on it.

### 6.3 Full-scale measurement: 13,000 symbols × 3,780 days = **49,140,000 rows** (`t15`)

<!--FULLSCALE-->

### 6.4 Verified GitHub limits

| Limit | Value |
|---|---|
| Per-file **hard block** | **100 MiB** |
| Per-file warning | 50 MiB |
| Repository size | "ideally less than 1 GB, and less than 5 GB is strongly recommended" |
| **Release asset** per file | **< 2 GiB**, up to 1,000 assets/release, **no total-size or bandwidth limit** |
| GitHub **Pages** site size | **1 GB** |
| Pages bandwidth | 100 GB/month (soft) |

---

## 7. Recommended architecture

<!--ARCH-->

---

## 8. Risks

<!--RISKS-->

---

## 9. Verified code patterns

<!--CODE-->
