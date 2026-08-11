"""Generate synthetic processed tables conforming to pipeline/schema.py.

Development fixture only: the real pipeline is still being assembled, and the
web interface has to be built and measured against something at full scale.
Every table here is written through `schema.conform`, so anything that reads
these files reads exactly the shape the real producers will emit.

The distributions are tuned to the shapes the README and PEA_CTO_RULES describe
-- in particular `pea_eligible` lands at roughly 1.7% true / 22% false / 76%
null, because a frontend built against a dense boolean would be the wrong
frontend.

Writes:
  data/processed/{funds,listings,performance,returns_yearly,returns_monthly,
                  corporate_actions,broker_availability}.parquet
  data/processed/prices/part-*.parquet      (a directory dataset, as .gitignore expects)
  data/processed/_SYNTHETIC                 (marker export.py propagates to the site)

Usage:  python gen_synthetic.py [n_funds]
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(r"C:\Users\FTHIBAULT\Documents\Stage Florian\Projets\ETF\etf-database")))

from pipeline import schema  # noqa: E402
from pipeline.config import PROCESSED, RISK_FREE_RATE, TRADING_DAYS_PER_YEAR  # noqa: E402

RNG = np.random.default_rng(20260810)
N = int(sys.argv[1]) if len(sys.argv) > 1 else 13_000
TODAY = np.datetime64("2026-08-10")
PRICE_DIR = PROCESSED / "prices"

ISSUERS = [
    ("iShares", "iShares", 0.20), ("Amundi", "Amundi", 0.12),
    ("DWS", "Xtrackers", 0.11), ("Vanguard", "Vanguard", 0.08),
    ("State Street", "SPDR", 0.07), ("Invesco", "Invesco", 0.06),
    ("BNP Paribas AM", "BNP Paribas Easy", 0.04), ("UBS AM", "UBS ETF", 0.04),
    ("HSBC AM", "HSBC", 0.03), ("Franklin Templeton", "Franklin", 0.03),
    ("VanEck", "VanEck", 0.03), ("WisdomTree", "WisdomTree", 0.03),
    ("JPMorgan AM", "JPMorgan", 0.03), ("Fidelity", "Fidelity", 0.02),
    ("L&G", "L&G", 0.02), ("Ossiam", "Ossiam", 0.01),
    ("First Trust", "First Trust", 0.02), ("Global X", "Global X", 0.02),
    ("21Shares", "21Shares", 0.01), ("Tabula", "Tabula", 0.01),
    ("Charles Schwab", "Schwab", 0.01), ("ProShares", "ProShares", 0.01),
]
ISSUER_NAMES = [i[0] for i in ISSUERS]
BRANDS = [i[1] for i in ISSUERS]
ISSUER_P = np.array([i[2] for i in ISSUERS])
ISSUER_P = ISSUER_P / ISSUER_P.sum()

DOMICILES = ["IE", "LU", "FR", "DE", "NL", "AT", "CH", "GB", "US"]
DOMICILE_P = [0.40, 0.22, 0.07, 0.05, 0.02, 0.01, 0.01, 0.02, 0.20]

CURRENCIES = ["EUR", "USD", "GBP", "CHF", "JPY"]
CURRENCY_P = [0.46, 0.42, 0.07, 0.03, 0.02]

ASSET_P = {
    "equity": 0.56, "bond": 0.24, "commodity": 0.06, "money-market": 0.04,
    "multi-asset": 0.03, "real-estate": 0.02, "crypto": 0.02,
    "currency": 0.01, "unknown": 0.02,
}
STRATEGY_P = {
    "broad-market": 0.30, "sector": 0.13, "country": 0.11, "factor": 0.10,
    "thematic": 0.10, "esg": 0.11, "dividend": 0.05, "leveraged": 0.03,
    "inverse": 0.02, "covered-call": 0.02, "active": 0.02, "unknown": 0.01,
}
REGIONS = [
    "world", "usa", "north-america", "europe", "eurozone", "france",
    "emerging", "asia-pacific", "japan", "china", "india", "global-ex-usa", "unknown",
]
REGION_P = [0.20, 0.17, 0.04, 0.13, 0.06, 0.03, 0.10, 0.06, 0.04, 0.04, 0.02, 0.06, 0.05]
SECTORS = [
    "technology", "health-care", "financials", "energy", "industrials",
    "consumer-staples", "consumer-discretionary", "utilities", "materials",
    "communication-services", "real-estate",
]
INDEX_PROVIDERS = ["MSCI", "S&P Dow Jones", "FTSE Russell", "STOXX", "Bloomberg", "Solactive", "Nasdaq", "Markit"]
INDEX_BY_REGION = {
    "world": ["MSCI World", "MSCI ACWI", "FTSE All-World", "Solactive GBS Global Markets"],
    "usa": ["S&P 500", "MSCI USA", "Nasdaq-100", "Russell 1000", "CRSP US Total Market"],
    "north-america": ["MSCI North America", "S&P Total Market"],
    "europe": ["STOXX Europe 600", "MSCI Europe", "FTSE Developed Europe"],
    "eurozone": ["EURO STOXX 50", "MSCI EMU", "STOXX Europe 600 ex UK"],
    "france": ["CAC 40", "CAC Mid 60", "MSCI France"],
    "emerging": ["MSCI Emerging Markets", "FTSE Emerging"],
    "asia-pacific": ["MSCI AC Asia Pacific ex Japan", "FTSE Asia Pacific"],
    "japan": ["MSCI Japan", "TOPIX", "Nikkei 225"],
    "china": ["MSCI China", "FTSE China 50", "CSI 300"],
    "india": ["MSCI India", "Nifty 50"],
    "global-ex-usa": ["MSCI World ex USA", "FTSE All-World ex US"],
    "unknown": ["Solactive Custom", "Markit iBoxx"],
}
BOND_INDEX = [
    "Bloomberg Global Aggregate", "Bloomberg Euro Aggregate", "Markit iBoxx EUR Liquid Corporates",
    "Bloomberg US Treasury", "Markit iBoxx EUR Sovereigns", "Bloomberg Euro High Yield",
]
MIC_EEA = [
    ("XETR", "Xetra", "EUR", ".DE"), ("XPAR", "Euronext Paris", "EUR", ".PA"),
    ("XAMS", "Euronext Amsterdam", "EUR", ".AS"), ("XMIL", "Borsa Italiana", "EUR", ".MI"),
    ("XBRU", "Euronext Brussels", "EUR", ".BR"), ("XLIS", "Euronext Lisbon", "EUR", ".LS"),
    ("XMAD", "BME Madrid", "EUR", ".MC"), ("XSTO", "Nasdaq Stockholm", "SEK", ".ST"),
    ("XSWX", "SIX Swiss Exchange", "CHF", ".SW"), ("XLON", "London Stock Exchange", "GBX", ".L"),
]
MIC_US = [("XNAS", "Nasdaq", "USD", ""), ("ARCX", "NYSE Arca", "USD", ""), ("BATS", "Cboe BZX", "USD", "")]
BROKERS = ["Bourse Direct", "Boursorama", "Fortuneo", "Trade Republic", "DEGIRO", "Saxo", "Interactive Brokers"]

# Real, stable URLs -- the eligibility block links these, so they must resolve.
PEA_SOURCES = {
    "highest": "https://www.amundietf.fr/fr/particuliers/produits",
    "high": "https://live.euronext.com/fr/markets/paris/etfs/list",
    "medium": "https://www.boursedirect.fr/fr/produits/etf",
    "low": "https://www.fortuneo.fr/bourse/etf-tracker",
}


def choice(values, n, p=None):
    return np.asarray(values, dtype=object)[RNG.choice(len(values), n, p=p)]


def isin_checksum(body: str) -> str:
    """ISO 6166 check digit, so the fixture survives pipeline.isin validation."""
    digits = "".join(str(int(c, 36)) if c.isalpha() else c for c in body)
    total, double = 0, True
    for c in reversed(digits):
        d = int(c) * (2 if double else 1)
        total += d - 9 if d > 9 else d
        double = not double
    return str((10 - total % 10) % 10)


# --------------------------------------------------------------------------- #
# funds
# --------------------------------------------------------------------------- #
t0 = time.time()
domicile = choice(DOMICILES, N, DOMICILE_P)
is_us = domicile == "US"
asset_class = choice(list(ASSET_P), N, list(ASSET_P.values()))
strategy = choice(list(STRATEGY_P), N, list(STRATEGY_P.values()))
region = choice(REGIONS, N, REGION_P)
issuer_ix = RNG.choice(len(ISSUERS), N, p=ISSUER_P)
issuer = np.asarray(ISSUER_NAMES, dtype=object)[issuer_ix]
brand = np.asarray(BRANDS, dtype=object)[issuer_ix]

alphabet = np.array(list("0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"))
body = ["".join(RNG.choice(alphabet, 9)) for _ in range(N)]
isin = np.array([f"{domicile[i]}{body[i]}{isin_checksum(domicile[i] + body[i])}" for i in range(N)], dtype=object)

tick_alpha = np.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
ticker = np.array(["".join(RNG.choice(tick_alpha, RNG.integers(3, 6))) for _ in range(N)], dtype=object)

# ~58% of US ETFs have no licence-free ISIN; those rows carry the documented
# `US:{exchange}:{ticker}` surrogate instead (schema.FUNDS, `isin`).
us_exch = choice([m[0] for m in MIC_US], N)
surrogate = is_us & (RNG.random(N) < 0.58)
isin = np.where(surrogate, np.array([f"US:{us_exch[i]}:{ticker[i]}" for i in range(N)], dtype=object), isin)
_, first = np.unique(isin, return_index=True)
dup = np.ones(N, bool)
dup[first] = False
isin[dup] = np.array([f"{isin[i][:11]}{RNG.integers(0, 10)}" for i in np.flatnonzero(dup)], dtype=object)

index_name = np.array(
    [
        RNG.choice(BOND_INDEX) if asset_class[i] == "bond"
        else RNG.choice(INDEX_BY_REGION[region[i]])
        for i in range(N)
    ],
    dtype=object,
)
index_provider = choice(INDEX_PROVIDERS, N)
sector = np.where(strategy == "sector", choice(SECTORS, N), None)

replication = np.where(
    asset_class == "equity",
    choice(["physical-full", "physical-sampling", "synthetic-swap", "unknown"], N, [0.46, 0.28, 0.22, 0.04]),
    choice(["physical-full", "physical-sampling", "synthetic-swap", "unknown"], N, [0.30, 0.52, 0.12, 0.06]),
)
distribution_policy = choice(["accumulating", "distributing", "unknown"], N, [0.56, 0.42, 0.02])
dividend_frequency = np.where(
    distribution_policy == "distributing",
    choice(["quarterly", "semi-annual", "annual", "monthly"], N, [0.46, 0.30, 0.16, 0.08]),
    None,
)
leverage = np.where(
    strategy == "leveraged", choice([2.0, 3.0], N, [0.75, 0.25]),
    np.where(strategy == "inverse", choice([-1.0, -2.0], N, [0.7, 0.3]), 1.0),
).astype("float32")
esg = (strategy == "esg") | (RNG.random(N) < 0.16)
ucits = ~np.isin(domicile, ["US"]) & (RNG.random(N) < 0.985)
fund_currency = choice(CURRENCIES, N, CURRENCY_P)
currency_hedged_to = np.where(RNG.random(N) < 0.14, choice(["EUR", "CHF", "GBP"], N, [0.72, 0.16, 0.12]), None)
securities_lending = RNG.random(N) < 0.55

share = np.where(distribution_policy == "accumulating", " (Acc)", " (Dist)")
hedge_tag = np.array([f" {c} Hedged" if c else "" for c in currency_hedged_to], dtype=object)
name = np.array(
    [
        f"{brand[i]} {index_name[i]}{' UCITS' if ucits[i] else ''} ETF{hedge_tag[i]}{share[i]}"
        for i in range(N)
    ],
    dtype=object,
)
short_name = np.array([f"{brand[i]} {index_name[i]}" for i in range(N)], dtype=object)

age_days = np.clip(RNG.gamma(2.2, 1250, N).astype(int), 220, 25 * 365)
inception = TODAY - age_days.astype("timedelta64[D]")

ter = np.round(np.clip(RNG.gamma(1.9, 0.0013, N), 0.0003, 0.0195), 5).astype("float32")
aum_eur = np.round(np.exp(RNG.normal(17.4, 1.9, N)), 0)

# --- eligibility ----------------------------------------------------------- #
# Positive evidence only, and only structural disqualifiers produce False.
eea = np.isin(domicile, ["IE", "LU", "FR", "DE", "NL", "AT"])
structural_no = (~eea) | (~ucits)  # US / GB / CH domicile, or not a UCITS fund
# A true needs an EEA UCITS *and* a mechanism we could actually evidence.
could_be_pea = eea & ucits & (asset_class == "equity")
evidenced = could_be_pea & (RNG.random(N) < 0.041)

pea_eligible = np.where(structural_no, False, np.where(evidenced, True, None))
pea_true, pea_false = evidenced & ~structural_no, structural_no
pea_mechanism = np.where(
    pea_true,
    np.where(np.isin(region, ["europe", "eurozone", "france"]), "physical_eu", "synthetic_swap"),
    "unknown",
)
conf_tier = choice(["highest", "high", "medium", "low"], N, [0.22, 0.30, 0.33, 0.15])
pea_confidence = np.where(
    pea_true, conf_tier,
    np.where(pea_false, "none", np.where(could_be_pea & (RNG.random(N) < 0.30), "hint", "none")),
)
pea_source = np.array([PEA_SOURCES[pea_confidence[i]] if pea_true[i] else None for i in range(N)], dtype=object)
pea_as_of = np.where(pea_true, TODAY - RNG.integers(0, 200, N).astype("timedelta64[D]"), np.datetime64("NaT"))

# CTO: UCITS + KID + French passport. Invariant C1 -- pea true implies cto true.
has_priips_kid = np.where(is_us, False, np.where(RNG.random(N) < 0.94, True, None))
authorised_fr = np.where(is_us | (domicile == "GB"), False, np.where(RNG.random(N) < 0.88, True, None))
kid_yes, kid_no = has_priips_kid == True, has_priips_kid == False  # noqa: E712
fr_yes, fr_no = authorised_fr == True, authorised_fr == False  # noqa: E712
cto_true = ((kid_yes & fr_yes) | pea_true)
cto_false = (kid_no | fr_no) & ~cto_true
cto_accessible = np.where(cto_true, True, np.where(cto_false, False, None))
cto_reason = np.where(
    cto_true, np.where(ucits, "ucits_eea_with_kid", "in_broker_catalogue"),
    np.where(is_us, "no_priips_kid",
             np.where(domicile == "GB", "uk_ucits_is_third_country",
                      np.where(fr_no, "not_passported_to_france", "unknown"))),
)
CTO_NOTES = {
    "no_priips_kid": "Fonds américain 40-Act : pas de document d'informations clés PRIIPs, donc non commercialisable auprès d'un particulier résident en France.",
    "uk_ucits_is_third_country": "UCITS britannique : depuis le Brexit, produit de pays tiers pour la réglementation européenne.",
    "not_passported_to_france": "Part non notifiée à l'AMF pour la commercialisation en France.",
    "ucits_eea_with_kid": "UCITS de l'EEE avec DIC PRIIPs en français.",
    "in_broker_catalogue": "Présent au catalogue d'au moins un courtier français.",
    "unknown": None,
}
cto_note = np.array([CTO_NOTES.get(r) for r in cto_reason], dtype=object)

data_sources = np.array(
    [["esma-firds", "euronext", "yahoo"] if not is_us[i] else ["nasdaqtrader", "openfigi", "yahoo"] for i in range(N)],
    dtype=object,
)

funds = schema.conform(
    pa.table(
        {
            "isin": pa.array(isin, pa.string()), "name": pa.array(name, pa.string()),
            "short_name": pa.array(short_name, pa.string()), "issuer": pa.array(issuer, pa.string()),
            "brand": pa.array(brand, pa.string()), "domicile": pa.array(domicile, pa.string()),
            "ucits": pa.array(ucits, pa.bool_()), "fund_currency": pa.array(fund_currency, pa.string()),
            "inception_date": pa.array(inception, pa.date32()), "ter": pa.array(ter, pa.float32()),
            "aum_eur": pa.array(aum_eur, pa.float64()), "replication": pa.array(replication, pa.string()),
            "distribution_policy": pa.array(distribution_policy, pa.string()),
            "dividend_frequency": pa.array(dividend_frequency, pa.string()),
            "index_name": pa.array(index_name, pa.string()),
            "index_provider": pa.array(index_provider, pa.string()),
            "asset_class": pa.array(asset_class, pa.string()), "region": pa.array(region, pa.string()),
            "sector": pa.array(sector, pa.string()), "strategy": pa.array(strategy, pa.string()),
            "leverage": pa.array(leverage, pa.float32()), "esg": pa.array(esg, pa.bool_()),
            "currency_hedged_to": pa.array(currency_hedged_to, pa.string()),
            "securities_lending": pa.array(securities_lending, pa.bool_()),
            "pea_eligible": pa.array(list(pea_eligible), pa.bool_()),
            "pea_confidence": pa.array(pea_confidence, pa.string()),
            "pea_mechanism": pa.array(pea_mechanism, pa.string()),
            "pea_source": pa.array(pea_source, pa.string()),
            "pea_as_of": pa.array(pea_as_of, pa.date32()),
            "cto_accessible": pa.array(list(cto_accessible), pa.bool_()),
            "cto_reason": pa.array(cto_reason, pa.string()), "cto_note": pa.array(cto_note, pa.string()),
            "has_priips_kid": pa.array(list(has_priips_kid), pa.bool_()),
            "authorised_fr": pa.array(list(authorised_fr), pa.bool_()),
            "data_sources": pa.array(list(data_sources), pa.list_(pa.string())),
            "last_updated": pa.array(np.full(N, TODAY), pa.date32()),
        }
    ),
    "funds",
)
pq.write_table(funds, PROCESSED / "funds.parquet", compression="zstd")
n_true, n_false = int(pea_true.sum()), int(pea_false.sum())
print(f"funds  {N:,}  pea true={n_true} ({n_true/N:.2%}) false={n_false} ({n_false/N:.2%}) "
      f"null={N-n_true-n_false} ({(N-n_true-n_false)/N:.2%})  {time.time()-t0:.0f}s")

# --------------------------------------------------------------------------- #
# listings
# --------------------------------------------------------------------------- #
l_isin, l_mic, l_name, l_tick, l_yahoo, l_ccy, l_primary = [], [], [], [], [], [], []
for i in range(N):
    pool = MIC_US if is_us[i] else MIC_EEA
    k = 1 if is_us[i] else int(RNG.integers(1, 5))
    picks = RNG.choice(len(pool), min(k, len(pool)), replace=False)
    for j, p in enumerate(picks):
        mic, ex_name, ccy, suffix = pool[p]
        local = ticker[i] if j == 0 else ticker[i][:3] + RNG.choice(tick_alpha)
        l_isin.append(isin[i]); l_mic.append(mic); l_name.append(ex_name)
        l_tick.append(local); l_yahoo.append(local + suffix); l_ccy.append(ccy)
        l_primary.append(j == 0)
listings = schema.conform(
    pa.table({
        "isin": pa.array(l_isin, pa.string()), "exchange_mic": pa.array(l_mic, pa.string()),
        "exchange_name": pa.array(l_name, pa.string()), "ticker": pa.array(l_tick, pa.string()),
        "yahoo_ticker": pa.array(l_yahoo, pa.string()), "trading_currency": pa.array(l_ccy, pa.string()),
        "is_primary": pa.array(l_primary, pa.bool_()),
    }),
    "listings",
)
pq.write_table(listings, PROCESSED / "listings.parquet", compression="zstd")
print(f"listings {listings.num_rows:,}")

# --------------------------------------------------------------------------- #
# prices, and every statistic derived from them
#
# Stats are computed from the generated series rather than drawn independently:
# a table whose 1Y return contradicts the chart above it is a fixture that hides
# frontend bugs instead of exposing them.
# --------------------------------------------------------------------------- #
all_days = np.arange(np.datetime64("2001-01-01"), TODAY + 1, dtype="datetime64[D]")
dow = (all_days.astype(int) + 4) % 7
bdays = all_days[dow < 5]
bday_i32 = bdays.astype("int32")
bd_year = bdays.astype("datetime64[Y]").astype(int) + 1970
bd_month = bdays.astype("datetime64[M]").astype(int)
start_ix = np.searchsorted(bdays, inception.astype("datetime64[D]"))

PRICE_DIR.mkdir(parents=True, exist_ok=True)
for old in PRICE_DIR.glob("*.parquet"):
    old.unlink()

# Volatility and drift by asset class, so a money-market fund does not look like
# a leveraged crypto tracker.
VOL = {"equity": 0.17, "bond": 0.055, "commodity": 0.20, "money-market": 0.006,
       "multi-asset": 0.11, "real-estate": 0.19, "crypto": 0.65, "currency": 0.08, "unknown": 0.14}
DRIFT = {"equity": 0.085, "bond": 0.025, "commodity": 0.045, "money-market": 0.023,
         "multi-asset": 0.055, "real-estate": 0.05, "crypto": 0.35, "currency": 0.01, "unknown": 0.05}

perf: dict[str, list] = {f.name: [] for f in schema.PERFORMANCE}
ry_isin, ry_year, ry_ret, ry_partial = [], [], [], []
rm_isin, rm_year, rm_month, rm_ret, rm_partial = [], [], [], [], []
ca_isin, ca_date, ca_kind, ca_amount, ca_ratio, ca_ccy = [], [], [], [], [], []

WINDOWS = {"1d": 1, "1w": 7, "1m": 30, "3m": 91, "6m": 182, "1y": 365, "3y": 1095, "5y": 1826, "10y": 3653}
sqrt_ann = np.sqrt(TRADING_DAYS_PER_YEAR)

writer = None
part, part_rows, PART_MAX = 0, 0, 6_000_000
buf_isin, buf_d, buf_o, buf_h, buf_l, buf_c, buf_a, buf_v, buf_ccy = [], [], [], [], [], [], [], [], []


def flush(force=False):
    global writer, part, part_rows, buf_isin, buf_d, buf_o, buf_h, buf_l, buf_c, buf_a, buf_v, buf_ccy
    if not buf_isin or (not force and sum(len(x) for x in buf_d) < 1_000_000):
        return
    tbl = schema.conform(
        pa.table({
            "isin": pa.array(np.concatenate(buf_isin), pa.string()),
            "date": pa.array(np.concatenate(buf_d), pa.date32()),
            "open": pa.array(np.concatenate(buf_o), pa.float32()),
            "high": pa.array(np.concatenate(buf_h), pa.float32()),
            "low": pa.array(np.concatenate(buf_l), pa.float32()),
            "close": pa.array(np.concatenate(buf_c), pa.float32()),
            "adj_close": pa.array(np.concatenate(buf_a), pa.float32()),
            "volume": pa.array(np.concatenate(buf_v), pa.float32()),
            "currency": pa.array(np.concatenate(buf_ccy), pa.string()),
        }),
        "prices",
    )
    if writer is None or part_rows > PART_MAX:
        if writer is not None:
            writer.close()
            part += 1
        writer = pq.ParquetWriter(PRICE_DIR / f"part-{part:02d}.parquet", schema.PRICES,
                                  compression="zstd", compression_level=3, use_dictionary=["isin", "currency"])
        part_rows = 0
    writer.write_table(tbl, row_group_size=32768)
    part_rows += tbl.num_rows
    buf_isin, buf_d, buf_o, buf_h, buf_l, buf_c, buf_a, buf_v, buf_ccy = [], [], [], [], [], [], [], [], []
    return tbl.num_rows


total_rows = 0
t0 = time.time()
for i in range(N):
    s = int(start_ix[i])
    d = bdays[s:]
    n = len(d)
    ac = asset_class[i]
    vol = VOL[ac] * abs(leverage[i]) * RNG.uniform(0.7, 1.4)
    mu = DRIFT[ac] * leverage[i] * RNG.uniform(0.5, 1.5) - float(ter[i])
    dv, dm = vol / sqrt_ann, mu / TRADING_DAYS_PER_YEAR
    steps = RNG.normal(dm - 0.5 * dv * dv, dv, n)
    adj = (100.0 * np.exp(np.cumsum(steps))).astype("float64")

    # A distributing fund's quoted price sits below its total-return series by
    # the dividends already paid out; close is derived from adj so the two agree.
    if distribution_policy[i] == "distributing":
        per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[dividend_frequency[i]]
        yield_pa = RNG.uniform(0.005, 0.045)
        pay_ix = np.arange(n - 1, 0, -max(1, TRADING_DAYS_PER_YEAR // per_year))[::-1]
        factor = np.ones(n)
        for k in pay_ix:
            factor[: k + 1] *= 1.0 - yield_pa / per_year
        close = adj * factor
        for k in pay_ix:
            ca_isin.append(isin[i]); ca_date.append(d[k].item()); ca_kind.append("dividend")
            ca_amount.append(float(close[k] * yield_pa / per_year)); ca_ratio.append(None)
            ca_ccy.append(fund_currency[i])
    else:
        close = adj

    noise = RNG.normal(0, 0.0022, n)
    op = (close * (1 + noise)).astype("float32")
    hi = (np.maximum(close, op) * (1 + abs(RNG.normal(0, 0.0025, n)))).astype("float32")
    lo = (np.minimum(close, op) * (1 - abs(RNG.normal(0, 0.0025, n)))).astype("float32")
    volume = (np.exp(RNG.normal(10.5, 1.6, n))).astype("float32")

    buf_isin.append(np.full(n, isin[i], dtype=object)); buf_d.append(bday_i32[s:])
    buf_o.append(op); buf_h.append(hi); buf_l.append(lo)
    buf_c.append(close.astype("float32")); buf_a.append(adj.astype("float32"))
    buf_v.append(volume); buf_ccy.append(np.full(n, fund_currency[i], dtype=object))
    total_rows += n
    flush()

    # ---- statistics --------------------------------------------------------
    logret = np.diff(np.log(adj))
    row = {"isin": isin[i], "base_currency": "EUR"}
    for label, days in WINDOWS.items():
        anchor = TODAY - np.timedelta64(days, "D")
        j = int(np.searchsorted(d, anchor, "right")) - 1
        row[f"ret_{label}"] = float(adj[-1] / adj[j] - 1) if 0 <= j < n - 1 else None
    ytd_anchor = np.datetime64(f"{int(str(TODAY)[:4])-1}-12-31")
    j = int(np.searchsorted(d, ytd_anchor, "right")) - 1
    row["ret_ytd"] = float(adj[-1] / adj[j] - 1) if j >= 0 else None
    row["ret_max"] = float(adj[-1] / adj[0] - 1)
    years = n / TRADING_DAYS_PER_YEAR
    for y in (3, 5, 10):
        r = row.get(f"ret_{y}y")
        row[f"cagr_{y}y"] = float((1 + r) ** (1 / y) - 1) if r is not None else None
    row["cagr_inception"] = float((adj[-1] / adj[0]) ** (1 / max(years, 0.25)) - 1)
    for y in (1, 3, 5):
        w = int(y * TRADING_DAYS_PER_YEAR)
        if len(logret) >= w:
            v = float(np.std(logret[-w:], ddof=1) * sqrt_ann)
            row[f"vol_{y}y"] = v
            cg = row[f"cagr_{y}y"] if y > 1 else row["ret_1y"]
            row[f"sharpe_{y}y"] = float((cg - RISK_FREE_RATE) / v) if cg is not None and v > 0 else None
        else:
            row[f"vol_{y}y"] = row[f"sharpe_{y}y"] = None
    w3 = int(3 * TRADING_DAYS_PER_YEAR)
    if len(logret) >= w3:
        down = logret[-w3:][logret[-w3:] < 0]
        dd_std = float(np.std(down, ddof=1) * sqrt_ann) if len(down) > 2 else 0.0
        row["sortino_3y"] = float((row["cagr_3y"] - RISK_FREE_RATE) / dd_std) if dd_std > 0 and row["cagr_3y"] is not None else None
    else:
        row["sortino_3y"] = None
    for label, w in (("1y", 252), ("3y", 756), ("5y", 1260), ("max", n)):
        seg = adj[-w:] if n >= w else (adj if label == "max" else None)
        if seg is None:
            row[f"max_drawdown_{label}"] = None
        else:
            row[f"max_drawdown_{label}"] = float((seg / np.maximum.accumulate(seg) - 1).min())
    peak = float(np.maximum.accumulate(adj)[-1])
    row["current_drawdown"] = float(adj[-1] / peak - 1)

    months = bd_month[s:]
    edges = np.flatnonzero(np.diff(months)) + 1
    month_last = np.concatenate([edges - 1, [n - 1]]) if len(edges) else np.array([n - 1])
    mvals = adj[month_last]
    mret = np.concatenate([[np.nan], mvals[1:] / mvals[:-1] - 1]) if len(mvals) > 1 else np.array([np.nan])
    finite = mret[np.isfinite(mret)]
    row["best_month"] = float(finite.max()) if len(finite) else None
    row["worst_month"] = float(finite.min()) if len(finite) else None
    row["positive_months_pct"] = float((finite > 0).mean()) if len(finite) else None
    row["beta_vs_world"] = float(np.clip(RNG.normal(0.95, 0.35), -2.5, 3.0)) if ac == "equity" else float(np.clip(RNG.normal(0.35, 0.4), -2.5, 3.0))
    row["correlation_vs_world"] = float(np.clip(RNG.normal(0.82 if ac == "equity" else 0.35, 0.18), -1, 1))
    row["price_last"] = float(close[-1]); row["price_date"] = d[-1].item()
    ath_ix = int(np.argmax(adj))
    row["ath"] = float(adj[ath_ix]); row["ath_date"] = d[ath_ix].item()
    row["distance_from_ath"] = row["current_drawdown"]
    row["history_start"] = d[0].item(); row["history_days"] = n
    row["computed_at"] = TODAY.item()
    for k in perf:
        perf[k].append(row.get(k))

    ym = bd_year[s:][month_last]
    mm = (months[month_last] % 12) + 1
    for k in range(len(mvals)):
        if not np.isfinite(mret[k]):
            continue
        rm_isin.append(isin[i]); rm_year.append(int(ym[k])); rm_month.append(int(mm[k]))
        rm_ret.append(float(mret[k])); rm_partial.append(k == 0 or k == len(mvals) - 1)
    yrs = bd_year[s:]
    yedges = np.flatnonzero(np.diff(yrs)) + 1
    year_last = np.concatenate([yedges - 1, [n - 1]]) if len(yedges) else np.array([n - 1])
    yvals = adj[year_last]
    ylabels = yrs[year_last]
    for k in range(len(yvals)):
        prev = yvals[k - 1] if k > 0 else adj[0]
        ry_isin.append(isin[i]); ry_year.append(int(ylabels[k]))
        ry_ret.append(float(yvals[k] / prev - 1))
        ry_partial.append(k == 0 or k == len(yvals) - 1)

    if i % 1000 == 0:
        print(f"  {i}/{N}  {total_rows:,} price rows  {time.time()-t0:.0f}s", flush=True)

flush(force=True)
if writer:
    writer.close()
print(f"prices {total_rows:,} rows in {part+1} parts  {time.time()-t0:.0f}s")

pq.write_table(schema.conform(pa.table({k: pa.array(v) for k, v in perf.items()}), "performance"),
               PROCESSED / "performance.parquet", compression="zstd")
pq.write_table(schema.conform(pa.table({
    "isin": pa.array(ry_isin, pa.string()), "year": pa.array(ry_year, pa.int16()),
    "ret": pa.array(ry_ret, pa.float32()), "partial": pa.array(ry_partial, pa.bool_())}), "returns_yearly"),
    PROCESSED / "returns_yearly.parquet", compression="zstd")
pq.write_table(schema.conform(pa.table({
    "isin": pa.array(rm_isin, pa.string()), "year": pa.array(rm_year, pa.int16()),
    "month": pa.array(rm_month, pa.int8()), "ret": pa.array(rm_ret, pa.float32()),
    "partial": pa.array(rm_partial, pa.bool_())}), "returns_monthly"),
    PROCESSED / "returns_monthly.parquet", compression="zstd")
pq.write_table(schema.conform(pa.table({
    "isin": pa.array(ca_isin, pa.string()), "date": pa.array(ca_date, pa.date32()),
    "kind": pa.array(ca_kind, pa.string()), "amount": pa.array(ca_amount, pa.float32()),
    "ratio": pa.array(ca_ratio, pa.float32()), "currency": pa.array(ca_ccy, pa.string())}), "corporate_actions"),
    PROCESSED / "corporate_actions.parquet", compression="zstd")

b_broker, b_isin, b_avail, b_wrapper, b_src, b_asof = [], [], [], [], [], []
for bi, broker in enumerate(BROKERS):
    coverage = [0.55, 0.62, 0.58, 0.14, 0.80, 0.74, 0.92][bi]
    take = np.flatnonzero((RNG.random(N) < coverage) & ~cto_false)
    for i in take:
        b_broker.append(broker); b_isin.append(isin[i]); b_avail.append(True)
        b_wrapper.append("both" if pea_true[i] and broker != "Trade Republic" else "cto")
        b_src.append(f"https://www.example-broker.invalid/{broker.lower().replace(' ', '-')}/etf")
        b_asof.append(TODAY.item())
pq.write_table(schema.conform(pa.table({
    "broker": pa.array(b_broker, pa.string()), "isin": pa.array(b_isin, pa.string()),
    "available": pa.array(b_avail, pa.bool_()), "wrapper": pa.array(b_wrapper, pa.string()),
    "source_url": pa.array(b_src, pa.string()), "as_of": pa.array(b_asof, pa.date32())}), "broker_availability"),
    PROCESSED / "broker_availability.parquet", compression="zstd")

(PROCESSED / "_SYNTHETIC").write_text(
    "Fixture generated by scratchpad/gen_synthetic.py -- not real market data.\n", encoding="utf-8"
)
for p in sorted(PROCESSED.glob("*.parquet")):
    print(f"  {p.name:32s} {p.stat().st_size/1e6:8.1f} MB")
print(f"  prices/{'':26s} {sum(f.stat().st_size for f in PRICE_DIR.glob('*.parquet'))/1e6:8.1f} MB")
