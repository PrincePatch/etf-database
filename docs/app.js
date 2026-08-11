/* Shared runtime for the ETF database site.
 *
 * There is no server here. The browser boots DuckDB-WASM and range-reads the
 * Parquet files in ./data/ over plain HTTP, so this module owns the engine, the
 * formatting rules, the eligibility rendering and the URL-state plumbing that
 * both pages depend on.
 *
 * UI copy is French; identifiers and comments are English.
 */

import * as duckdb from './vendor/duckdb/duckdb-bundle.mjs';

export const DATA = new URL('./data/', location.href).href;

/* ---------------------------------------------------------------- theme --- */

const THEME_KEY = 'etfdb-theme';

export function initTheme() {
  const stored = safeGet(THEME_KEY) || 'auto';
  applyTheme(stored);
  const group = document.querySelector('.themeswitch');
  if (!group) return;
  const input = group.querySelector(`input[value="${stored}"]`);
  if (input) input.checked = true;
  group.addEventListener('change', (e) => {
    if (e.target.name !== 'theme') return;
    applyTheme(e.target.value);
    safeSet(THEME_KEY, e.target.value);
    window.dispatchEvent(new CustomEvent('themechange'));
  });
}

function applyTheme(value) {
  // "auto" removes the attribute entirely so the prefers-color-scheme media
  // query is what decides -- stamping a resolved value would freeze the page at
  // whatever the OS happened to be when it loaded.
  if (value === 'light' || value === 'dark') document.documentElement.dataset.theme = value;
  else delete document.documentElement.dataset.theme;
}

function safeGet(k) { try { return localStorage.getItem(k); } catch { return null; } }
function safeSet(k, v) { try { localStorage.setItem(k, v); } catch { /* private mode */ } }

/* ----------------------------------------------------------------- boot --- */

let connection = null;
export let manifest = { synthetic: false, counts: {}, tr_index: { file: 'tr_index.parquet' } };

export async function boot(onState) {
  if (connection) return connection;
  const t0 = performance.now();
  onState?.('boot', 'démarrage du moteur…');

  // The .wasm URL is resolved INSIDE the worker, so a relative path would
  // resolve against the worker's own directory and 404. Always absolute.
  const abs = (p) => new URL(p, location.href).href;
  const worker = new Worker(abs('./vendor/duckdb/duckdb-browser-eh.worker.js'));
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);

  // The single-threaded "eh" bundle, deliberately: the multi-threaded one needs
  // COOP/COEP response headers and GitHub Pages cannot set them.
  await db.instantiate(abs('./vendor/duckdb/duckdb-eh.wasm'));

  // The most important configuration on the site. Since duckdb-wasm v1.30.0
  // `forceFullHTTPReads` defaults to TRUE, i.e. every query silently downloads
  // the entire Parquet file -- everything still works, just ~100x slower.
  // `reliableHeadRequests` must also be false: GitHub Pages ignores Range on
  // HEAD and answers 200, so duckdb's HEAD probe would conclude that ranges are
  // unsupported and fall back to full reads. Its GET probe (Range: bytes=0-0
  // -> 206) is the one that works.
  await db.open({
    path: undefined,
    filesystem: { forceFullHTTPReads: false, reliableHeadRequests: false, allowFullHTTPReads: true },
  });

  const conn = await db.connect();
  await conn.query('SET enable_http_metadata_cache=true').catch(() => {});
  connection = conn;

  try {
    const res = await fetch(new URL('manifest.json', DATA));
    if (res.ok) manifest = await res.json();
  } catch { /* the site still works without it, on defaults */ }

  onState?.('ready', `moteur prêt en ${Math.round(performance.now() - t0)} ms`);
  return conn;
}

export function table(name) {
  return `'${DATA}${name}.parquet'`;
}

/** SQL date literal N days before the dataset's own as-of date.
 *
 *  Anchored to the data rather than to `current_date`: if a refresh is three
 *  days late, "1 an" must still mean a full year of bars, not a year ending in
 *  a gap. Formatted here rather than as date arithmetic in SQL so the window is
 *  identical whatever timezone the visitor sits in.
 */
export function cutoff(days) {
  const anchor = manifest.as_of ? new Date(`${manifest.as_of}T00:00:00Z`) : new Date();
  anchor.setUTCDate(anchor.getUTCDate() - days);
  return `DATE '${anchor.toISOString().slice(0, 10)}'`;
}

/** SQL reference to the published total-return index.
 *
 *  The site has no access to raw quotes: what ships is a weekly index in euros,
 *  rebased to 100 at each fund's first observation. `manifest.tr_index.file` may
 *  be repointed at an off-repository URL without a frontend change. */
export function trIndex() {
  const file = manifest.tr_index?.file || 'tr_index.parquet';
  return `'${new URL(file, DATA).href}'`;
}

/** The Arrow table itself, for the chart paths: a 6,000-point series read
 *  column-wise costs one typed array, while the row-object form below would
 *  allocate 6,000 objects to throw them away a millisecond later. */
export async function rawQuery(sql) {
  const conn = await boot();
  return conn.query(sql);
}

/** Column of an Arrow result as a plain array, preserving nulls -- `toArray()`
 *  on a nullable numeric vector renders nulls as 0, which a chart would draw as
 *  a spike to the axis. */
export function column(result, name) {
  const vector = result.getChild(name);
  const out = new Array(result.numRows);
  for (let i = 0; i < result.numRows; i += 1) {
    const v = vector.get(i);
    out[i] = typeof v === 'bigint' ? Number(v) : v;
  }
  return out;
}

/** Run a query and return plain objects: Arrow rows are proxies and BigInt
 *  counts break every downstream `.toFixed`, so both are normalised once here. */
export async function query(sql) {
  const conn = await boot();
  const result = await conn.query(sql);
  const fields = result.schema.fields.map((f) => f.name);
  return result.toArray().map((row) => {
    const out = {};
    for (const f of fields) {
      const v = row[f];
      out[f] = typeof v === 'bigint' ? Number(v) : v;
    }
    return out;
  });
}

/** Single-quote a SQL string literal. Values reaching this are user input from
 *  the search box; the file is public and read-only, but a stray apostrophe in
 *  a fund name would still break the query, so nothing is interpolated raw. */
export const lit = (s) => `'${String(s).replace(/'/g, "''")}'`;

/* ------------------------------------------------------------ formatting --- */

const nf = (min, max) => new Intl.NumberFormat('fr-FR', { minimumFractionDigits: min, maximumFractionDigits: max });
const NF2 = nf(2, 2);
const NF1 = nf(1, 1);
const NF0 = nf(0, 0);

// Narrow no-break space: French typography puts one before a % sign or a
// unit, and it must not wrap onto the next line.
const NBSP = ' ';

export function pct(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return null;
  const f = digits === 1 ? NF1 : digits === 0 ? NF0 : NF2;
  // Rounded before formatting so a value of -0.00002 prints "0,0 %" and not the
  // nonsense "-0,0 %".
  const rounded = Number((x * 100).toFixed(digits));
  return `${f.format(rounded === 0 ? 0 : rounded)}${NBSP}%`;
}

export function signedPct(x, digits = 1) {
  const s = pct(x, digits);
  if (s === null) return null;
  // Tested on the rounded value, not the raw one: +0.00002 must print "0,0 %",
  // not "+0,0 %".
  return Number((x * 100).toFixed(digits)) > 0 ? `+${s}` : s;
}

export function eur(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return null;
  if (x >= 1e9) return `${NF1.format(x / 1e9)}${NBSP}Md${NBSP}€`;
  // A decimal is signal at 2.5 M€ and noise at 480 M€.
  if (x >= 1e8) return `${NF0.format(x / 1e6)}${NBSP}M${NBSP}€`;
  if (x >= 1e6) return `${NF1.format(x / 1e6)}${NBSP}M${NBSP}€`;
  if (x >= 1e3) return `${NF0.format(x / 1e3)}${NBSP}k${NBSP}€`;
  return `${NF0.format(x)}${NBSP}€`;
}

export function num(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return null;
  return nf(digits, digits).format(x);
}

/** Dates come out of SQL already formatted as ISO strings (see the queries):
 *  Arrow hands back date32 as either a Date or a day count depending on the
 *  build, and guessing which is a bug waiting to happen. */
export function frDate(iso) {
  if (!iso) return null;
  const [y, m, d] = String(iso).slice(0, 10).split('-');
  return `${d}/${m}/${y}`;
}

export const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => (
  { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
));

/** null / undefined -> an explicit dash with a screen-reader word, never an
 *  empty cell that reads as zero. */
export function orNA(s) {
  return s === null || s === undefined
    ? '<span class="faint" title="Donnée non disponible">—<span class="sr-only"> non disponible</span></span>'
    : esc(s);
}

/* ----------------------------------------------------------- vocabulary --- */

const LABELS = {
  asset_class: {
    equity: 'Actions', bond: 'Obligations', commodity: 'Matières premières',
    'money-market': 'Monétaire', 'real-estate': 'Immobilier', crypto: 'Crypto',
    'multi-asset': 'Multi-actifs', currency: 'Devises', unknown: 'Non renseigné',
  },
  strategy: {
    'broad-market': 'Marché large', sector: 'Sectoriel', country: 'Pays',
    factor: 'Factoriel', thematic: 'Thématique', esg: 'ESG', dividend: 'Dividendes',
    leveraged: 'À effet de levier', inverse: 'Inverse', 'covered-call': 'Covered call',
    active: 'Actif', unknown: 'Non renseigné',
  },
  replication: {
    'physical-full': 'Physique intégrale', 'physical-sampling': 'Physique échantillonnée',
    'synthetic-swap': 'Synthétique (swap)', unknown: 'Non renseigné',
  },
  distribution_policy: {
    accumulating: 'Capitalisant', distributing: 'Distribuant', unknown: 'Non renseigné',
  },
  region: {
    world: 'Monde', usa: 'États-Unis', 'north-america': 'Amérique du Nord',
    europe: 'Europe', eurozone: 'Zone euro', france: 'France', emerging: 'Émergents',
    'asia-pacific': 'Asie-Pacifique', japan: 'Japon', china: 'Chine', india: 'Inde',
    'global-ex-usa': 'Monde hors États-Unis', unknown: 'Non renseigné',
  },
  pea_confidence: {
    highest: 'Prospectus ou DIC du fonds', high: 'Place de cotation ou émetteur',
    medium: 'Deux courtiers indépendants ou plus', low: 'Un seul courtier — à revoir',
    hint: 'Indice structurel seulement', none: 'Aucune',
  },
  pea_mechanism: {
    physical_eu: 'Détention physique de titres UE/EEE',
    synthetic_swap: 'Panier européen + swap de performance',
    unknown: 'Non établi',
  },
  cto_reason: {
    ucits_eea_with_kid: 'UCITS de l’EEE avec DIC PRIIPs',
    in_broker_catalogue: 'Présent au catalogue d’un courtier',
    no_priips_kid: 'Pas de DIC PRIIPs',
    uk_ucits_is_third_country: 'UCITS britannique — pays tiers depuis le Brexit',
    not_passported_to_france: 'Non notifié à l’AMF pour la France',
    kid_language_unconfirmed: 'Langue du DIC non confirmée',
    not_in_broker_catalogue: 'Absent des catalogues consultés',
    unknown: 'Non établi',
  },
};

export const label = (kind, value) => LABELS[kind]?.[value] ?? value ?? '—';

/* ---------------------------------------------------------- eligibility --- */

/* pea_eligible is tri-state and null is the majority case (~76% of funds).
 * null means "we could not establish it", NOT "no". The three states therefore
 * differ on four redundant channels -- word, colour, glyph and texture -- so
 * none of them depends on the reader distinguishing two shades of grey. */
const ELIG_STATE = {
  yes: { cls: 'elig-yes', glyph: '✓', word: 'oui' },
  no: { cls: 'elig-no', glyph: '✕', word: 'non' },
  unknown: { cls: 'elig-unknown', glyph: '?', word: 'inconnu' },
};

export function stateOf(value) {
  if (value === true) return 'yes';
  if (value === false) return 'no';
  return 'unknown';
}

export function eligBadge(kind, value, extra = '') {
  const s = ELIG_STATE[stateOf(value)];
  const title = kind === 'PEA'
    ? { yes: 'Éligible au PEA d’après nos sources', no: 'Structurellement inéligible au PEA', unknown: 'Éligibilité PEA non établie — ce n’est pas un « non »' }[stateOf(value)]
    : { yes: 'Accessible sur compte-titres ordinaire', no: 'Non accessible sur CTO en France', unknown: 'Accès CTO non établi' }[stateOf(value)];
  const text = [kind, s.word].filter(Boolean).join(' ');
  return `<span class="elig ${s.cls}" title="${esc(title)}">`
    + `<span class="glyph" aria-hidden="true">${s.glyph}</span>`
    + `<span>${esc(text)}</span>${extra}</span>`;
}

const TIERS = ['hint', 'low', 'medium', 'high', 'highest'];

/** Confidence as five pips, so "medium" is a position on a scale rather than a
 *  word the reader has to rank from memory. */
export function tierPips(confidence) {
  const i = TIERS.indexOf(confidence);
  if (i < 0) return '';
  const pips = TIERS.map((_, k) => `<span class="pip${k <= i ? ' on' : ''}"></span>`).join('');
  return `<span class="tier"><span class="pips" role="img" aria-label="Niveau de preuve ${i + 1} sur 5">${pips}</span>`
    + `<span>${esc(label('pea_confidence', confidence))}</span></span>`;
}

/* ------------------------------------------------------------ URL state --- */

export function readState(defaults) {
  const params = new URLSearchParams(location.search);
  const state = { ...defaults };
  for (const key of Object.keys(defaults)) {
    if (params.has(key)) state[key] = params.get(key);
  }
  return state;
}

/** Only non-default values reach the URL, so a shared link stays readable and
 *  a future default change does not silently pin old visitors to the old one. */
export function writeState(state, defaults, { push = false } = {}) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(state)) {
    if (value !== '' && value !== null && value !== undefined && String(value) !== String(defaults[key])) {
      params.set(key, value);
    }
  }
  const url = params.toString() ? `${location.pathname}?${params}` : location.pathname;
  history[push ? 'pushState' : 'replaceState'](null, '', url);
}

/* --------------------------------------------------------------- charts --- */

const TICK_YEAR = new Intl.DateTimeFormat('fr-FR', { year: 'numeric' });
const TICK_MONTH = new Intl.DateTimeFormat('fr-FR', { month: 'short', year: '2-digit' });
const TICK_DAY = new Intl.DateTimeFormat('fr-FR', { day: 'numeric', month: 'short' });

function frTick(seconds, increment) {
  const d = new Date(seconds * 1000);
  if (increment >= 365 * 86400) return TICK_YEAR.format(d);
  if (increment >= 28 * 86400) return TICK_MONTH.format(d);
  return TICK_DAY.format(d);
}

export const SERIES_COLORS = [
  '#3f7fd0', '#e08a2e', '#2e9e76', '#c1547f', '#7c63cc',
  '#1b9db5', '#a2762b', '#6e9a33', '#d9584b', '#8c8fa0',
];

/** Build a uPlot line chart sized to its container and themed from the CSS
 *  tokens, and keep it in sync with resizes and theme changes. */
export function lineChart(host, { x, series, labels, log = false, valueFmt = (v) => v?.toFixed(2) ?? '—', height = 340 }) {
  const css = getComputedStyle(document.documentElement);
  const grid = css.getPropertyValue('--rule').trim();
  const fg = css.getPropertyValue('--ink-soft').trim();
  host.innerHTML = '';
  const opts = {
    width: host.clientWidth || 600,
    height,
    scales: { x: { time: true }, y: { distr: log ? 3 : 1 } },
    axes: [
      {
        stroke: fg, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid },
        font: '11px "IBM Plex Sans", sans-serif',
        // uPlot's built-in tick labels are English; the audience is French.
        values: (u, splits, _ai, _space, incr) => splits.map((s) => frTick(s, incr)),
      },
      {
        stroke: fg, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid },
        font: '11px "IBM Plex Sans", sans-serif',
        values: (u, vals) => vals.map((v) => (v == null ? '' : new Intl.NumberFormat('fr-FR').format(Number(v.toFixed(2))))),
      },
    ],
    cursor: { focus: { prox: 24 } },
    legend: { live: true },
    series: [{ label: 'Date', value: (u, v) => (v == null ? '—' : frDate(new Date(v * 1000).toISOString())) }].concat(
      series.map((_, i) => ({
        label: labels[i],
        stroke: SERIES_COLORS[i % SERIES_COLORS.length],
        width: 1.6,
        spanGaps: true,
        points: { show: false },
        value: (u, v) => (v == null ? '—' : valueFmt(v)),
      })),
    ),
  };
  const chart = new uPlot(opts, [x, ...series], host);
  const resize = () => chart.setSize({ width: host.clientWidth, height });
  const observer = new ResizeObserver(resize);
  observer.observe(host);
  return { chart, destroy: () => { observer.disconnect(); chart.destroy(); } };
}

/* --------------------------------------------------------------- notices --- */

export function renderSyntheticNotice() {
  if (!manifest.synthetic) return;
  const el = document.createElement('div');
  el.className = 'notice notice-synthetic';
  el.setAttribute('role', 'status');
  el.innerHTML = '<span aria-hidden="true">⚠</span><span><strong>Jeu de données de démonstration.</strong> '
    + 'Ces chiffres sont synthétiques et ne correspondent à aucun fonds réel — ils servent à valider l’interface '
    + 'pendant la construction du pipeline de données.</span>';
  document.querySelector('.masthead').after(el);
}
