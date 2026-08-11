/* The screener: search, filter, sort, compare.
 *
 * Every query goes straight to docs/data/funds.parquet over HTTP range reads;
 * there is no intermediate index and no server. The whole page state -- filters,
 * sort, selection, chart window -- lives in the URL, so any view is a link.
 */

import * as A from './app.js';

const $ = (id) => document.getElementById(id);

/* ------------------------------------------------------------- columns --- */

const pctTone = (v) => (v === null || v === undefined ? '' : v > 0 ? 'pos' : v < 0 ? 'neg' : '');

/** Everything the user may sort on. `stat: true` means it also earns a column
 *  in the table when it becomes the sort key, so you never sort by a number you
 *  cannot see. */
const FIELDS = {
  name: { label: 'Nom', group: 'Identité', align: 'left', text: true },
  issuer: { label: 'Émetteur', group: 'Identité', align: 'left', text: true },
  asset_class: { label: 'Classe', group: 'Identité', align: 'left', text: true, fmt: (v) => A.label('asset_class', v) },
  inception_date: { label: 'Création', group: 'Identité', fmt: (v) => A.frDate(v) },
  ter: { label: 'TER', group: 'Coûts et taille', fmt: (v) => A.pct(v, 2), invert: true },
  aum_eur: { label: 'Encours', group: 'Coûts et taille', fmt: A.eur },
  ret_1d: { label: '1 jour', group: 'Rendements', fmt: (v) => A.signedPct(v, 2), tone: true },
  ret_1w: { label: '1 sem.', group: 'Rendements', fmt: (v) => A.signedPct(v, 2), tone: true },
  ret_1m: { label: '1 mois', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  ret_3m: { label: '3 mois', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  ret_6m: { label: '6 mois', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  ret_ytd: { label: 'Depuis le 1ᵉʳ janv.', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  ret_1y: { label: '1 an', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  ret_3y: { label: '3 ans', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  ret_5y: { label: '5 ans', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  ret_10y: { label: '10 ans', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  ret_max: { label: 'Depuis création', group: 'Rendements', fmt: (v) => A.signedPct(v, 1), tone: true },
  cagr_3y: { label: 'TCAM 3 ans', group: 'Rendements annualisés', fmt: (v) => A.signedPct(v, 2), tone: true },
  cagr_5y: { label: 'TCAM 5 ans', group: 'Rendements annualisés', fmt: (v) => A.signedPct(v, 2), tone: true },
  cagr_10y: { label: 'TCAM 10 ans', group: 'Rendements annualisés', fmt: (v) => A.signedPct(v, 2), tone: true },
  cagr_inception: { label: 'TCAM depuis création', group: 'Rendements annualisés', fmt: (v) => A.signedPct(v, 2), tone: true },
  vol_1y: { label: 'Volatilité 1 an', group: 'Risque', fmt: (v) => A.pct(v, 1), invert: true },
  vol_3y: { label: 'Volatilité 3 ans', group: 'Risque', fmt: (v) => A.pct(v, 1), invert: true },
  vol_5y: { label: 'Volatilité 5 ans', group: 'Risque', fmt: (v) => A.pct(v, 1), invert: true },
  sharpe_1y: { label: 'Sharpe 1 an', group: 'Risque', fmt: (v) => A.num(v, 2), tone: true },
  sharpe_3y: { label: 'Sharpe 3 ans', group: 'Risque', fmt: (v) => A.num(v, 2), tone: true },
  sharpe_5y: { label: 'Sharpe 5 ans', group: 'Risque', fmt: (v) => A.num(v, 2), tone: true },
  sortino_3y: { label: 'Sortino 3 ans', group: 'Risque', fmt: (v) => A.num(v, 2), tone: true },
  max_drawdown_1y: { label: 'Perte max 1 an', group: 'Pertes', fmt: (v) => A.pct(v, 1), tone: true },
  max_drawdown_3y: { label: 'Perte max 3 ans', group: 'Pertes', fmt: (v) => A.pct(v, 1), tone: true },
  max_drawdown_5y: { label: 'Perte max 5 ans', group: 'Pertes', fmt: (v) => A.pct(v, 1), tone: true },
  max_drawdown_max: { label: 'Perte max historique', group: 'Pertes', fmt: (v) => A.pct(v, 1), tone: true },
  current_drawdown: { label: 'Écart au plus haut', group: 'Pertes', fmt: (v) => A.pct(v, 1), tone: true },
  best_month: { label: 'Meilleur mois', group: 'Mois', fmt: (v) => A.signedPct(v, 1), tone: true },
  worst_month: { label: 'Pire mois', group: 'Mois', fmt: (v) => A.signedPct(v, 1), tone: true },
  positive_months_pct: { label: 'Mois positifs', group: 'Mois', fmt: (v) => A.pct(v, 0) },
  beta_vs_world: { label: 'Bêta / MSCI World', group: 'Marché', fmt: (v) => A.num(v, 2) },
  correlation_vs_world: { label: 'Corrélation / MSCI World', group: 'Marché', fmt: (v) => A.num(v, 2) },
  pea_eligible: { label: 'PEA', group: 'Enveloppe', align: 'left', badge: 'pea' },
  cto_accessible: { label: 'CTO', group: 'Enveloppe', align: 'left', badge: 'cto' },
};

const BASE_COLUMNS = [
  'name', 'issuer', 'asset_class', 'ter', 'aum_eur',
  'ret_1y', 'ret_5y', 'cagr_5y', 'vol_1y', 'sharpe_3y',
  'pea_eligible', 'cto_accessible',
];

/* Columns fetched on every query regardless of what is displayed: the badges
   need their evidence, and the compare chart needs to know whether a fund has a
   published index at all. */
const ALWAYS = [
  'isin', 'primary_ticker', 'pea_eligible', 'pea_confidence', 'pea_mechanism',
  'pea_source', 'cto_accessible', 'cto_reason', 'has_index', 'name',
];

const FACETS = [
  ['ac', 'asset_class', 'f-ac', 'asset_class'],
  ['iss', 'issuer', 'f-iss', null],
  ['reg', 'region', 'f-reg', 'region'],
  ['strat', 'strategy', 'f-strat', 'strategy'],
  ['repl', 'replication', 'f-repl', 'replication'],
  ['dist', 'distribution_policy', 'f-dist', 'distribution_policy'],
  ['ccy', 'fund_currency', 'f-ccy', null],
  ['dom', 'domicile', 'f-dom', null],
];

const DEFAULTS = {
  q: '', pea: '', cto: '', ac: '', iss: '', reg: '', strat: '', repl: '', dist: '',
  ccy: '', dom: '', ucits: '', termin: '', termax: '', aummin: '', aummax: '',
  sort: 'aum_eur', dir: 'desc', sel: '', win: '5y', base: '1', log: '0',
};

const LIMIT = 300;

let state = A.readState(DEFAULTS);
let rows = [];
let selected = new Map(); // isin -> { name, ticker, hasIndex }
let chart = null;
let bootStart = performance.now();

/* ---------------------------------------------------------------- boot --- */

function engine(kind, text) {
  $('engine').dataset.state = kind;
  $('engine').textContent = text;
  $('progress').dataset.state = kind === 'ready' ? 'idle' : kind;
}

async function main() {
  A.initTheme();
  buildSortOptions();
  wire();

  try {
    await A.boot(engine);
  } catch (err) {
    engine('error', 'moteur indisponible');
    $('status').textContent = 'Le moteur de base de données n’a pas pu démarrer dans ce navigateur.';
    console.error(err);
    return;
  }
  A.renderSyntheticNotice();
  $('footer-meta').textContent = footerMeta();

  await loadFacets();
  applyStateToForm();
  await restoreSelection();
  await run();

  window.__TTI__ = Math.round(performance.now() - bootStart);
  window.__READY__ = true;
  engine('ready', `prêt · ${window.__TTI__} ms`);
}

function footerMeta() {
  const c = A.manifest.counts || {};
  const parts = [];
  if (c.funds) parts.push(`${new Intl.NumberFormat('fr-FR').format(c.funds)} fonds`);
  if (c.listings) parts.push(`${new Intl.NumberFormat('fr-FR').format(c.listings)} cotations`);
  if (A.manifest.as_of) parts.push(`données arrêtées au ${A.frDate(A.manifest.as_of)}`);
  return parts.join(' · ');
}

/* -------------------------------------------------------------- facets --- */

async function loadFacets() {
  // One query, one pass over the eight facet columns, with counts: a select
  // that says "Actions (7 280)" tells the user where the database actually is.
  const unions = FACETS.map(([key, col]) =>
    `SELECT ${A.lit(key)} AS f, ${col}::VARCHAR AS v, count(*) AS n FROM ${A.table('funds')} WHERE ${col} IS NOT NULL GROUP BY 2`
  ).join(' UNION ALL ');
  const facets = await A.query(`${unions} ORDER BY 1, 3 DESC`);

  const nf = new Intl.NumberFormat('fr-FR');
  for (const [key, , elementId, vocab] of FACETS) {
    const select = $(elementId);
    const values = facets.filter((r) => r.f === key);
    // Controlled vocabularies keep their declared order (it is meaningful);
    // open-ended lists like issuers are ranked by how many funds they carry.
    if (vocab) values.sort((a, b) => A.label(vocab, a.v).localeCompare(A.label(vocab, b.v), 'fr'));
    for (const { v, n } of values) {
      const option = document.createElement('option');
      option.value = v;
      option.textContent = `${vocab ? A.label(vocab, v) : v} (${nf.format(n)})`;
      select.append(option);
    }
  }
}

/* --------------------------------------------------------------- query --- */

function whereSql() {
  const w = [];
  const text = state.q.trim().toLowerCase();
  // search_blob is pre-lowercased by the exporter, so this is LIKE rather than
  // ILIKE: it reads one column instead of five and skips the case fold.
  if (text) w.push(`search_blob LIKE ${A.lit(`%${text}%`)}`);

  for (const [key, col] of FACETS) {
    if (state[key]) w.push(`${col} = ${A.lit(state[key])}`);
  }
  for (const [key, col] of [['pea', 'pea_eligible'], ['cto', 'cto_accessible'], ['ucits', 'ucits']]) {
    if (state[key] === 'yes') w.push(`${col} IS TRUE`);
    else if (state[key] === 'no') w.push(`${col} IS FALSE`);
    else if (state[key] === 'unknown') w.push(`${col} IS NULL`);
  }
  const range = (key, col, scale) => {
    const v = Number(state[key]);
    if (state[key] === '' || Number.isNaN(v)) return null;
    return { v: v * scale, col };
  };
  const bounds = [
    ['termin', 'ter', 0.01, '>='], ['termax', 'ter', 0.01, '<='],
    ['aummin', 'aum_eur', 1e6, '>='], ['aummax', 'aum_eur', 1e6, '<='],
  ];
  for (const [key, col, scale, op] of bounds) {
    const r = range(key, col, scale);
    // `col IS NOT NULL` is explicit rather than implied: a threshold silently
    // dropping every fund whose TER we never found would be the same mistake as
    // hiding the unknown-PEA majority behind a checkbox.
    if (r) w.push(`(${col} IS NOT NULL AND ${col} ${op} ${r.v})`);
  }
  return w.length ? w.join(' AND ') : '1=1';
}

function visibleColumns() {
  return BASE_COLUMNS.includes(state.sort) ? BASE_COLUMNS : [...BASE_COLUMNS, state.sort];
}

async function run() {
  const columns = [...new Set([...ALWAYS, ...visibleColumns()])];
  const where = whereSql();
  const dir = state.dir === 'asc' ? 'ASC' : 'DESC';

  // count(*) OVER () rides along with the page of results, so the total and the
  // rows come back from one scan instead of two.
  const sql = `SELECT ${columns.join(', ')}, count(*) OVER () AS n_match
               FROM ${A.table('funds')}
               WHERE ${where}
               ORDER BY ${state.sort} ${dir} NULLS LAST, aum_eur DESC NULLS LAST
               LIMIT ${LIMIT}`;

  const t0 = performance.now();
  $('status').textContent = 'Recherche…';
  try {
    rows = await A.query(sql);
  } catch (err) {
    console.error(err, sql);
    $('status').textContent = 'La requête a échoué.';
    return;
  }
  const ms = Math.round(performance.now() - t0);
  window.__LAST_QUERY_MS__ = ms;

  const total = rows.length ? rows[0].n_match : 0;
  const nf = new Intl.NumberFormat('fr-FR');
  $('status').textContent = total === 0
    ? 'Aucun fonds ne correspond.'
    : `${nf.format(total)} fonds · ${ms} ms`;
  $('tablefoot').textContent = total > LIMIT
    ? `Les ${LIMIT} premiers résultats sur ${nf.format(total)} sont affichés — affinez les filtres ou changez le tri.`
    : '';
  render();
}

/* -------------------------------------------------------------- render --- */

function render() {
  const columns = visibleColumns();
  $('thead').innerHTML = `<tr>
    <th class="cellcheck plain" scope="col"><span class="sr-only">Comparer</span></th>
    ${columns.map((key) => {
      const field = FIELDS[key];
      const sorted = state.sort === key;
      const ariaSort = sorted ? ` aria-sort="${state.dir === 'asc' ? 'ascending' : 'descending'}"` : '';
      return `<th scope="col" class="${field.align === 'left' ? 'left' : ''}"${ariaSort}>
        <button type="button" class="sortbtn" data-sort="${key}">
          <span>${A.esc(field.label)}</span>
          <span class="arrow" aria-hidden="true">${state.dir === 'asc' ? '↑' : '↓'}</span>
        </button></th>`;
    }).join('')}
  </tr>`;

  // The sorted column doubles as a bar chart; the scale is the visible page, so
  // it reads as "where does this row sit among what I am looking at".
  const field = FIELDS[state.sort];
  let lo = 0, hi = 0, ranked = false;
  if (field && !field.text && !field.badge) {
    const values = rows.map((r) => r[state.sort]).filter((v) => typeof v === 'number' && Number.isFinite(v));
    if (values.length > 1) { lo = Math.min(...values); hi = Math.max(...values); ranked = hi > lo; }
  }

  $('tbody').innerHTML = rows.map((row) => {
    const isSel = selected.has(row.isin);
    const cells = columns.map((key) => cell(row, key, ranked && key === state.sort ? { lo, hi } : null)).join('');
    return `<tr class="${isSel ? 'is-selected' : ''}" data-isin="${A.esc(row.isin)}">
      <td class="cellcheck"><input type="checkbox" data-pick="${A.esc(row.isin)}" ${isSel ? 'checked' : ''}
        aria-label="Ajouter ${A.esc(row.name)} à la comparaison"></td>${cells}</tr>`;
  }).join('');

  if (!rows.length) {
    $('tbody').innerHTML = `<tr><td colspan="${columns.length + 1}" class="empty">
      <p><strong>Aucun fonds ne correspond à ces critères.</strong></p>
      <p class="muted">Si vous avez choisi « PEA : éligible », rappelez-vous que seuls ~2 % des fonds
      ont une éligibilité établie — essayez « inconnu » pour voir les candidats non tranchés.</p></td></tr>`;
  }
}

function cell(row, key, rank) {
  const field = FIELDS[key];
  const value = row[key];

  if (key === 'name') {
    return `<td class="left name"><a href="./fund.html?isin=${encodeURIComponent(row.isin)}">${A.esc(value)}</a>
      <span class="sub">${A.esc(row.primary_ticker || '')} · ${A.esc(row.isin)}</span></td>`;
  }
  if (field.badge === 'pea') {
    const source = row.pea_source
      ? ` <a href="${A.esc(row.pea_source)}" rel="noopener nofollow" title="Source de l’éligibilité" aria-label="Source de l’éligibilité PEA">↗</a>`
      : '';
    return `<td class="left">${A.eligBadge('PEA', value)}${source}</td>`;
  }
  if (field.badge === 'cto') {
    return `<td class="left">${A.eligBadge('CTO', value)}</td>`;
  }

  const text = field.fmt ? field.fmt(value) : value;
  const tone = field.tone ? pctTone(value) : '';
  const classes = [field.align === 'left' ? 'left' : '', tone, rank ? 'ranked' : ''].filter(Boolean).join(' ');
  const bar = rank && typeof value === 'number' && Number.isFinite(value)
    ? ` style="--bar:${((value - rank.lo) / (rank.hi - rank.lo)).toFixed(3)}"` : '';
  return `<td class="${classes}"${bar}><span>${A.orNA(text)}</span></td>`;
}

/* ------------------------------------------------------------ selection --- */

async function restoreSelection() {
  const isins = state.sel ? state.sel.split(',').filter(Boolean) : [];
  if (!isins.length) return;
  const list = isins.map(A.lit).join(',');
  const found = await A.query(
    `SELECT isin, name, primary_ticker, has_index FROM ${A.table('funds')} WHERE isin IN (${list})`
  );
  selected = new Map(found.map((r) => [r.isin, { name: r.name, ticker: r.primary_ticker, hasIndex: r.has_index }]));
  await drawCompare();
}

function toggle(isin, on) {
  if (on) {
    const row = rows.find((r) => r.isin === isin);
    if (row) selected.set(isin, { name: row.name, ticker: row.primary_ticker, hasIndex: row.has_index });
  } else {
    selected.delete(isin);
  }
  state.sel = [...selected.keys()].join(',');
  A.writeState(state, DEFAULTS);
  document.querySelector(`tr[data-isin="${CSS.escape(isin)}"]`)?.classList.toggle('is-selected', on);
  drawCompare();
}

/* -------------------------------------------------------------- compare --- */

const WINDOW_DAYS = { '1y': 365, '3y': 1095, '5y': 1826, '10y': 3653, max: null };

async function drawCompare() {
  const panel = $('compare');
  if (!selected.size) {
    panel.hidden = true;
    chart?.destroy();
    chart = null;
    return;
  }
  panel.hidden = false;
  $('compare-count').textContent = `${selected.size} fonds`;
  renderSwatches();

  const withPrices = [...selected.entries()].filter(([, meta]) => meta.hasIndex);
  if (!withPrices.length) {
    $('chart').innerHTML = '<p class="empty muted">Aucun historique disponible pour cette sélection.</p>';
    return;
  }
  const days = WINDOW_DAYS[state.win];
  const cutoff = days ? ` AND date >= ${A.cutoff(days)}` : '';

  // One equality-filtered scan per fund, unioned, then pivoted.
  //
  // Measured, and not the obvious shape: `isin IN (a,b,c,d,e)` pulls several
  // times more bytes because DuckDB stops skipping row groups on an IN-list, and
  // an OR chain is worse still. A separate `isin = ...` scan per fund keeps the
  // row-group statistics usable. The outer aggregate then returns one row per
  // date with one column per fund, which is already uPlot's layout, and
  // reconciles the funds' differing calendars in the same pass.
  const legs = withPrices.map(([isin], i) =>
    `SELECT ${i} AS k, date, tr_index AS v
     FROM read_parquet(${A.trIndex()})
     WHERE isin = ${A.lit(isin)}${cutoff}`).join(' UNION ALL ');
  const pivot = withPrices.map((_, i) => `max(CASE WHEN k = ${i} THEN v END) AS s${i}`).join(', ');

  const t0 = performance.now();
  const result = await A.rawQuery(
    `SELECT epoch(date)::DOUBLE AS ts, ${pivot} FROM (${legs}) GROUP BY 1 ORDER BY 1`
  );
  window.__LAST_CHART_MS__ = Math.round(performance.now() - t0);

  const timeline = A.column(result, 'ts');
  // The published index is already base 100 at each fund's own inception, which
  // is the right frame for "since launch" but not for a window: over five years
  // a 2001 fund would start at 800 and a 2020 one at 130. Rebasing to the first
  // point in view is arithmetic on data we published, and it is what a
  // comparison actually asks.
  const rebase = state.base === '1';
  const series = [];
  const labels = [];
  withPrices.forEach(([isin, meta], i) => {
    const values = A.column(result, `s${i}`);
    if (rebase) {
      const first = values.find((v) => v !== null && v !== undefined);
      if (first) for (let k = 0; k < values.length; k += 1) {
        if (values[k] !== null && values[k] !== undefined) values[k] = (values[k] / first) * 100;
      }
    }
    series.push(values);
    labels.push(meta.ticker || isin);
  });

  chart?.destroy();
  chart = A.lineChart($('chart'), {
    x: timeline,
    series,
    labels,
    log: state.log === '1',
    valueFmt: (v) => new Intl.NumberFormat('fr-FR', { maximumFractionDigits: 2 }).format(v),
    height: 340,
  });
}

function renderSwatches() {
  $('swatches').innerHTML = [...selected.entries()].map(([isin, meta], i) => `
    <span class="swatch">
      <span class="dot" style="background:${A.SERIES_COLORS[i % A.SERIES_COLORS.length]}" aria-hidden="true"></span>
      <a href="./fund.html?isin=${encodeURIComponent(isin)}">${A.esc(meta.ticker || isin)}</a>
      <button type="button" data-drop="${A.esc(isin)}" aria-label="Retirer ${A.esc(meta.name)} de la comparaison">×</button>
    </span>`).join('');
}

/* --------------------------------------------------------------- wiring --- */

function buildSortOptions() {
  const select = $('f-sort');
  const groups = new Map();
  for (const [key, field] of Object.entries(FIELDS)) {
    if (!groups.has(field.group)) groups.set(field.group, []);
    groups.get(field.group).push([key, field.label]);
  }
  for (const [group, entries] of groups) {
    const optgroup = document.createElement('optgroup');
    optgroup.label = group;
    for (const [key, text] of entries) {
      const option = document.createElement('option');
      option.value = key;
      option.textContent = text;
      optgroup.append(option);
    }
    select.append(optgroup);
  }
}

function applyStateToForm() {
  for (const key of Object.keys(DEFAULTS)) {
    const el = document.querySelector(`#filters [name="${key}"]`);
    if (el) el.value = state[key];
  }
  syncDirButtons();
  syncCompareButtons();
}

function syncDirButtons() {
  for (const b of document.querySelectorAll('[data-dir]')) {
    b.setAttribute('aria-pressed', String(b.dataset.dir === state.dir));
  }
}

function syncCompareButtons() {
  for (const b of document.querySelectorAll('[data-win]')) {
    b.setAttribute('aria-pressed', String(b.dataset.win === state.win));
  }
  $('c-base').setAttribute('aria-pressed', String(state.base === '1'));
  $('c-log').setAttribute('aria-pressed', String(state.log === '1'));
}

function commit({ rerun = true } = {}) {
  A.writeState(state, DEFAULTS);
  if (rerun) run();
}

function wire() {
  let timer = null;
  // The controls live in a <form> so labels, fieldsets and Enter-to-search work
  // natively; there is nowhere to submit to, so the default is suppressed.
  $('filters').addEventListener('submit', (e) => e.preventDefault());
  $('filters').addEventListener('input', (e) => {
    const name = e.target.name;
    if (!name || !(name in DEFAULTS)) return;
    state[name] = e.target.value;
    // Typing debounces; picking from a select should feel immediate.
    clearTimeout(timer);
    const wait = e.target.tagName === 'INPUT' ? 260 : 0;
    timer = setTimeout(() => commit(), wait);
  });

  $('filters').addEventListener('click', (e) => {
    const dir = e.target.closest('[data-dir]');
    if (dir) {
      state.dir = dir.dataset.dir;
      syncDirButtons();
      commit();
    }
  });

  $('reset').addEventListener('click', () => {
    state = { ...DEFAULTS, sel: state.sel };
    applyStateToForm();
    commit();
  });

  $('thead').addEventListener('click', (e) => {
    const button = e.target.closest('[data-sort]');
    if (!button) return;
    const key = button.dataset.sort;
    // Second click on the same header flips the direction, which is what every
    // table on the web does and what a keyboard user expects from Enter twice.
    if (state.sort === key) state.dir = state.dir === 'desc' ? 'asc' : 'desc';
    else { state.sort = key; state.dir = FIELDS[key].invert || FIELDS[key].text ? 'asc' : 'desc'; }
    $('f-sort').value = state.sort;
    syncDirButtons();
    commit();
  });

  $('tbody').addEventListener('change', (e) => {
    const pick = e.target.closest('[data-pick]');
    if (pick) toggle(pick.dataset.pick, pick.checked);
  });

  $('swatches').addEventListener('click', (e) => {
    const drop = e.target.closest('[data-drop]');
    if (!drop) return;
    const isin = drop.dataset.drop;
    selected.delete(isin);
    const box = document.querySelector(`input[data-pick="${CSS.escape(isin)}"]`);
    if (box) box.checked = false;
    document.querySelector(`tr[data-isin="${CSS.escape(isin)}"]`)?.classList.remove('is-selected');
    state.sel = [...selected.keys()].join(',');
    A.writeState(state, DEFAULTS);
    drawCompare();
  });

  $('compare').addEventListener('click', (e) => {
    const win = e.target.closest('[data-win]');
    if (win) { state.win = win.dataset.win; syncCompareButtons(); A.writeState(state, DEFAULTS); drawCompare(); return; }
    if (e.target.id === 'c-base') { state.base = state.base === '1' ? '0' : '1'; syncCompareButtons(); A.writeState(state, DEFAULTS); drawCompare(); return; }
    if (e.target.id === 'c-log') { state.log = state.log === '1' ? '0' : '1'; syncCompareButtons(); A.writeState(state, DEFAULTS); drawCompare(); return; }
    if (e.target.id === 'c-clear') {
      selected.clear();
      state.sel = '';
      A.writeState(state, DEFAULTS);
      for (const box of document.querySelectorAll('[data-pick]')) box.checked = false;
      for (const tr of document.querySelectorAll('tr.is-selected')) tr.classList.remove('is-selected');
      drawCompare();
    }
  });

  const toggleFilters = $('toggle-filters');
  toggleFilters.addEventListener('click', () => {
    const hidden = $('filters').hasAttribute('hidden');
    $('filters').toggleAttribute('hidden', !hidden);
    toggleFilters.setAttribute('aria-expanded', String(hidden));
  });
  // Collapsed by default on a phone, where the filter column would otherwise
  // push every result below the fold.
  if (window.matchMedia('(max-width: 960px)').matches) {
    $('filters').setAttribute('hidden', '');
    toggleFilters.setAttribute('aria-expanded', 'false');
  }

  window.addEventListener('popstate', async () => {
    state = A.readState(DEFAULTS);
    applyStateToForm();
    await restoreSelection();
    run();
  });

  // uPlot draws to a canvas with colours read at construction time, so a theme
  // change has to rebuild it.
  window.addEventListener('themechange', () => { if (chart) drawCompare(); });
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => { if (chart) drawCompare(); });
}

main();
