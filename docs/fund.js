/* Per-fund detail page.
 *
 * Six small range-read queries -- the fund row, its listings, its calendar
 * returns, its broker coverage and its published index -- assembled into one
 * page. The eligibility block comes first because it is the question this
 * database exists to answer, and it always carries its evidence.
 */

import * as A from './app.js';

const $ = (id) => document.getElementById(id);
const MONTHS = ['janv.', 'févr.', 'mars', 'avr.', 'mai', 'juin', 'juil.', 'août', 'sept.', 'oct.', 'nov.', 'déc.'];
const WINDOW_DAYS = { '1y': 365, '3y': 1095, '5y': 1826, '10y': 3653, max: null };

const DEFAULTS = { isin: '', win: 'max', log: '0' };
let state = A.readState(DEFAULTS);
let fund = null;
let chart = null;

function engine(kind, text) {
  $('engine').dataset.state = kind;
  $('engine').textContent = text;
  $('progress').dataset.state = kind === 'ready' ? 'idle' : kind;
}

async function main() {
  A.initTheme();
  if (!state.isin) { fail('Aucun ISIN demandé.', 'Ajoutez <code>?isin=…</code> à l’adresse, ou revenez au screener.'); return; }

  try {
    await A.boot(engine);
  } catch (err) {
    engine('error', 'moteur indisponible');
    fail('Le moteur de base de données n’a pas pu démarrer.', 'Essayez un navigateur récent.');
    console.error(err);
    return;
  }
  A.renderSyntheticNotice();

  // Dates are cast to text in SQL: Arrow hands date32 back as a Date or as a
  // day count depending on the build, and the two are silently incompatible.
  const rows = await A.query(`
    SELECT * EXCLUDE (search_blob, inception_date, pea_as_of, price_date, ath_date, history_start, last_updated),
           inception_date::VARCHAR AS inception_date, pea_as_of::VARCHAR AS pea_as_of,
           price_date::VARCHAR AS price_date, ath_date::VARCHAR AS ath_date,
           history_start::VARCHAR AS history_start, last_updated::VARCHAR AS last_updated
    FROM ${A.table('funds')} WHERE isin = ${A.lit(state.isin)} LIMIT 1`);

  if (!rows.length) {
    fail('Fonds introuvable.', `Aucun fonds ne porte l’identifiant <span class="mono">${A.esc(state.isin)}</span> dans cette base.`);
    engine('ready', 'prêt');
    return;
  }
  fund = rows[0];
  document.title = `${fund.name} — base ETF PEA · CTO`;

  const [listings, yearly, monthly, brokers] = await Promise.all([
    A.query(`SELECT exchange_mic, exchange_name, ticker, yahoo_ticker, trading_currency, is_primary
             FROM ${A.table('listings')} WHERE isin = ${A.lit(state.isin)}
             ORDER BY is_primary DESC NULLS LAST, exchange_mic`),
    A.query(`SELECT year, ret, partial FROM ${A.table('returns_yearly')}
             WHERE isin = ${A.lit(state.isin)} ORDER BY year DESC`),
    A.query(`SELECT year, month, ret, partial FROM ${A.table('returns_monthly')}
             WHERE isin = ${A.lit(state.isin)} ORDER BY year DESC, month`),
    A.query(`SELECT broker, available, wrapper, source_url, as_of::VARCHAR AS as_of
             FROM ${A.table('broker_availability')} WHERE isin = ${A.lit(state.isin)} ORDER BY broker`),
  ]);

  $('content').innerHTML = page(fund, listings, yearly, monthly, brokers);
  $('footer-meta').textContent = footerMeta();
  wire();
  await drawChart();

  window.__READY__ = true;
  engine('ready', 'prêt');
}

function fail(title, detail) {
  $('content').innerHTML = `<div class="panel"><div class="empty">
    <p><strong>${A.esc(title)}</strong></p><p class="muted">${detail}</p>
    <p><a href="./index.html">Retour au screener</a></p></div></div>`;
}

function footerMeta() {
  const parts = [];
  if (A.manifest.as_of) parts.push(`données arrêtées au ${A.frDate(A.manifest.as_of)}`);
  if (fund?.last_updated) parts.push(`fiche mise à jour le ${A.frDate(fund.last_updated)}`);
  if (fund?.data_sources?.length) parts.push(`sources : ${[...fund.data_sources].join(', ')}`);
  return parts.join(' · ');
}

/* ----------------------------------------------------------------- page --- */

function page(f, listings, yearly, monthly, brokers) {
  return `
  ${head(f)}
  ${eligibility(f)}
  ${chartPanel(f)}
  ${keyStats(f)}
  <div class="grid grid-2" style="margin-top:1rem">
    ${statTable('Performance (en euros, rendement total)', [
      ['1 jour', f.ret_1d, 'pct'], ['1 semaine', f.ret_1w, 'pct'], ['1 mois', f.ret_1m, 'pct'],
      ['3 mois', f.ret_3m, 'pct'], ['6 mois', f.ret_6m, 'pct'], ['Depuis le 1ᵉʳ janvier', f.ret_ytd, 'pct'],
      ['1 an', f.ret_1y, 'pct'], ['3 ans (cumulé)', f.ret_3y, 'pct'], ['5 ans (cumulé)', f.ret_5y, 'pct'],
      ['10 ans (cumulé)', f.ret_10y, 'pct'], ['Depuis la création', f.ret_max, 'pct'],
      ['TCAM 3 ans', f.cagr_3y, 'pct'], ['TCAM 5 ans', f.cagr_5y, 'pct'], ['TCAM 10 ans', f.cagr_10y, 'pct'],
      ['TCAM depuis la création', f.cagr_inception, 'pct'],
    ])}
    ${statTable('Risque', [
      ['Volatilité 1 an', f.vol_1y, 'pct0'], ['Volatilité 3 ans', f.vol_3y, 'pct0'], ['Volatilité 5 ans', f.vol_5y, 'pct0'],
      ['Sharpe 1 an', f.sharpe_1y, 'num'], ['Sharpe 3 ans', f.sharpe_3y, 'num'], ['Sharpe 5 ans', f.sharpe_5y, 'num'],
      ['Sortino 3 ans', f.sortino_3y, 'num'],
      ['Perte maximale 1 an', f.max_drawdown_1y, 'pct'], ['Perte maximale 3 ans', f.max_drawdown_3y, 'pct'],
      ['Perte maximale 5 ans', f.max_drawdown_5y, 'pct'], ['Perte maximale historique', f.max_drawdown_max, 'pct'],
      ['Écart au plus haut', f.current_drawdown, 'pct'],
      ['Meilleur mois', f.best_month, 'pct'], ['Pire mois', f.worst_month, 'pct'],
      ['Mois positifs', f.positive_months_pct, 'pct0'],
      ['Bêta / MSCI World', f.beta_vs_world, 'num'], ['Corrélation / MSCI World', f.correlation_vs_world, 'num'],
    ])}
  </div>
  ${yearlyPanel(yearly)}
  ${monthlyPanel(monthly, yearly)}
  <div class="grid grid-2" style="margin-top:1rem">
    ${listingsPanel(listings)}
    ${brokersPanel(brokers)}
  </div>
  ${factsheet(f)}`;
}

function head(f) {
  const badges = [
    ['badge-key mono', f.isin],
    ['badge-key', f.primary_ticker],
    [f.ucits ? 'badge' : '', f.ucits === true ? 'UCITS' : f.ucits === false ? 'Non-UCITS' : null],
    ['badge', f.domicile ? `Domicilié ${f.domicile}` : null],
    ['badge', A.label('replication', f.replication)],
    ['badge', A.label('distribution_policy', f.distribution_policy)],
    ['badge', f.esg ? 'ESG' : null],
    ['badge', f.currency_hedged_to ? `Couvert en ${f.currency_hedged_to}` : null],
    ['badge', f.leverage && f.leverage !== 1 ? `Levier ${A.num(f.leverage, 1)}×` : null],
    ['badge', f.fund_currency],
  ].filter(([, v]) => v).map(([cls, v]) => `<span class="badge ${cls}">${A.esc(v)}</span>`).join('');

  const context = [f.issuer, A.label('asset_class', f.asset_class), A.label('region', f.region), f.index_name]
    .filter(Boolean).map(A.esc).join(' · ');

  return `<div class="fund-head">
    <p class="eyebrow">${context}</p>
    <h1>${A.esc(f.name)}</h1>
    <div class="badges">${badges}</div>
  </div>`;
}

/* ---------------------------------------------------------- eligibility --- */

function eligibility(f) {
  const peaState = A.stateOf(f.pea_eligible);
  const peaText = {
    yes: `Ce fonds est éligible au PEA d’après la source ci-dessous. Le mécanisme retenu est :
          <strong>${A.esc(A.label('pea_mechanism', f.pea_mechanism))}</strong>.`,
    no: `Ce fonds est <strong>structurellement</strong> inéligible au PEA : sa domiciliation ou sa forme
         juridique l’exclut du champ de l’article L221-31 du code monétaire et financier. Ce n’est pas une
         absence de preuve, c’est une disqualification.`,
    unknown: `<strong>Nous n’avons pas pu établir l’éligibilité de ce fonds — ce n’est pas un « non ».</strong>
              Il n’existe aucun registre public des OPCVM éligibles au PEA : l’AMF détient un engagement par
              fonds et ne publie rien. En l’absence de preuve positive, la base laisse la case vide plutôt que
              d’avancer une réponse. La vérité pour un fonds donné se lit dans son prospectus, à la ligne
              « éligibilité PEA ».`,
  }[peaState];

  const evidence = [];
  if (f.pea_confidence && f.pea_confidence !== 'none') {
    evidence.push(['Niveau de preuve', A.tierPips(f.pea_confidence)]);
  }
  if (f.pea_mechanism && f.pea_mechanism !== 'unknown') {
    evidence.push(['Mécanisme', A.esc(A.label('pea_mechanism', f.pea_mechanism))]);
  }
  if (f.pea_source) {
    evidence.push(['Source', `<a href="${A.esc(f.pea_source)}" rel="noopener nofollow">${A.esc(shortUrl(f.pea_source))}</a>`]);
  }
  if (f.pea_as_of) evidence.push(['Vérifiée le', A.frDate(f.pea_as_of)]);

  const ctoState = A.stateOf(f.cto_accessible);
  const ctoText = {
    yes: 'Ce fonds peut être détenu sur un compte-titres ordinaire depuis la France.',
    no: 'Ce fonds n’est pas accessible à un particulier résident en France sur un compte-titres.',
    unknown: 'Nous n’avons pas pu établir l’accessibilité sur compte-titres.',
  }[ctoState];

  return `<section class="panel" style="margin-bottom:1rem" aria-labelledby="elig-title">
    <header><h2 id="elig-title">Enveloppe fiscale française</h2></header>
    <div class="panel-body elig-block">

      <div class="elig-card is-${peaState}">
        <h3>${A.eligBadge('PEA', f.pea_eligible)} <span class="muted" style="font-weight:400">Plan d’épargne en actions</span></h3>
        <p>${peaText}</p>
        ${evidence.length ? `<dl class="elig-meta">${evidence.map(([k, v]) => `<div><dt>${A.esc(k)}</dt><dd>${v}</dd></div>`).join('')}</dl>` : ''}
      </div>

      <div class="elig-card is-${ctoState}">
        <h3>${A.eligBadge('CTO', f.cto_accessible)} <span class="muted" style="font-weight:400">Compte-titres ordinaire</span></h3>
        <p>${ctoText} ${f.cto_reason ? `Motif retenu : <strong>${A.esc(A.label('cto_reason', f.cto_reason))}</strong>.` : ''}</p>
        ${f.cto_note ? `<p>${A.esc(f.cto_note)}</p>` : ''}
        <dl class="elig-meta">
          <div><dt>Document d’informations clés (PRIIPs)</dt><dd>${A.eligBadge('DIC', f.has_priips_kid)}</dd></div>
          <div><dt>Commercialisation notifiée en France</dt><dd>${A.eligBadge('AMF', f.authorised_fr)}</dd></div>
        </dl>
      </div>

      <p class="muted" style="margin:0;font-size:.82rem">
        <strong>Ces indicateurs ne remplacent pas la lecture du prospectus.</strong> L’éligibilité peut cesser
        sans annonce entre deux rapports semestriels, et détenir un titre inéligible dans un PEA peut entraîner
        la clôture du plan et la perte de son antériorité fiscale.
      </p>
    </div>
  </section>`;
}

const shortUrl = (u) => { try { return new URL(u).hostname.replace(/^www\./, ''); } catch { return u; } };

/* --------------------------------------------------------------- charts --- */

function chartPanel(f) {
  return `<section class="panel" aria-labelledby="chart-title">
    <header>
      <h2 id="chart-title">Performance totale, base 100</h2>
      <span style="flex:1"></span>
      <div class="actions">
        <div class="btn-group" role="group" aria-label="Fenêtre">
          ${Object.keys(WINDOW_DAYS).map((w) => `<button type="button" class="btn btn-sm" data-win="${w}" aria-pressed="${state.win === w}">${w === 'max' ? 'Max' : w.replace('y', ' an') + (w === '1y' ? '' : 's')}</button>`).join('')}
        </div>
        <button type="button" class="btn btn-ghost btn-sm" id="f-log" aria-pressed="${state.log === '1'}">Échelle log.</button>
      </div>
    </header>
    <div class="chart-host" id="chart">${f.has_index ? '' : '<p class="empty muted">Aucun historique n’est publié pour ce fonds.</p>'}</div>
    <p class="panel-body faint" style="padding-top:0;font-size:.75rem">
      Indice hebdomadaire de performance totale en euros, reconstruit à partir des cours et des distributions,
      ramené à 100 au début de la période affichée. ${f.history_start ? `Historique depuis le ${A.frDate(f.history_start)}.` : ''}
      Ce site publie des statistiques calculées, pas une reproduction des cours d’un fournisseur.
    </p>
  </section>`;
}

async function drawChart() {
  if (!fund.has_index) return;
  const days = WINDOW_DAYS[state.win];
  const cutoff = days ? ` AND date >= ${A.cutoff(days)}` : '';
  const result = await A.rawQuery(
    `SELECT epoch(date)::DOUBLE AS ts, tr_index::DOUBLE AS v
     FROM read_parquet(${A.trIndex()})
     WHERE isin = ${A.lit(fund.isin)}${cutoff} ORDER BY ts`
  );
  // The published index is base 100 at the fund's own launch; rebasing to the
  // first point in view makes "5 ans" read directly as the five-year return
  // instead of a level the reader has to divide in their head. On the "Max"
  // window the two are the same series.
  const values = A.column(result, 'v');
  const first = values.find((v) => v !== null && v !== undefined);
  if (first) for (let i = 0; i < values.length; i += 1) {
    if (values[i] !== null && values[i] !== undefined) values[i] = (values[i] / first) * 100;
  }
  chart?.destroy();
  chart = A.lineChart($('chart'), {
    x: A.column(result, 'ts'),
    series: [values],
    labels: ['Base 100'],
    log: state.log === '1',
    valueFmt: (v) => new Intl.NumberFormat('fr-FR', { maximumFractionDigits: 2 }).format(v),
    height: 320,
  });
}

/* ---------------------------------------------------------------- blocks --- */

function fmt(kind, v) {
  if (v === null || v === undefined || Number.isNaN(v)) return null;
  if (kind === 'pct') return A.signedPct(v, 2);
  if (kind === 'pct0') return A.pct(v, 1);
  if (kind === 'eur') return A.eur(v);
  return A.num(v, 2);
}

function keyStats(f) {
  const items = [
    ['Frais courants', A.pct(f.ter, 2)],
    ['Encours', A.eur(f.aum_eur)],
    ['1 an', A.signedPct(f.ret_1y, 1), f.ret_1y],
    ['TCAM 5 ans', A.signedPct(f.cagr_5y, 2), f.cagr_5y],
    ['Volatilité 1 an', A.pct(f.vol_1y, 1)],
    ['Sharpe 3 ans', A.num(f.sharpe_3y, 2), f.sharpe_3y],
    ['Perte max. 5 ans', A.pct(f.max_drawdown_5y, 1), f.max_drawdown_5y],
    ['Écart au plus haut', A.pct(f.current_drawdown, 1), f.current_drawdown],
  ];
  return `<dl class="stats" style="margin:1rem 0 0;border:1px solid var(--rule);border-radius:4px;overflow:hidden">
    ${items.map(([k, v, tone]) => `<div class="stat">
      <dt>${A.esc(k)}</dt>
      <dd class="${tone > 0 ? 'pos' : tone < 0 ? 'neg' : ''}">${A.orNA(v)}</dd></div>`).join('')}
  </dl>`;
}

function statTable(title, rows) {
  return `<section class="panel" aria-label="${A.esc(title)}">
    <header><h2>${A.esc(title)}</h2></header>
    <div class="table-wrap" style="max-height:none">
      <table><tbody>${rows.map(([k, v, kind]) => {
        const text = fmt(kind, v);
        const tone = kind === 'pct' && typeof v === 'number' ? (v > 0 ? 'pos' : v < 0 ? 'neg' : '') : '';
        return `<tr><th scope="row" class="left plain" style="position:static;background:transparent;text-transform:none;font-size:.82rem;font-weight:400;color:var(--ink-soft)">${A.esc(k)}</th>
          <td class="${tone}">${A.orNA(text)}</td></tr>`;
      }).join('')}</tbody></table>
    </div>
  </section>`;
}

function yearlyPanel(rows) {
  if (!rows.length) return '';
  const scale = Math.max(...rows.map((r) => Math.abs(r.ret ?? 0)), 0.01);
  return `<section class="panel" style="margin-top:1rem" aria-labelledby="year-title">
    <header><h2 id="year-title">Rendement par année civile</h2>
      <span class="faint" style="font-size:.75rem">les années incomplètes sont signalées</span></header>
    <div class="panel-body">
      <div class="yearbars">${rows.map((r) => {
        const v = r.ret ?? 0;
        const width = (Math.abs(v) / scale) * 50;
        const left = v >= 0 ? 50 : 50 - width;
        return `<div class="yearbar">
          <span class="mono faint">${r.year}${r.partial ? '*' : ''}</span>
          <span class="track"><span class="zero" style="left:50%"></span>
            <i class="${v < 0 ? 'neg' : ''}" style="left:${left}%;width:${width}%"></i></span>
          <span class="v ${v > 0 ? 'pos' : v < 0 ? 'neg' : ''}">${A.orNA(A.signedPct(r.ret, 1))}</span>
        </div>`;
      }).join('')}</div>
      ${rows.some((r) => r.partial) ? '<p class="faint" style="font-size:.75rem;margin:.6rem 0 0">* année partielle : le fonds n’a pas été coté sur les douze mois.</p>' : ''}
    </div>
  </section>`;
}

/* A month/year grid reads far better than a list here, but colour alone is not
   a value: every cell still prints its number, and partial months are marked
   with an outline rather than a lighter shade. */
function monthlyPanel(rows, yearly) {
  if (!rows.length) return '';
  // The year column comes from returns_yearly, not from compounding the twelve
  // cells: a fund launched mid-November has no return for its first month, so
  // compounding what is displayed would quietly disagree with the annual figure
  // shown three panels up.
  const annual = new Map(yearly.map((y) => [y.year, y]));
  const byYear = new Map();
  for (const r of rows) {
    if (!byYear.has(r.year)) byYear.set(r.year, new Array(12).fill(null));
    byYear.get(r.year)[r.month - 1] = r;
  }
  const scale = Math.max(...rows.map((r) => Math.abs(r.ret ?? 0)), 0.01);
  const years = [...byYear.keys()].sort((a, b) => b - a);

  const body = years.map((year) => {
    const months = byYear.get(year);
    const whole = annual.get(year);
    const cells = months.map((m, i) => {
      if (!m || m.ret === null) {
        return `<td><span class="cell na" aria-label="${MONTHS[i]} ${year} : donnée non disponible">—</span></td>`;
      }
      // Quantised into five steps so the palette stays legible, and expressed
      // with color-mix over the theme tokens so it follows light/dark.
      const step = Math.min(5, Math.ceil((Math.abs(m.ret) / scale) * 5)) * 14;
      const token = m.ret >= 0 ? '--pos' : '--neg';
      return `<td><span class="cell${m.partial ? ' part' : ''}"
        style="background:color-mix(in srgb, var(${token}) ${step}%, transparent)"
        title="${MONTHS[i]} ${year}${m.partial ? ' (mois partiel)' : ''}">${A.signedPct(m.ret, 1)}</span></td>`;
    }).join('');
    return `<tr><td class="y">${year}</td>${cells}
      <td><span class="cell${whole?.partial ? ' part' : ''}" style="font-weight:600"
        ${whole?.partial ? 'title="Année partielle : le fonds n’a pas été coté sur les douze mois."' : ''}
        >${A.orNA(A.signedPct(whole?.ret, 1))}</span></td></tr>`;
  }).join('');

  return `<section class="panel" style="margin-top:1rem" aria-labelledby="month-title">
    <header><h2 id="month-title">Rendements mensuels</h2></header>
    <div class="table-wrap" style="max-height:min(60vh,700px)">
      <table class="heat">
        <caption class="sr-only">Rendement mensuel par année. La dernière colonne donne le rendement de l’année civile.</caption>
        <thead><tr><th scope="col"><span class="sr-only">Année</span></th>
          ${MONTHS.map((m) => `<th scope="col">${m}</th>`).join('')}<th scope="col">Année</th></tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  </section>`;
}

function listingsPanel(rows) {
  return `<section class="panel" aria-labelledby="list-title">
    <header><h2 id="list-title">Cotations</h2><span class="faint" style="font-size:.75rem">${rows.length} place${rows.length > 1 ? 's' : ''}</span></header>
    <div class="table-wrap" style="max-height:none">
      <table>
        <thead><tr><th scope="col" class="left plain">Place</th><th scope="col" class="left plain">MIC</th>
          <th scope="col" class="left plain">Ticker</th><th scope="col" class="plain">Devise</th></tr></thead>
        <tbody>${rows.length ? rows.map((r) => `<tr>
          <td class="left">${A.esc(r.exchange_name || '—')}${r.is_primary ? ' <span class="badge">principale</span>' : ''}</td>
          <td class="left mono">${A.esc(r.exchange_mic)}</td>
          <td class="left mono">${A.esc(r.ticker || '—')}</td>
          <td>${A.esc(r.trading_currency || '—')}</td></tr>`).join('')
        : '<tr><td colspan="4" class="empty muted">Aucune cotation connue.</td></tr>'}</tbody>
      </table>
    </div>
  </section>`;
}

function brokersPanel(rows) {
  return `<section class="panel" aria-labelledby="broker-title">
    <header><h2 id="broker-title">Disponibilité chez les courtiers</h2></header>
    <div class="table-wrap" style="max-height:none">
      <table>
        <thead><tr><th scope="col" class="left plain">Courtier</th><th scope="col" class="left plain">Disponible</th>
          <th scope="col" class="left plain">Enveloppe</th><th scope="col" class="left plain">Relevé le</th></tr></thead>
        <tbody>${rows.length ? rows.map((r) => `<tr>
          <td class="left">${A.esc(r.broker)}</td>
          <td class="left">${A.eligBadge('', r.available)}</td>
          <td class="left">${A.esc({ pea: 'PEA', cto: 'CTO', both: 'PEA et CTO' }[r.wrapper] || '—')}</td>
          <td class="left faint">${r.source_url ? `<a href="${A.esc(r.source_url)}" rel="noopener nofollow">${A.frDate(r.as_of) || '—'}</a>` : A.frDate(r.as_of) || '—'}</td>
        </tr>`).join('')
        : `<tr><td colspan="4" class="empty muted">Aucun catalogue de courtier ne mentionne ce fonds dans nos relevés.
             <strong>Cela ne signifie pas qu’il est indisponible</strong> — seulement que nous ne l’avons pas constaté.</td></tr>`}
        </tbody>
      </table>
    </div>
  </section>`;
}

function factsheet(f) {
  const items = [
    ['Indice suivi', f.index_name], ['Fournisseur d’indice', f.index_provider],
    ['Émetteur', f.issuer], ['Marque', f.brand],
    ['Classe d’actif', A.label('asset_class', f.asset_class)],
    ['Stratégie', A.label('strategy', f.strategy)],
    ['Région', A.label('region', f.region)], ['Secteur', f.sector],
    ['Réplication', A.label('replication', f.replication)],
    ['Politique de distribution', A.label('distribution_policy', f.distribution_policy)],
    ['Fréquence de distribution', f.dividend_frequency],
    ['Devise du fonds', f.fund_currency], ['Couverture de change', f.currency_hedged_to],
    ['Domiciliation', f.domicile], ['UCITS', f.ucits === null ? null : f.ucits ? 'Oui' : 'Non'],
    ['Prêt de titres', f.securities_lending === null ? null : f.securities_lending ? 'Oui' : 'Non'],
    ['Date de création', A.frDate(f.inception_date)],
    ['Dernier cours', f.price_last ? `${A.num(f.price_last, 2)} ${f.primary_currency || ''}` : null],
    ['Date du cours', A.frDate(f.price_date)],
    ['Date du plus haut', A.frDate(f.ath_date)],
    ['Points de l’indice publié', f.index_points ? new Intl.NumberFormat('fr-FR').format(f.index_points) + ' (hebdomadaires)' : null],
  ];
  return `<section class="panel" style="margin-top:1rem" aria-labelledby="facts-title">
    <header><h2 id="facts-title">Fiche technique</h2></header>
    <div class="panel-body">
      <dl class="kv">${items.map(([k, v]) => `<dt>${A.esc(k)}</dt><dd>${A.orNA(v)}</dd>`).join('')}</dl>
    </div>
  </section>`;
}

/* --------------------------------------------------------------- wiring --- */

function wire() {
  $('content').addEventListener('click', (e) => {
    const win = e.target.closest('[data-win]');
    const log = e.target.closest('#f-log');
    if (!win && !log) return;
    if (win) state.win = win.dataset.win;
    if (log) state.log = state.log === '1' ? '0' : '1';
    for (const b of document.querySelectorAll('[data-win]')) b.setAttribute('aria-pressed', String(b.dataset.win === state.win));
    $('f-log').setAttribute('aria-pressed', String(state.log === '1'));
    A.writeState(state, DEFAULTS);
    drawChart();
  });
  window.addEventListener('themechange', () => drawChart());
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => drawChart());
}

main();
