/**
 * Gera dados.js do painel "Metricas VestiPago" a partir do VestiHouse (Fabric).
 *
 * Fontes:
 *  - dbo.vestipago_transaction_detail  -> financeiro de CARTAO (alimentado pela API
 *    payment/v1/transaction-detail/orders). value, netValue, antifraudValue,
 *    vestiPagoValue, mdrCardBrandValue, mdrVestiValue, antecipationValue,
 *    antecipationProviderFee, antecipationVestiFee, method, cardBrand, antecipationProvider.
 *  - dbo.bdr                            -> ledger de antecipacao BDR (juros/rebate).
 *  - dbo.ODBC_Company_Method_Payments   -> metodos de pagamento habilitados por marca.
 *  - dbo.ODBC_Companies / ODBC_Domains  -> nomes de marca/dominio.
 *
 * Toda agregacao e server-side (GROUP BY), por SEMANA (ISO) e por MES.
 * Ganhos da Vesti destacados no painel: antifraudValue, mdrVestiValue, antecipationVestiFee.
 *
 * Auth: refresh token AAD -> access token scope database.windows.net (padrao da casa).
 * Env (Secrets GH Actions): FABRIC_REFRESH_TOKEN, FABRIC_TENANT_ID, FABRIC_CLIENT_ID
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const { Connection, Request } = require('tedious');

const DIR = __dirname;
const SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com';
const SQL_DATABASE = 'VestiHouse';
const TX = 'dbo.vestipago_transaction_detail';

function loadEnv() {
  const e = {};
  for (const p of [path.join(DIR, '.env'), path.join(DIR, '..', 'CS-Sucesso-do-cliente', '.env')]) {
    if (fs.existsSync(p)) fs.readFileSync(p, 'utf-8').split('\n').forEach(l => {
      const m = l.match(/^([A-Z_]+)=(.*)$/); if (m && !e[m[1]]) e[m[1]] = m[2].trim();
    });
  }
  for (const k of ['FABRIC_REFRESH_TOKEN', 'FABRIC_TENANT_ID', 'FABRIC_CLIENT_ID'])
    if (process.env[k]) e[k] = process.env[k];
  return e;
}
function post(host, p, body) {
  return new Promise((res, rej) => {
    const data = require('querystring').stringify(body);
    const r = https.request({ hostname: host, path: p, method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(data) } },
      x => { const c = []; x.on('data', d => c.push(d)); x.on('end', () => res(JSON.parse(Buffer.concat(c).toString()))); });
    r.on('error', rej); r.write(data); r.end();
  });
}
async function sqlToken(env) {
  const d = await post('login.microsoftonline.com', `/${env.FABRIC_TENANT_ID}/oauth2/v2.0/token`, {
    client_id: env.FABRIC_CLIENT_ID || '04b07795-8ddb-461a-bbee-02f9e1bf7b46',
    grant_type: 'refresh_token', refresh_token: env.FABRIC_REFRESH_TOKEN,
    scope: 'https://database.windows.net//.default offline_access',
  });
  if (!d.access_token) throw new Error('Falha token SQL: ' + (d.error_description || d.error));
  if (d.refresh_token) { try { fs.writeFileSync(path.join(DIR, '.new_refresh_token'), d.refresh_token, 'utf-8'); } catch (e) {} }
  return d.access_token;
}
function query(token, sql, label) {
  return new Promise((resolve, reject) => {
    const conn = new Connection({ server: SQL_SERVER,
      authentication: { type: 'azure-active-directory-access-token', options: { token } },
      options: { database: SQL_DATABASE, encrypt: true, port: 1433, requestTimeout: 180000, connectTimeout: 30000 } });
    const rows = []; let done = false;
    conn.on('connect', err => { if (err) { reject(err); return; }
      const req = new Request(sql, e2 => { if (e2 && !done) { done = true; reject(e2); } conn.close(); });
      req.on('row', cols => { const o = {}; cols.forEach(c => { o[c.metadata.colName] = c.value; }); rows.push(o); });
      req.on('requestCompleted', () => { if (!done) { done = true; console.log(`  ${label}: ${rows.length} linha(s)`); resolve(rows); } });
      conn.execSql(req); });
    conn.connect();
  });
}
const num = v => Number(v) || 0;
// expressoes de periodo sobre paidAt_ts
const PER = {
  semana: { y: 'DATEPART(YEAR, paidAt_ts)', p: 'DATEPART(ISO_WEEK, paidAt_ts)' },
  mes:    { y: 'DATEPART(YEAR, paidAt_ts)', p: 'DATEPART(MONTH, paidAt_ts)' },
};
const MESES = ['', 'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];

async function main() {
  const env = loadEnv();
  if (!env.FABRIC_REFRESH_TOKEN || !env.FABRIC_TENANT_ID) { console.error('ERRO: FABRIC_* nao definidos.'); process.exit(1); }
  console.log('Token SQL...');
  const tok = await sqlToken(env);

  const dados = { geradoEm: new Date().toISOString(), card: {}, metodo: {}, bandeira: {}, antecipProvider: {}, bdrLedger: {}, empresaPeriodo: {},
    ordersGMV: {}, pix: {}, antifraud: {}, seguro: {}, activeUsers: {}, activeUsersMetodo: {} };

  // KPIs / periodo
  const k = (await query(tok, `SELECT COUNT(*) n, MIN(paidAt_ts) mn, MAX(paidAt_ts) mx, COUNT(DISTINCT companyId) emp,
      SUM(value) value, SUM(netValue) netValue, SUM(antifraudValue) antifraud, SUM(vestiPagoValue) vestiPago,
      SUM(mdrCardBrandValue) mdrBanco, SUM(mdrVestiValue) mdrVesti,
      SUM(antecipationValue) antecip, SUM(antecipationProviderFee) antecipBanco, SUM(antecipationVestiFee) antecipVesti
    FROM ${TX} WHERE paidAt_ts IS NOT NULL`, 'KPIs'))[0] || {};
  dados.periodo = { de: k.mn ? new Date(k.mn).toISOString().slice(0, 10) : null, ate: k.mx ? new Date(k.mx).toISOString().slice(0, 10) : null };
  dados.kpis = {
    n: num(k.n), empresas: num(k.emp),
    value: num(k.value), netValue: num(k.netValue), antifraud: num(k.antifraud), vestiPago: num(k.vestiPago),
    mdrBanco: num(k.mdrBanco), mdrVesti: num(k.mdrVesti),
    antecip: num(k.antecip), antecipBanco: num(k.antecipBanco), antecipVesti: num(k.antecipVesti),
    ganhoVesti: num(k.antifraud) + num(k.mdrVesti) + num(k.antecipVesti),
  };

  for (const [pk, pe] of Object.entries(PER)) {
    const lbl = pk === 'semana' ? `'S' + ${pe.p}` : `${pe.p}`; // label tratado no front

    // 1) CARD total por periodo
    dados.card[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, COUNT(*) n,
        SUM(value) value, SUM(netValue) netValue, SUM(antifraudValue) antifraud, SUM(vestiPagoValue) vestiPago,
        SUM(mdrCardBrandValue) mdrBanco, SUM(mdrVestiValue) mdrVesti,
        SUM(antecipationValue) antecip, SUM(antecipationProviderFee) antecipBanco, SUM(antecipationVestiFee) antecipVesti
      FROM ${TX} WHERE paidAt_ts IS NOT NULL
      GROUP BY ${pe.y}, ${pe.p} ORDER BY ano, p`, `card.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), n: num(r.n),
        value: num(r.value), netValue: num(r.netValue), antifraud: num(r.antifraud), vestiPago: num(r.vestiPago),
        mdrBanco: num(r.mdrBanco), mdrVesti: num(r.mdrVesti),
        antecip: num(r.antecip), antecipBanco: num(r.antecipBanco), antecipVesti: num(r.antecipVesti),
        ganhoVesti: num(r.antifraud) + num(r.mdrVesti) + num(r.antecipVesti) }));

    // 2) por METODO de pagamento
    dados.metodo[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, ISNULL(method,'(sem)') metodo, COUNT(*) n,
        SUM(value) value, SUM(netValue) netValue, SUM(mdrVestiValue) mdrVesti, SUM(antifraudValue) antifraud, SUM(antecipationVestiFee) antecipVesti
      FROM ${TX} WHERE paidAt_ts IS NOT NULL
      GROUP BY ${pe.y}, ${pe.p}, ISNULL(method,'(sem)') ORDER BY ano, p`, `metodo.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), metodo: r.metodo, n: num(r.n),
        value: num(r.value), netValue: num(r.netValue), mdrVesti: num(r.mdrVesti), antifraud: num(r.antifraud), antecipVesti: num(r.antecipVesti) }));

    // 3) por BANDEIRA do cartao
    dados.bandeira[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, ISNULL(cardBrand,'(sem)') brand, COUNT(*) n, SUM(value) value
      FROM ${TX} WHERE paidAt_ts IS NOT NULL
      GROUP BY ${pe.y}, ${pe.p}, ISNULL(cardBrand,'(sem)') ORDER BY ano, p`, `bandeira.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), brand: r.brand, n: num(r.n), value: num(r.value) }));

    // 4) ANTECIPACAO por provedor (BDR/STARKBANK/IUGU...)
    dados.antecipProvider[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, ISNULL(antecipationProvider,'(sem)') prov, COUNT(*) n,
        SUM(antecipationValue) antecip, SUM(antecipationProviderFee) antecipBanco, SUM(antecipationVestiFee) antecipVesti
      FROM ${TX} WHERE paidAt_ts IS NOT NULL AND antecipationValue > 0
      GROUP BY ${pe.y}, ${pe.p}, ISNULL(antecipationProvider,'(sem)') ORDER BY ano, p`, `antecipProvider.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), prov: r.prov, n: num(r.n),
        antecip: num(r.antecip), antecipBanco: num(r.antecipBanco), antecipVesti: num(r.antecipVesti) }));

    // 5) por EMPRESA (pra pivot marca x periodo + tabela de marcas)
    dados.empresaPeriodo[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, companyId, MAX(domainId) domainId, COUNT(*) n,
        SUM(value) value, SUM(netValue) netValue, SUM(antifraudValue)+SUM(mdrVestiValue)+SUM(antecipationVestiFee) ganhoVesti
      FROM ${TX} WHERE paidAt_ts IS NOT NULL AND companyId IS NOT NULL
      GROUP BY ${pe.y}, ${pe.p}, companyId ORDER BY ano, p`, `empresaPeriodo.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), c: r.companyId, dom: r.domainId, n: num(r.n),
        value: num(r.value), netValue: num(r.netValue), ganhoVesti: num(r.ganhoVesti) }));
  }

  // BDR ledger (usa dt_ref como data) — agregado por ano/semana e ano/mes
  for (const pk of ['semana', 'mes']) {
    const pexpr = pk === 'semana' ? 'DATEPART(ISO_WEEK, CONVERT(date, dt_ref))' : 'DATEPART(MONTH, CONVERT(date, dt_ref))';
    dados.bdrLedger[pk] = (await query(tok, `
      SELECT DATEPART(YEAR, CONVERT(date, dt_ref)) ano, ${pexpr} p, COUNT(*) n,
        SUM(valor_onerado) onerado, SUM(valor_presente) presente, SUM(valor_juros) juros,
        SUM(rebate) rebate, SUM(rebate_financeiro) rebateFin
      FROM dbo.bdr WHERE dt_ref IS NOT NULL AND TRY_CONVERT(date, dt_ref) IS NOT NULL
      GROUP BY DATEPART(YEAR, CONVERT(date, dt_ref)), ${pexpr} ORDER BY ano, p`, `bdrLedger.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), n: num(r.n),
        onerado: num(r.onerado), presente: num(r.presente), juros: num(r.juros), rebate: num(r.rebate), rebateFin: num(r.rebateFin) }));
  }

  // ===== PEDIDOS (Vesti, dbo.MongoDB_Pedidos_Geral) — tudo que NAO e financeiro de cartao =====
  // PIX, Antifraud, Seguro, Active Users, nº pedidos + GMV. Janela: pagos em 2026.
  const ODATE = 'TRY_CONVERT(datetime, payment_paidAt)';
  const OW = `payment_isPaid = 1 AND ${ODATE} >= '2026-01-01'`;
  const OT = 'dbo.MongoDB_Pedidos_Geral';
  const OPER = {
    semana: { y: `DATEPART(YEAR, ${ODATE})`, p: `DATEPART(ISO_WEEK, ${ODATE})` },
    mes:    { y: `DATEPART(YEAR, ${ODATE})`, p: `DATEPART(MONTH, ${ODATE})` },
  };
  const metBucket = `CASE WHEN payment_method IN ('PIX','Pix') THEN 'PIX'
    WHEN payment_method = 'CREDIT_CARD' THEN 'Cartão de crédito' ELSE 'Outros' END`;
  for (const [pk, pe] of Object.entries(OPER)) {
    // nº pedidos + GMV total (tabela 2 de "Completo")
    dados.ordersGMV[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, COUNT(*) nPedidos, SUM(summary_total) gmv
      FROM ${OT} WHERE ${OW} GROUP BY ${pe.y}, ${pe.p} ORDER BY ano, p`, `ordersGMV.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), nPedidos: num(r.nPedidos), gmv: num(r.gmv) }));
    // PIX
    dados.pix[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, COUNT(*) n, SUM(summary_total) value
      FROM ${OT} WHERE ${OW} AND payment_method IN ('PIX','Pix') GROUP BY ${pe.y}, ${pe.p} ORDER BY ano, p`, `pix.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), n: num(r.n), value: num(r.value) }));
    // Antifraud por fonte
    dados.antifraud[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, ISNULL(payment_transaction_antifraudSource,'(sem)') src, COUNT(*) n
      FROM ${OT} WHERE ${OW} GROUP BY ${pe.y}, ${pe.p}, ISNULL(payment_transaction_antifraudSource,'(sem)') ORDER BY ano, p`, `antifraud.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), src: r.src, n: num(r.n) }));
    // Seguro (creditCard.insurance)
    dados.seguro[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p,
        SUM(CASE WHEN payment_creditCard_insurance = 1 THEN 1 ELSE 0 END) comSeguro,
        SUM(CASE WHEN payment_creditCard_insurance = 0 THEN 1 ELSE 0 END) semSeguro
      FROM ${OT} WHERE ${OW} GROUP BY ${pe.y}, ${pe.p} ORDER BY ano, p`, `seguro.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), comSeguro: num(r.comSeguro), semSeguro: num(r.semSeguro) }));
    // Active Users: marcas distintas + pedidos por periodo
    dados.activeUsers[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, COUNT(DISTINCT companyId) marcas, COUNT(*) pedidos
      FROM ${OT} WHERE ${OW} GROUP BY ${pe.y}, ${pe.p} ORDER BY ano, p`, `activeUsers.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), marcas: num(r.marcas), pedidos: num(r.pedidos) }));
    // Active Users por metodo (bucket PIX/Cartao/Outros)
    dados.activeUsersMetodo[pk] = (await query(tok, `
      SELECT ${pe.y} ano, ${pe.p} p, ${metBucket} metodo, COUNT(DISTINCT companyId) marcas
      FROM ${OT} WHERE ${OW} GROUP BY ${pe.y}, ${pe.p}, ${metBucket} ORDER BY ano, p`, `activeUsersMetodo.${pk}`))
      .map(r => ({ ano: num(r.ano), p: num(r.p), metodo: r.metodo, marcas: num(r.marcas) }));
  }

  // nomes de marca/dominio (so das empresas que aparecem no cartao)
  const nomes = await query(tok, `
    SELECT c.id companyId, c.company_name nome FROM dbo.ODBC_Companies c
    WHERE c.id IN (SELECT DISTINCT companyId FROM ${TX} WHERE companyId IS NOT NULL)`, 'nomes empresas');
  const doms = await query(tok, `
    SELECT DISTINCT t.domainId, d.name FROM ${TX} t LEFT JOIN dbo.ODBC_Domains d ON t.domainId = d.id
    WHERE t.domainId IS NOT NULL`, 'nomes dominios');
  dados.empresas = {};
  nomes.forEach(r => { if (r.companyId) dados.empresas[r.companyId] = { nome: r.nome || '' }; });
  dados.dominios = {};
  doms.forEach(r => { if (r.domainId != null) dados.dominios[r.domainId] = r.name || ''; });

  // Metodos de pagamento habilitados por marca
  const mp = await query(tok, `
    SELECT ISNULL(name,'(sem)') metodo, COUNT(DISTINCT company_id) empresas
    FROM dbo.ODBC_Company_Method_Payments GROUP BY ISNULL(name,'(sem)') ORDER BY empresas DESC`, 'metodos pagamento');
  const totEmp = (await query(tok, `SELECT COUNT(DISTINCT company_id) n FROM dbo.ODBC_Company_Method_Payments`, 'tot empresas metodo'))[0] || {};
  dados.metodosPagamento = { porMetodo: mp.map(r => ({ metodo: r.metodo, empresas: num(r.empresas) })), totalEmpresas: num(totEmp.n) };

  fs.writeFileSync(path.join(DIR, 'dados.js'), 'window.DADOS = ' + JSON.stringify(dados) + ';\n', 'utf-8');
  const sz = (fs.statSync(path.join(DIR, 'dados.js')).size / 1024).toFixed(0);
  console.log(`OK: dados.js gerado (${sz} KB) — ${dados.kpis.n} transacoes, ${dados.kpis.empresas} marcas, periodo ${dados.periodo.de}..${dados.periodo.ate}.`);
}
main().catch(e => { console.error('FALHA:', e.message); process.exit(1); });
