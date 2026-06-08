/**
 * Gera dados.js do relatorio de Movimentacao de Estoque a partir da tabela
 * dbo.movimentacao_estoque no VestiHouse (Fabric).
 *
 * A tabela tem ~1,5M linhas/dia (log de estoque), entao TODA a agregacao e feita
 * server-side (GROUP BY no SQL) e so os resumos compactos vao pro dados.js.
 *
 * Como a tabela e um SNAPSHOT (guarda so o dia mais recente), mantemos um
 * historico.json acumulando 1 snapshot por dia, pra ter serie temporal.
 *
 * Auth: refresh token AAD -> access token scope database.windows.net (mesmo
 * padrao do build-cloud.js / fetch_data.py).
 *
 * Env (Secrets no GitHub Actions):
 *   FABRIC_REFRESH_TOKEN, FABRIC_TENANT_ID, FABRIC_CLIENT_ID
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const { Connection, Request } = require('tedious');

const DIR = __dirname;
const SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com';
const SQL_DATABASE = 'VestiHouse';
const TABELA = 'dbo.movimentacao_estoque';

// --- env: process.env (CI) com fallback pra .env local (dev) ---
function loadEnv() {
  const e = {};
  // .env local (opcional, pra rodar na maquina)
  for (const p of [path.join(DIR, '.env'), path.join(DIR, '..', 'CS-Sucesso-do-cliente', '.env')]) {
    if (fs.existsSync(p)) {
      fs.readFileSync(p, 'utf-8').split('\n').forEach(l => {
        const m = l.match(/^([A-Z_]+)=(.*)$/);
        if (m && !e[m[1]]) e[m[1]] = m[2].trim();
      });
    }
  }
  // process.env vence (CI)
  for (const k of ['FABRIC_REFRESH_TOKEN', 'FABRIC_TENANT_ID', 'FABRIC_CLIENT_ID']) {
    if (process.env[k]) e[k] = process.env[k];
  }
  return e;
}

function post(host, p, body) {
  return new Promise((res, rej) => {
    const data = require('querystring').stringify(body);
    const r = https.request({
      hostname: host, path: p, method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(data) },
    }, x => { const c = []; x.on('data', d => c.push(d)); x.on('end', () => res(JSON.parse(Buffer.concat(c).toString()))); });
    r.on('error', rej); r.write(data); r.end();
  });
}

async function sqlToken(env) {
  const d = await post('login.microsoftonline.com', `/${env.FABRIC_TENANT_ID}/oauth2/v2.0/token`, {
    client_id: env.FABRIC_CLIENT_ID || '04b07795-8ddb-461a-bbee-02f9e1bf7b46',
    grant_type: 'refresh_token',
    refresh_token: env.FABRIC_REFRESH_TOKEN,
    scope: 'https://database.windows.net//.default offline_access',
  });
  if (!d.access_token) throw new Error('Falha no token SQL: ' + (d.error_description || d.error || 'desconhecido'));
  // rotaciona o refresh token (se vier novo) pro workflow atualizar o secret
  if (d.refresh_token) {
    try { fs.writeFileSync(path.join(DIR, '.new_refresh_token'), d.refresh_token, 'utf-8'); } catch (e) {}
  }
  return d.access_token;
}

function query(token, sql, label) {
  return new Promise((resolve, reject) => {
    const conn = new Connection({
      server: SQL_SERVER,
      authentication: { type: 'azure-active-directory-access-token', options: { token } },
      options: { database: SQL_DATABASE, encrypt: true, port: 1433, requestTimeout: 180000, connectTimeout: 30000 },
    });
    const rows = []; let done = false;
    conn.on('connect', err => {
      if (err) { reject(err); return; }
      const req = new Request(sql, e2 => { if (e2 && !done) { done = true; reject(e2); } conn.close(); });
      req.on('row', cols => { const o = {}; cols.forEach(c => { o[c.metadata.colName] = c.value; }); rows.push(o); });
      req.on('requestCompleted', () => { if (!done) { done = true; console.log(`  ${label}: ${rows.length} linha(s)`); resolve(rows); } });
      conn.execSql(req);
    });
    conn.connect();
  });
}

function isoDate(v) { return v ? new Date(v).toISOString().slice(0, 10) : null; }

async function main() {
  const env = loadEnv();
  if (!env.FABRIC_REFRESH_TOKEN || !env.FABRIC_TENANT_ID) {
    console.error('ERRO: FABRIC_REFRESH_TOKEN/FABRIC_TENANT_ID nao definidos.');
    process.exit(1);
  }
  console.log('Obtendo token SQL...');
  const tok = await sqlToken(env);

  // 1) KPIs gerais
  const kpiRows = await query(tok, `
    SELECT COUNT(*) AS totalMov,
           COUNT(DISTINCT domain_id)  AS dominios,
           COUNT(DISTINCT company_id) AS empresas,
           COUNT(DISTINCT sku)        AS skus,
           COUNT(DISTINCT product_id) AS produtos,
           COUNT(DISTINCT order_id)   AS pedidos,
           MIN(created_at) AS minD, MAX(created_at) AS maxD
    FROM ${TABELA}`, 'KPIs');
  const kpi = kpiRows[0] || {};

  // 2) por acao
  const porAcao = await query(tok,
    `SELECT action, COUNT(*) AS n FROM ${TABELA} GROUP BY action ORDER BY n DESC`, 'por acao');

  // 3) por origem
  const porOrigem = await query(tok,
    `SELECT origin, COUNT(*) AS n FROM ${TABELA} GROUP BY origin ORDER BY n DESC`, 'por origem');

  // 4) por dia
  const porDia = await query(tok,
    `SELECT CONVERT(date, created_at) AS d, COUNT(*) AS n FROM ${TABELA} GROUP BY CONVERT(date, created_at) ORDER BY d`, 'por dia');

  // 5) top 50 empresas (com nome via ODBC_Companies) + split por acao + saldo liquido
  const topEmpresas = await query(tok, `
    SELECT TOP 50 m.company_id, m.domain_id,
           MAX(c.company_name) AS nome,
           COUNT(*) AS n,
           SUM(CASE WHEN m.action='INSERT' THEN 1 ELSE 0 END) AS ins,
           SUM(CASE WHEN m.action='UPDATE' THEN 1 ELSE 0 END) AS upd,
           SUM(CASE WHEN m.action='DELETE' THEN 1 ELSE 0 END) AS del,
           COUNT(DISTINCT m.sku) AS skus,
           SUM(CAST(m.new_balance AS bigint) - CAST(m.old_balance AS bigint)) AS saldoLiq
    FROM ${TABELA} m
    LEFT JOIN dbo.ODBC_Companies c ON m.company_id = c.id
    GROUP BY m.company_id, m.domain_id
    ORDER BY n DESC`, 'top empresas');

  const dados = {
    geradoEm: new Date().toISOString(),
    periodo: { de: isoDate(kpi.minD), ate: isoDate(kpi.maxD) },
    kpis: {
      totalMov: Number(kpi.totalMov) || 0,
      dominios: Number(kpi.dominios) || 0,
      empresas: Number(kpi.empresas) || 0,
      skus: Number(kpi.skus) || 0,
      produtos: Number(kpi.produtos) || 0,
      pedidos: Number(kpi.pedidos) || 0,
    },
    porAcao: porAcao.map(r => ({ action: r.action, n: Number(r.n) })),
    porOrigem: porOrigem.map(r => ({ origin: r.origin, n: Number(r.n) })),
    porDia: porDia.map(r => ({ d: isoDate(r.d), n: Number(r.n) })),
    topEmpresas: topEmpresas.map(r => ({
      companyId: r.company_id, domainId: r.domain_id, nome: r.nome || '(sem nome)',
      n: Number(r.n), ins: Number(r.ins), upd: Number(r.upd), del: Number(r.del),
      skus: Number(r.skus), saldoLiq: Number(r.saldoLiq) || 0,
    })),
  };

  // --- historico.json: 1 snapshot por dia (a tabela e snapshot do dia mais recente) ---
  const histPath = path.join(DIR, 'historico.json');
  let hist = [];
  try { hist = JSON.parse(fs.readFileSync(histPath, 'utf-8')); } catch (e) {}
  const diaRef = dados.periodo.ate || isoDate(new Date());
  const snap = { d: diaRef, totalMov: dados.kpis.totalMov, dominios: dados.kpis.dominios, empresas: dados.kpis.empresas, skus: dados.kpis.skus };
  hist = hist.filter(h => h.d !== diaRef);  // upsert por dia (reexecucao nao duplica)
  hist.push(snap);
  hist.sort((a, b) => (a.d < b.d ? -1 : 1));
  fs.writeFileSync(histPath, JSON.stringify(hist, null, 0), 'utf-8');
  dados.historico = hist;

  fs.writeFileSync(path.join(DIR, 'dados.js'), 'window.DADOS = ' + JSON.stringify(dados) + ';\n', 'utf-8');
  console.log(`OK: dados.js gerado (${dados.kpis.totalMov} movimentacoes, periodo ${dados.periodo.de}..${dados.periodo.ate}).`);

  // ===================== BUSCA (estatica) =====================
  // As sincronizacoes em massa (INTEGRATION_BULK_*) sao 95% do volume e ruido
  // automatico. Embarcamos so as movimentacoes "de gente" (reserva/venda/
  // separacao/app) — poucas dezenas de milhar — pra busca client-side por
  // dominio/empresa/produto/sku/data. Chaves curtas pra reduzir o payload.
  const FILTRO_REAIS = "origin NOT LIKE 'INTEGRATION_BULK%'";
  const linhas = await query(tok, `
    SELECT created_at, action, origin, sku,
           old_qty, new_qty, old_balance, new_balance,
           domain_id, company_id, product_id, order_id
    FROM ${TABELA}
    WHERE ${FILTRO_REAIS}
    ORDER BY created_at DESC`, 'linhas busca (nao-bulk)');

  const empNomes = await query(tok, `
    SELECT DISTINCT m.company_id, c.company_name
    FROM ${TABELA} m LEFT JOIN dbo.ODBC_Companies c ON m.company_id = c.id
    WHERE ${FILTRO_REAIS}`, 'nomes empresas');
  const domNomes = await query(tok, `
    SELECT DISTINCT m.domain_id, d.name
    FROM ${TABELA} m LEFT JOIN dbo.ODBC_Domains d ON m.domain_id = d.id
    WHERE ${FILTRO_REAIS}`, 'nomes dominios');

  const empresas = {}; empNomes.forEach(r => { if (r.company_id) empresas[r.company_id] = r.company_name || ''; });
  const dominios = {}; domNomes.forEach(r => { if (r.domain_id != null) dominios[r.domain_id] = r.name || ''; });

  const busca = {
    geradoEm: dados.geradoEm,
    periodo: dados.periodo,
    nota: 'Busca cobre movimentacoes de venda/reserva/separacao/app. Sincronizacoes de integracao em massa (INTEGRATION_BULK_*) aparecem so nos totais.',
    empresas, dominios,
    rows: linhas.map(r => ({
      t: r.created_at ? new Date(r.created_at).toISOString() : null,
      a: r.action, o: r.origin, s: r.sku,
      dq: r.old_qty, nq: r.new_qty, db: r.old_balance, nb: r.new_balance,
      dm: r.domain_id, c: r.company_id, p: r.product_id, od: r.order_id,
    })),
  };
  fs.writeFileSync(path.join(DIR, 'busca.js'), 'window.BUSCA = ' + JSON.stringify(busca) + ';\n', 'utf-8');
  console.log(`OK: busca.js gerado (${busca.rows.length} linhas pesquisaveis, ${Object.keys(empresas).length} empresas, ${Object.keys(dominios).length} dominios).`);

  // ===================== HISTORICO POR EMPRESA (dia a dia) =====================
  // 1 linha por empresa/dia (resumo compacto, NAO as linhas cruas). Acumula a cada
  // snapshot pra dar serie temporal por marca: permite pesquisar uma marca e ver
  // todos os dias que ela movimentou, OU pesquisar um dia e ver todas as marcas.
  // Inclui TODAS as empresas com movimentacao no dia (bulk + nao-bulk).
  const RETENCAO_DIAS = 180;
  const empDia = await query(tok, `
    SELECT m.company_id, m.domain_id,
           MAX(c.company_name) AS nome,
           COUNT(*) AS n,
           SUM(CASE WHEN m.action='INSERT' THEN 1 ELSE 0 END) AS ins,
           SUM(CASE WHEN m.action='UPDATE' THEN 1 ELSE 0 END) AS upd,
           SUM(CASE WHEN m.action='DELETE' THEN 1 ELSE 0 END) AS del,
           SUM(CASE WHEN m.origin NOT LIKE 'INTEGRATION_BULK%' THEN 1 ELSE 0 END) AS naoBulk,
           COUNT(DISTINCT m.sku) AS skus,
           SUM(CAST(m.new_balance AS bigint) - CAST(m.old_balance AS bigint)) AS saldoLiq
    FROM ${TABELA} m
    LEFT JOIN dbo.ODBC_Companies c ON m.company_id = c.id
    GROUP BY m.company_id, m.domain_id`, 'empresas/dia');

  const edPath = path.join(DIR, 'empresas_dia.json');
  let ed = { dias: [], empresas: {}, rows: [] };
  try {
    const prev = JSON.parse(fs.readFileSync(edPath, 'utf-8'));
    if (prev && prev.rows) ed = prev;
  } catch (e) {}

  // upsert do dia de referencia (reexecucao do mesmo dia nao duplica)
  ed.rows = ed.rows.filter(r => r.d !== diaRef);
  empDia.forEach(r => {
    if (!r.company_id) return;
    const nome = (r.nome || '').trim();
    if (nome) ed.empresas[r.company_id] = nome;
    ed.rows.push({
      d: diaRef, c: r.company_id, dom: r.domain_id,
      n: Number(r.n) || 0, i: Number(r.ins) || 0, u: Number(r.upd) || 0, x: Number(r.del) || 0,
      nb: Number(r.naoBulk) || 0, sk: Number(r.skus) || 0, sl: Number(r.saldoLiq) || 0,
    });
  });

  // retencao: mantem so os ultimos RETENCAO_DIAS dias
  let dias = Array.from(new Set(ed.rows.map(r => r.d))).sort();
  if (dias.length > RETENCAO_DIAS) {
    const corte = dias[dias.length - RETENCAO_DIAS];
    ed.rows = ed.rows.filter(r => r.d >= corte);
    dias = Array.from(new Set(ed.rows.map(r => r.d))).sort();
  }
  ed.dias = dias;
  // limpa nomes de empresas que nao aparecem mais em nenhuma linha
  const ativos = new Set(ed.rows.map(r => r.c));
  Object.keys(ed.empresas).forEach(id => { if (!ativos.has(id)) delete ed.empresas[id]; });

  ed.atualizadoEm = dados.geradoEm;
  fs.writeFileSync(edPath, JSON.stringify(ed), 'utf-8');
  fs.writeFileSync(path.join(DIR, 'empresas_dia.js'), 'window.EMPRESAS_DIA = ' + JSON.stringify(ed) + ';\n', 'utf-8');
  console.log(`OK: empresas_dia gerado (${ed.rows.length} linhas empresa/dia, ${ed.dias.length} dia(s), ${Object.keys(ed.empresas).length} empresas).`);
}

main().catch(e => { console.error('FALHA:', e.message); process.exit(1); });
