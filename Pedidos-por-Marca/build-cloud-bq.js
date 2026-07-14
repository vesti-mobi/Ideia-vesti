/**
 * Build do painel Pedidos por Marca a partir do BigQuery (vestilake_BI).
 *
 * Substitui o build-cloud.js (Fabric/Power BI), cujo dataset 'Marcas e Planos' passou a dar 404.
 *
 * ESTRATEGIA — snapshot congelado + BQ incremental:
 *   O BigQuery NAO tem o historico completo de pedidos: odbc_quotes esta truncada em 200k linhas
 *   (vai so ate mai/2024), OBDC_Quotes_Anterior2023 nao foi ingerida, e MongoDB_Pedidos_Geral so
 *   comeca em jul/2025. Migrar "puro" perderia ~63% dos pedidos e abriria um buraco de 14 meses.
 *   Entao: os pedidos anteriores ao CUTOFF continuam vindo dos chunks ja publicados (historico do
 *   Fabric, congelado em 31/03/2026) e o BQ fornece de CUTOFF em diante. Sem perda e sem buraco.
 *
 *   Consequencia: o build LE os proprios chunks do run anterior e os reescreve. A parte < CUTOFF e
 *   sempre a mesma, entao o resultado e idempotente — mas se o snapshot vier vazio/curto, abortamos
 *   (MIN_HIST) em vez de publicar um painel mutilado.
 *
 *   Se um dia o time de dados completar a ingestao (quotes full + Anterior2023 + backfill do Mongo),
 *   da pra baixar o CUTOFF e o historico passa a vir todo do BQ.
 *
 * Env: GOOGLE_APPLICATION_CREDENTIALS (SA key com acesso ao BigQuery).
 */
const fs = require('fs');
const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');

const DIR = __dirname;
const DATA_DIR = path.join(DIR, 'data');
const PROJECT_ID = 'vesti-data-499015';
const DS = '`vesti-data-499015.vestilake_BI`';
const LOCATION = 'us-central1';

// Pedidos com data >= CUTOFF vem do BQ; anteriores vem do snapshot congelado (Fabric).
// O snapshot foi gerado em 31/03/2026, entao 2026-04-01 encaixa sem sobreposicao nem buraco.
const CUTOFF = '2026-04-01';
// Guarda: o historico congelado tem ~2,69M pedidos. Se carregarmos muito menos que isso, algo
// corrompeu os chunks — abortar antes de reescrever e destruir o historico.
const MIN_HIST = 2000000;
const BATCH = 50;

const bq = new BigQuery({ projectId: PROJECT_ID });

async function bqQuery(query, label) {
    console.log('  [bq] ' + label + '...');
    const [rows] = await bq.query({ query, location: LOCATION });
    console.log('  [bq] ' + label + ': ' + rows.length + ' rows');
    return rows;
}

// ---------- 1. Snapshot congelado (chunks ja publicados) ----------
function loadSnapshot() {
    const snap = { meta: {}, pedidos: {} };

    const dadosPath = path.join(DIR, 'dados.js');
    if (fs.existsSync(dadosPath)) {
        const sandbox = {};
        new Function('g', fs.readFileSync(dadosPath, 'utf-8').replace(/^\s*const\s+DADOS\s*=/, 'g.DADOS=')).call(null, sandbox);
        for (const e of (sandbox.DADOS && sandbox.DADOS.empresas) || []) snap.meta[e.id] = e;
    }

    if (fs.existsSync(DATA_DIR)) {
        for (const f of fs.readdirSync(DATA_DIR)) {
            if (!f.startsWith('chunk_')) continue;
            const src = fs.readFileSync(path.join(DATA_DIR, f), 'utf-8');
            const collect = obj => {
                for (const [empId, lista] of Object.entries(obj || {})) {
                    // so o historico: pedidos >= CUTOFF sao reconstruidos do BQ a cada run
                    const hist = (lista || []).filter(p => p[0] && p[0] < CUTOFF);
                    if (hist.length) snap.pedidos[empId] = (snap.pedidos[empId] || []).concat(hist);
                }
            };
            new Function('loadChunk', src)(collect);
        }
    }

    const total = Object.values(snap.pedidos).reduce((s, l) => s + l.length, 0);
    console.log('  Snapshot: ' + Object.keys(snap.meta).length + ' empresas, ' +
                total.toLocaleString('pt-BR') + ' pedidos historicos (< ' + CUTOFF + ')');
    if (total < MIN_HIST) {
        console.error('FATAL: snapshot historico tem so ' + total + ' pedidos (minimo ' + MIN_HIST + ').');
        console.error('Os chunks provavelmente estao corrompidos. Abortando SEM reescrever nada.');
        process.exit(1);
    }
    return snap;
}

// ---------- 2. Empresas (odbc_domains + odbc_companies + odbc_angels) ----------
async function fetchEmpresas() {
    const rows = await bqQuery(`
        WITH doms AS (
            SELECT ID AS id, name, CAST(angel_id AS STRING) angel_id
            FROM ${DS}.odbc_domains
            WHERE LOWER(IFNULL(name,'')) NOT LIKE '%teste%'
        ),
        ranked AS (
            SELECT CAST(c.id AS STRING) id, CAST(c.domain_id AS STRING) domain_id,
                   c.tax_document, c.social_name, c.company_name, c.created_at,
                   ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.updated_at DESC) rn
            FROM ${DS}.odbc_companies c
        )
        SELECT rc.id, rc.domain_id, rc.tax_document, rc.social_name, rc.company_name,
               d.name domain_name, ang.name anjo
        FROM ranked rc
        JOIN doms d ON CAST(d.id AS STRING) = rc.domain_id
        LEFT JOIN ${DS}.odbc_angels ang ON CAST(ang.id AS STRING) = d.angel_id
        WHERE rc.rn = 1`, 'Empresas (companies + domains + angels)');

    const empresas = {};
    for (const r of rows) {
        if (!r.id) continue;
        empresas[r.id] = {
            id: r.id,
            nome: r.domain_name || r.company_name || r.social_name || '',
            cnpj: r.tax_document || '',
            anjo: r.anjo || '',
            canal: '',
            dominioId: r.domain_id || '',
        };
    }
    return empresas;
}

// ---------- 3. Pedidos novos (MongoDB_Pedidos_Geral >= CUTOFF) ----------
async function fetchPedidos() {
    const rows = await bqQuery(`
        SELECT _id, companyId, customer_name, summary_total, orderNumber,
               payment_consolidatedPaymentStatus pay, status_consolidatedOrderStatus ord,
               status_canceled_isCanceled canc, settings_source src, settings_createdAt dt
        FROM ${DS}.MongoDB_Pedidos_Geral
        WHERE settings_createdAt >= '${CUTOFF}'
          AND companyId IS NOT NULL`, 'Pedidos MongoDB (>= ' + CUTOFF + ')');
    return rows;
}

function statusDe(r) {
    const pay = String(r.pay || '').toUpperCase();
    const ord = String(r.ord || '').toUpperCase();
    const canc = r.canc === true || r.canc === 'True';
    if (pay === 'PAID') return 'P';
    if (canc || ord === 'CANCELED') return 'C';
    if (pay === 'PENDING' || pay === 'WAITING' || ord === 'PENDING') return 'E';
    if (pay === 'REJECTED' || pay === 'REFUSED') return 'C';
    if (pay === 'APPROVED' || pay === 'AUTHORIZED') return 'P';
    return 'O';
}

async function main() {
    console.log('=== Pedidos por Marca — build via BigQuery ===\n');

    const snap = loadSnapshot();
    const [empresasBQ, pedidosBQ] = await Promise.all([fetchEmpresas(), fetchPedidos()]);
    console.log('  Empresas no BQ: ' + Object.keys(empresasBQ).length);

    // Pedidos: historico congelado + novos do BQ
    const pedidosPorEmp = {};
    for (const [empId, lista] of Object.entries(snap.pedidos)) pedidosPorEmp[empId] = lista.slice();

    let novos = 0, semEmpresa = 0;
    for (const r of pedidosBQ) {
        const empId = String(r.companyId);
        // aceita a empresa se ela existe no cadastro do BQ OU ja aparecia no painel
        if (!empresasBQ[empId] && !snap.meta[empId]) { semEmpresa++; continue; }
        const dt = String(r.dt || '').substring(0, 10);
        if (!dt) continue;
        const val = Math.round((parseFloat(r.summary_total) || 0) * 100) / 100;
        (pedidosPorEmp[empId] = pedidosPorEmp[empId] || []).push([
            dt, val, statusDe(r),
            String(r.src || '').substring(0, 15),
            String(r.orderNumber || ''),
            String(r.customer_name || ''),
        ]);
        novos++;
    }
    console.log('  Pedidos novos (>= ' + CUTOFF + '): ' + novos.toLocaleString('pt-BR') +
                (semEmpresa ? ' | descartados por empresa desconhecida: ' + semEmpresa : ''));

    for (const id of Object.keys(pedidosPorEmp)) pedidosPorEmp[id].sort((a, b) => b[0].localeCompare(a[0]));

    // Empresas: cadastro vivo do BQ; marca/plano/canal so existem no snapshot (vinham do DAX
    // 'Marcas e Planos', que morreu). Empresa que so existe no snapshot continua no painel.
    const empList = [];
    for (const id of Object.keys(pedidosPorEmp)) {
        const qtd = pedidosPorEmp[id].length;
        if (!qtd) continue;
        const bqE = empresasBQ[id] || {};
        const snE = snap.meta[id] || {};
        empList.push({
            id,
            nome: bqE.nome || snE.nome || '',
            cnpj: bqE.cnpj || snE.cnpj || '',
            marca: snE.marca || '',
            plano: snE.plano || '',
            anjo: bqE.anjo || snE.anjo || '',
            canal: snE.canal || bqE.canal || '',
            qtd,
        });
    }
    empList.sort((a, b) => b.qtd - a.qtd);

    const totalPed = empList.reduce((s, e) => s + e.qtd, 0);

    // Guardas: nunca publicar um painel menor que o que ja esta no ar.
    if (empList.length === 0 || totalPed < MIN_HIST) {
        console.error('FATAL: resultado degradado (' + empList.length + ' empresas, ' + totalPed +
                      ' pedidos). Nada foi escrito.');
        process.exit(1);
    }

    fs.writeFileSync(path.join(DIR, 'dados.js'),
        'const DADOS=' + JSON.stringify({ empresas: empList, gerado: new Date().toISOString() }) + ';', 'utf-8');

    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
    for (const f of fs.readdirSync(DATA_DIR)) { if (f.startsWith('chunk_')) fs.unlinkSync(path.join(DATA_DIR, f)); }

    const empIds = empList.map(e => e.id);
    for (let i = 0; i < empIds.length; i += BATCH) {
        const batch = {};
        for (let j = i; j < Math.min(i + BATCH, empIds.length); j++) batch[empIds[j]] = pedidosPorEmp[empIds[j]] || [];
        fs.writeFileSync(path.join(DATA_DIR, 'chunk_' + Math.floor(i / BATCH) + '.js'),
            'loadChunk(' + JSON.stringify(batch) + ');', 'utf-8');
    }
    const chunkMap = {};
    empIds.forEach((id, i) => { chunkMap[id] = Math.floor(i / BATCH); });
    fs.writeFileSync(path.join(DIR, 'chunks.js'), 'const CHUNKS=' + JSON.stringify(chunkMap) + ';', 'utf-8');

    console.log('\n=== RESULTADO ===');
    console.log('Empresas: ' + empList.length);
    console.log('Pedidos historicos (snapshot): ' + (totalPed - novos).toLocaleString('pt-BR'));
    console.log('Pedidos novos (BigQuery): ' + novos.toLocaleString('pt-BR'));
    console.log('Total pedidos: ' + totalPed.toLocaleString('pt-BR'));
    console.log('Chunks: ' + Math.ceil(empIds.length / BATCH));
    console.log('Done.');
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
