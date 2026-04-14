/**
 * Puxa Links Enviados (Product) e Cliques nos Links (Rankings.shared_links)
 * do dataset "CS - Sucesso do Cliente 2025" (Fabric/Power BI) e grava em
 * dados.js:
 *   - DADOS.empresas[].linksEnviados / .cliques    (total por empresa)
 *   - DADOS.mensal[].links / .cliques              (total global por mes)
 *   - DADOS.linksMensaisEmp[companyId][mes]        (por empresa/mes)
 *   - DADOS.cliquesMensaisEmp[companyId][mes]
 *
 * Mantém a mesma dedup de filiais em canonicalCompany (empresa.id ou matrizId).
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const qs = require('querystring');

const DIR = __dirname;
const ENV = {};
fs.readFileSync(path.join(DIR, '.env'), 'utf-8').split('\n').forEach(l => {
    const m = l.match(/^([^#=]+)=(.*)$/); if (m) ENV[m[1].trim()] = m[2].trim();
});

const WS = 'aced753a-0f0e-4bcf-9264-72f6496cf2cf';
const DS = '48a365b6-3c15-4d89-be00-64b9f17afece';

function hr(o, b) {
    return new Promise((r, j) => {
        const q = https.request(o, res => {
            const c = []; res.on('data', d => c.push(d));
            res.on('end', () => r({ s: res.statusCode, b: Buffer.concat(c).toString() }));
        });
        q.on('error', j); if (b) q.write(b); q.end();
    });
}

async function getToken() {
    const body = qs.stringify({
        client_id: ENV.FABRIC_CLIENT_ID, grant_type: 'refresh_token',
        refresh_token: ENV.FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
    });
    const tr = await hr({
        hostname: 'login.microsoftonline.com',
        path: '/' + ENV.FABRIC_TENANT_ID + '/oauth2/v2.0/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    const td = JSON.parse(tr.b);
    if (td.refresh_token && td.refresh_token !== ENV.FABRIC_REFRESH_TOKEN) {
        const p = path.join(DIR, '.env');
        let env = fs.readFileSync(p, 'utf-8');
        env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + td.refresh_token);
        fs.writeFileSync(p, env, 'utf-8');
    }
    return td.access_token;
}

async function dax(token, query) {
    const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const r = await hr({
        hostname: 'api.powerbi.com',
        path: `/v1.0/myorg/groups/${WS}/datasets/${DS}/executeQueries`,
        method: 'POST',
        headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    return JSON.parse(r.b);
}

async function main() {
    console.log('=== Patch Links (Product + Rankings) ===\n');
    const token = await getToken();

    // 1. Product -> Links Enviados (1 linha = 1 link)
    console.log('Buscando Product (links enviados)...');
    const daxProduct = `
EVALUATE
SUMMARIZECOLUMNS(
    Product[Cadastros Users ( Vendedores ).CompanyId],
    "mes", FORMAT(MAX(Product[product_sent_lists.created_at]), "YYYY-MM"),
    "links", COUNTROWS(Product)
)`;
    // SUMMARIZECOLUMNS com FORMAT(MAX(...)) nao agrupa por mes corretamente.
    // Usamos ADDCOLUMNS + SUMMARIZE para agrupar (CompanyId, Mes).
    const daxProduct2 = `
EVALUATE
ADDCOLUMNS(
    SUMMARIZE(
        Product,
        Product[Cadastros Users ( Vendedores ).CompanyId],
        Product[product_sent_lists.created_at]
    ),
    "links", 1
)`;
    // Anterior devolve 1 linha por (Company, timestamp exato). Tambem pesado.
    // Melhor: SUMMARIZE com coluna calculada de mes via ADDCOLUMNS + SUMMARIZECOLUMNS.
    const daxProduct3 = `
DEFINE
    COLUMN Product[_Mes] = FORMAT(Product[product_sent_lists.created_at], "YYYY-MM")
EVALUATE
SUMMARIZECOLUMNS(
    Product[Cadastros Users ( Vendedores ).CompanyId],
    Product[_Mes],
    "links", COUNTROWS(Product)
)`;
    let prodRes = await dax(token, daxProduct3);
    if (prodRes.error) {
        console.log('Tentativa 1 falhou:', JSON.stringify(prodRes.error).substring(0, 200));
        // Fallback: raw rows, aggregate in JS
        const raw = await dax(token, `EVALUATE SELECTCOLUMNS(Product, "cid", Product[Cadastros Users ( Vendedores ).CompanyId], "dt", Product[product_sent_lists.created_at])`);
        if (raw.error) { console.error('Raw Product falhou:', JSON.stringify(raw.error)); process.exit(1); }
        prodRes = { results: [{ tables: [{ rows: raw.results[0].tables[0].rows.map(r => {
            const dt = r['[dt]'] || '';
            const mes = (typeof dt === 'string' && dt.length >= 7) ? dt.substring(0, 7) : '';
            return { '[Cadastros Users ( Vendedores ).CompanyId]': r['[cid]'], '[_Mes]': mes, '[links]': 1 };
        }) }] }] };
    }
    const prodRows = prodRes.results[0].tables[0].rows;
    console.log('  linhas:', prodRows.length);

    // 2. Rankings -> Cliques (SUM de shared_links)
    console.log('Buscando Rankings (cliques)...');
    const daxRanking = `
DEFINE
    COLUMN Rankings[_Mes] = FORMAT(Rankings[rankings.created_at], "YYYY-MM")
EVALUATE
SUMMARIZECOLUMNS(
    Rankings[Cadastros Users ( Vendedores ).CompanyId],
    Rankings[_Mes],
    "cliques", SUM(Rankings[rankings.shared_links])
)`;
    let rankRes = await dax(token, daxRanking);
    if (rankRes.error) {
        console.log('Tentativa 1 falhou:', JSON.stringify(rankRes.error).substring(0, 200));
        const raw = await dax(token, `EVALUATE SELECTCOLUMNS(Rankings, "cid", Rankings[Cadastros Users ( Vendedores ).CompanyId], "dt", Rankings[rankings.created_at], "sl", Rankings[rankings.shared_links])`);
        if (raw.error) { console.error('Raw Rankings falhou:', JSON.stringify(raw.error)); process.exit(1); }
        rankRes = { results: [{ tables: [{ rows: raw.results[0].tables[0].rows.map(r => {
            const dt = r['[dt]'] || '';
            const mes = (typeof dt === 'string' && dt.length >= 7) ? dt.substring(0, 7) : '';
            return { '[Cadastros Users ( Vendedores ).CompanyId]': r['[cid]'], '[_Mes]': mes, '[cliques]': r['[sl]'] || 0 };
        }) }] }] };
    }
    const rankRows = rankRes.results[0].tables[0].rows;
    console.log('  linhas:', rankRows.length);

    // 3. Carregar dados.js
    console.log('\nCarregando dados.js...');
    const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf-8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();

    // 4. Montar canonicalCompany: companyId -> id canonico (matriz)
    // Regra: se empresa tem matrizId preenchido e nao e isMatriz, usa matrizId.
    //        caso contrario, usa o proprio id.
    const canonical = {}; // companyId -> canonical id
    const empById = {};
    DADOS.empresas.forEach(e => {
        empById[e.id] = e;
        const can = (e.matrizId && !e.isMatriz) ? e.matrizId : e.id;
        canonical[e.id] = can;
    });
    // Ativas: todas do dashboard (ja filtradas por statusEmpresa)
    const ativos = new Set(Object.keys(empById));

    // 5. Agregar
    const linksByCompany = {};      // canonical -> total
    const linksMensais = {};        // mes -> total global
    const linksMensaisEmp = {};     // canonical -> {mes: total}
    const cliquesByCompany = {};
    const cliquesMensais = {};
    const cliquesMensaisEmp = {};

    const K_CID_P = 'Product[Cadastros Users ( Vendedores ).CompanyId]';
    const K_MES_P = 'Product[_Mes]';
    const K_CID_R = 'Rankings[Cadastros Users ( Vendedores ).CompanyId]';
    const K_MES_R = 'Rankings[_Mes]';

    let semEmpProd = 0, semEmpRank = 0;
    prodRows.forEach(r => {
        const cid = r[K_CID_P] || r['[Cadastros Users ( Vendedores ).CompanyId]'];
        const mes = r[K_MES_P] || r['[_Mes]'] || '';
        const links = r['[links]'] || 0;
        if (!cid) return;
        const can = canonical[cid];
        if (!can) { semEmpProd += links; return; }
        linksByCompany[can] = (linksByCompany[can] || 0) + links;
        if (mes) {
            linksMensais[mes] = (linksMensais[mes] || 0) + links;
            if (!linksMensaisEmp[can]) linksMensaisEmp[can] = {};
            linksMensaisEmp[can][mes] = (linksMensaisEmp[can][mes] || 0) + links;
        }
    });

    rankRows.forEach(r => {
        const cid = r[K_CID_R] || r['[Cadastros Users ( Vendedores ).CompanyId]'];
        const mes = r[K_MES_R] || r['[_Mes]'] || '';
        const cl = r['[cliques]'] || 0;
        if (!cid) return;
        const can = canonical[cid];
        if (!can) { semEmpRank += cl; return; }
        cliquesByCompany[can] = (cliquesByCompany[can] || 0) + cl;
        if (mes) {
            cliquesMensais[mes] = (cliquesMensais[mes] || 0) + cl;
            if (!cliquesMensaisEmp[can]) cliquesMensaisEmp[can] = {};
            cliquesMensaisEmp[can][mes] = (cliquesMensaisEmp[can][mes] || 0) + cl;
        }
    });

    console.log('  Links sem match de empresa (ignorados):', semEmpProd);
    console.log('  Cliques sem match de empresa (ignorados):', semEmpRank);

    // 6. Aplicar em DADOS.empresas
    // e.m[mes] e um array onde idx 12=links e 13=cliques (veja build-data.js).
    let appliedEmp = 0;
    DADOS.empresas.forEach(e => {
        const can = (e.matrizId && !e.isMatriz) ? e.matrizId : e.id;
        // Atribui totais apenas na empresa canonica (matriz). Filiais ficam em 0
        // para evitar contagem duplicada quando a view soma linhas. Mantem a mesma
        // semantica de build-data.js (que so escrevia no empresasMap[canonical]).
        const isCanonical = (e.id === can);
        const L = isCanonical ? (linksByCompany[can] || 0) : 0;
        const C = isCanonical ? (cliquesByCompany[can] || 0) : 0;
        e.linksEnviados = L;
        e.cliques = C;
        if (L || C) appliedEmp++;

        // Atualizar o array sparse mensal por empresa (idx 12/13).
        const lMes = isCanonical ? (linksMensaisEmp[can] || {}) : {};
        const cMes = isCanonical ? (cliquesMensaisEmp[can] || {}) : {};
        if (!e.m) e.m = {};
        const mesesTocados = new Set([...Object.keys(lMes), ...Object.keys(cMes)]);
        mesesTocados.forEach(mes => {
            if (!e.m[mes]) {
                // cria linha sparse nova (12 zeros + links + cliques)
                e.m[mes] = [0,0,0,0,0,0,0,0,0,0,0,0, lMes[mes]||0, cMes[mes]||0];
            } else {
                e.m[mes][12] = lMes[mes] || 0;
                e.m[mes][13] = cMes[mes] || 0;
            }
        });
        // Zerar links/cliques nos meses onde nao ha mais dados (consistencia)
        Object.keys(e.m).forEach(mes => {
            if (!mesesTocados.has(mes) && e.m[mes]) {
                if (e.m[mes][12]) e.m[mes][12] = 0;
                if (e.m[mes][13]) e.m[mes][13] = 0;
            }
        });
    });
    console.log('  Empresas com links/cliques:', appliedEmp);

    // 7. Aplicar em DADOS.mensal (sobrescrever links/cliques por mes)
    if (Array.isArray(DADOS.mensal)) {
        DADOS.mensal.forEach(m => {
            const mk = m.mes || m.m || '';
            if (mk && linksMensais[mk] != null) m.links = linksMensais[mk];
            if (mk && cliquesMensais[mk] != null) m.cliques = cliquesMensais[mk];
        });
    }
    // Adicionar meses novos se nao existirem
    const mesesExistentes = new Set((DADOS.mensal || []).map(m => m.mes || m.m));
    Object.keys(linksMensais).forEach(mk => {
        if (!mesesExistentes.has(mk)) {
            if (!DADOS.mensal) DADOS.mensal = [];
            DADOS.mensal.push({ mes: mk, links: linksMensais[mk], cliques: cliquesMensais[mk] || 0 });
        }
    });

    DADOS.linksMensaisEmp = linksMensaisEmp;
    DADOS.cliquesMensaisEmp = cliquesMensaisEmp;

    // Totais
    const totalLinks = Object.values(linksByCompany).reduce((a, b) => a + b, 0);
    const totalCliques = Object.values(cliquesByCompany).reduce((a, b) => a + b, 0);
    console.log('\nTotal Links Enviados:', totalLinks);
    console.log('Total Cliques:', totalCliques);

    // 8. Salvar
    fs.writeFileSync(path.join(DIR, 'dados.js'), 'const DADOS = ' + JSON.stringify(DADOS), 'utf-8');
    console.log('\ndados.js salvo.');
}
main().catch(e => { console.error(e); process.exit(1); });
