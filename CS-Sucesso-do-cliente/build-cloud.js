/**
 * Cloud build script for CS Dashboard - runs in GitHub Actions.
 * Fetches data from Power BI DAX API + HubSpot + local controle CSV.
 * Produces dados.js identical in format to build-data.js.
 *
 * Required env vars:
 *   FABRIC_REFRESH_TOKEN - Microsoft refresh token
 *   FABRIC_TENANT_ID     - Azure AD tenant ID
 *   HUBSPOT_TOKEN        - HubSpot API bearer token
 *
 * Usage: node build-cloud.js
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const readline = require('readline');
const querystring = require('querystring');
const { Connection, Request } = require('tedious');

const DIR = __dirname;

// ===================== LAKEHOUSE (Fabric SQL endpoint) =====================
// VestiHouse lakehouse - workspace 2929476c-... / lakehouse 21b85aa7-...
// Descoberto via: GET /v1.0/myorg/groups/{ws}/datasets/{ds}/datasources (Painel CS)
const SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com';
const SQL_DATABASE = 'VestiHouse';

// ===================== CONSTANTS =====================
const WORKSPACE_ID = 'aced753a-0f0e-4bcf-9264-72f6496cf2cf';
const DATASET_ID = 'b3377e38-83ae-4ea2-a4fd-6d7a496f3a93'; // CS - Sucesso do Cliente 2024 (2025 live connection quebrada)
const DAX_ENDPOINT = `/v1.0/myorg/groups/${WORKSPACE_ID}/datasets/${DATASET_ID}/executeQueries`;

// VestiPago workspace + dataset (para lista de empresas com VestiPago)
const VP_WORKSPACE_ID = 'f80301c2-8735-40d2-8662-1f8a627d3f61';
const VP_DATASET_ID = '606be0ee-2c8c-4f43-8ad6-0be04f95d616';

// Invoices - dataset "Painel CS" (mesmo workspace do Oráculo)
const INV_WORKSPACE_ID = '2929476c-7b92-4366-9236-ccd13ffbd917';
const INV_DATASET_ID = '583e34d7-6dd1-467b-86aa-3b74cfe1ca56';

// Confeccao Métricas 2025 - Status Empresa + Controle de Estoque (mesmo workspace principal)
const METRICAS_WORKSPACE_ID = '786bfd95-0733-4fcb-aa84-ef2c97518959';
const METRICAS_DATASET_ID = '6d232602-d209-4dab-8be5-d9c34db57c0b';

// Frete - duas fontes que se complementam:
// (1) "Relatorio Confeccoes - Agencia" - canal antigo, ~34 empresas, match por nome
// (2) "Painel Frete" (workspace Vesti / Metricas) - 'OnLog - Fechamento', match por idDominio.
//     Cobre clientes que o canal (1) nao tem (ex: Arary).
// Damos merge dos dois mapas; se uma empresa aparece nos dois, somamos.
const FRETE_WORKSPACE_ID = '0f5bd202-471f-482d-bf3d-38295044d7db';
const FRETE_DATASET_ID = '92a0cf18-2bfd-4b02-873f-615df3ce2d7f';
const FRETE2_WORKSPACE_ID = '786bfd95-0733-4fcb-aa84-ef2c97518959';
const FRETE2_DATASET_ID = '6fd1cfe9-cedc-4028-8d11-b6eb1d653d46';

// Oráculo Fabric workspace + datasets
const FABRIC_CLIENT_ID = '14d82eec-204b-4c2f-b7e8-296a70dab67e';
const ORACULO_WS_ID = '2929476c-7b92-4366-9236-ccd13ffbd917';
const ORACULO_DS_ID = 'c6a480e9-2db4-45f7-ba67-b489407f59e6';
const ORACULO_PAINEIS_WS_ID = '63a65f3e-d96b-446e-a01d-f219132e1144';

const ORACULO_PIPELINE_ID = '794686264';
const ORACULO_STAGES = {
    '1165541427':'Fila','1165361278':'Grupo de Implementação','1165350737':'Reunião 1',
    '1165350738':'Configurações Iniciais','1273974154':'Link de relatório',
    '1199622545':'Problema conta Meta ou YCloud','1180878228':'Acompanhamento e melhorias prompt',
    '1165350742':'Eventos Vesti','1216864772':'Agente Aquecimento de leads',
    '1204236378':'Integração','1183765142':'Agente Inativos','1269319857':'Campanhas',
    '1165361281':'Concluído','1238455699':'Parado','1249275660':'Churn'
};

// Load .env file if present (for local execution)
const _envPath = path.join(DIR, '.env');
const _localEnv = {};
if (fs.existsSync(_envPath)) {
    fs.readFileSync(_envPath, 'utf-8').split('\n').forEach(l => {
        const m = l.match(/^([^#=]+)=(.*)$/);
        if (m) _localEnv[m[1].trim()] = m[2].trim();
    });
}

const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN || _localEnv.HUBSPOT_TOKEN || '';
const FABRIC_REFRESH_TOKEN = process.env.FABRIC_REFRESH_TOKEN || _localEnv.FABRIC_REFRESH_TOKEN || '';
const FABRIC_TENANT_ID = process.env.FABRIC_TENANT_ID || _localEnv.FABRIC_TENANT_ID || '';

// ===================== HTTP HELPERS =====================
function httpsRequest(options, body) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                const raw = Buffer.concat(chunks).toString();
                resolve({ statusCode: res.statusCode, headers: res.headers, body: raw });
            });
        });
        req.on('error', reject);
        if (body) req.write(body);
        req.end();
    });
}

// ===================== AUTH: MICROSOFT TOKEN REFRESH =====================
async function getAccessToken() {
    console.log('Authenticating with Microsoft...');
    if (!FABRIC_REFRESH_TOKEN || !FABRIC_TENANT_ID) {
        throw new Error('FABRIC_REFRESH_TOKEN and FABRIC_TENANT_ID env vars are required');
    }

    const postBody = querystring.stringify({
        client_id: process.env.FABRIC_CLIENT_ID || _localEnv.FABRIC_CLIENT_ID || '04b07795-8ddb-461a-bbee-02f9e1bf7b46',
        grant_type: 'refresh_token',
        refresh_token: FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
    });

    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: `/${FABRIC_TENANT_ID}/oauth2/v2.0/token`,
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Content-Length': Buffer.byteLength(postBody),
        },
    }, postBody);

    const data = JSON.parse(res.body);
    if (!data.access_token) {
        console.error('Token response:', res.body.substring(0, 500));
        throw new Error('Failed to get access token: ' + (data.error_description || data.error || 'unknown'));
    }

    console.log('  Access token obtained.');

    // Save new refresh token if returned (for GitHub Action to update the secret)
    if (data.refresh_token) {
        const rtPath = path.join(DIR, '.new_refresh_token');
        fs.writeFileSync(rtPath, data.refresh_token, 'utf-8');
        console.log('  New refresh token saved to .new_refresh_token');
        // Also update .env if it exists (for local execution)
        if (fs.existsSync(_envPath)) {
            let env = fs.readFileSync(_envPath, 'utf-8');
            env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + data.refresh_token);
            fs.writeFileSync(_envPath, env, 'utf-8');
            console.log('  .env refresh token updated');
        }
    }

    return data.access_token;
}

// ===================== AUTH: SQL SCOPE TOKEN =====================
// Fabric SQL endpoint usa audience database.windows.net (dupla barra eh quirk do AAD).
// O refresh token precisa vir de um client_id que tenha database.windows.net nas permissoes —
// Azure CLI (04b07795-8ddb-461a-bbee-02f9e1bf7b46) funciona; Graph CLI (14d82eec-...) NAO.
async function getSqlAccessToken() {
    console.log('Authenticating SQL (Fabric lakehouse)...');
    const postBody = querystring.stringify({
        client_id: _localEnv.FABRIC_CLIENT_ID || process.env.FABRIC_CLIENT_ID || '04b07795-8ddb-461a-bbee-02f9e1bf7b46',
        grant_type: 'refresh_token',
        refresh_token: FABRIC_REFRESH_TOKEN,
        scope: 'https://database.windows.net//.default offline_access',
    });
    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: `/${FABRIC_TENANT_ID}/oauth2/v2.0/token`,
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Content-Length': Buffer.byteLength(postBody),
        },
    }, postBody);
    const data = JSON.parse(res.body);
    if (!data.access_token) {
        console.error('SQL token response:', res.body.substring(0, 500));
        throw new Error('Failed to get SQL access token: ' + (data.error_description || data.error || 'unknown'));
    }
    console.log('  SQL token obtained.');
    // Rotaciona o refresh token (mesmo padrao do getAccessToken)
    if (data.refresh_token) {
        fs.writeFileSync(path.join(DIR, '.new_refresh_token'), data.refresh_token, 'utf-8');
        if (fs.existsSync(_envPath)) {
            let env = fs.readFileSync(_envPath, 'utf-8');
            env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + data.refresh_token);
            fs.writeFileSync(_envPath, env, 'utf-8');
        }
    }
    return data.access_token;
}

// ===================== SQL QUERY (tedious) =====================
function runSqlQuery(token, query, label, timeoutMs) {
    return new Promise((resolve, reject) => {
        console.log(`  SQL: ${label}...`);
        const conn = new Connection({
            server: SQL_SERVER,
            authentication: { type: 'azure-active-directory-access-token', options: { token } },
            options: {
                database: SQL_DATABASE,
                encrypt: true,
                port: 1433,
                requestTimeout: timeoutMs || 180000,
                connectTimeout: 30000,
            },
        });
        const rows = [];
        let done = false;
        conn.on('connect', err => {
            if (err) { done = true; reject(err); return; }
            const request = new Request(query, (err2) => {
                if (err2 && !done) { done = true; reject(err2); }
                conn.close();
            });
            request.on('row', columns => {
                const row = {};
                columns.forEach(col => { row[col.metadata.colName] = col.value; });
                rows.push(row);
            });
            request.on('requestCompleted', () => {
                if (!done) { done = true; console.log(`  ${label}: ${rows.length} rows`); resolve(rows); }
            });
            conn.execSql(request);
        });
        conn.connect();
    });
}

// ===================== LAKEHOUSE FETCH: empresas ativas via ODBC_Domains =====================
// Fonte de verdade: ODBC_Domains WHERE modulos LIKE '%vendas%' — todas as marcas com modulo
// Vendas habilitado, excluindo trial/treino/teste. Cada linha eh uma company de ODBC_Companies
// (matriz + filiais) — mesmo padrao do PainelCSGerencial. Filiais sao detectadas via
// ROW_NUMBER() OVER (PARTITION BY domain_id ORDER BY created_at ASC): rn=1 eh matriz, rn>1
// eh filial. Enriquecemos cada company com Confeccao2025_Query1 (Status, Anjo, Canal, etc.)
// via JOIN c.id = q.[Id Empresa].
async function fetchLakehouseEmpresas(sqlToken) {
    const q = `
        WITH active_domains AS (
            SELECT id, name FROM dbo.ODBC_Domains
            WHERE modulos LIKE '%vendas%'
              AND (partner_id IS NULL OR partner_id NOT IN (
                  'ff66c2f1-1f9f-456c-9308-028e48c89582',
                  '25fec57c-620c-4ecd-ae7d-cd4fee27b158'
              ))
              AND name NOT LIKE '%teste%'
              AND name NOT LIKE '%Teste%'
        ),
        ranked_companies AS (
            SELECT
                c.id, c.domain_id, c.tax_document, c.social_name, c.company_name,
                c.scheme_url, c.created_at, c.status,
                ROW_NUMBER() OVER (PARTITION BY c.domain_id ORDER BY c.created_at ASC) AS rn
            FROM dbo.ODBC_Companies c
            WHERE c.domain_id IN (SELECT id FROM active_domains)
        )
        SELECT
            rc.id              AS empresa_id,
            rc.domain_id       AS dominio_id,
            rc.tax_document    AS cnpj,
            rc.social_name     AS razao_social,
            rc.company_name    AS company_name,
            rc.scheme_url      AS scheme_url,
            rc.created_at      AS created_at,
            rc.status          AS company_status,
            rc.rn              AS row_num,
            d.name             AS domain_name,
            q.[Nome Fantasia]  AS q_nome_fantasia,
            q.[Status Empresa] AS q_status,
            q.[Canal de Vendas] AS q_canal,
            q.Anjo             AS q_anjo,
            q.Integracao       AS q_integracao,
            q.Email            AS q_email,
            q.[Tipo _Atacado | Varejo_] AS q_tipo,
            q.Tags             AS q_tags,
            q.[Modulos 2]      AS q_modulos
        FROM ranked_companies rc
        INNER JOIN active_domains d ON d.id = rc.domain_id
        LEFT JOIN dbo.Confeccao2025_Query1 q ON q.[Id Empresa] = rc.id
    `;
    const rows = await runSqlQuery(sqlToken, q, 'Lakehouse empresas (vendas + filiais)');

    const empresas = [];
    for (const r of rows) {
        const empresaId = r.empresa_id;
        if (!empresaId) continue;
        const isFilial = (r.row_num || 1) > 1;
        const statusText = r.q_status === 1 ? 'Ativa' : r.q_status === 2 ? 'Desativada' : '';
        empresas.push({
            id: empresaId,
            dominioId: r.dominio_id,
            rn: r.row_num || 1,
            isFilial,
            isMatriz: !isFilial,
            cnpj: r.cnpj || '',
            razaoSocial: r.razao_social || r.company_name || '',
            // nomeFantasia: pra matriz prefere domain_name (consistente com PainelCSGerencial),
            // pra filial usa company_name (nome especifico da filial)
            nomeFantasia: isFilial ? (r.company_name || r.domain_name || '') : (r.q_nome_fantasia || r.company_name || r.domain_name || ''),
            nomeDominio: r.domain_name || r.scheme_url || '',
            schemeUrl: r.scheme_url || '',
            canal: r.q_canal || '',
            anjo: r.q_anjo || '',
            integracao: r.q_integracao || '',
            statusEmpresa: statusText,
            email: r.q_email || '',
            tipoAtacado: r.q_tipo || '',
            tags: r.q_tags || '',
            modulos: r.q_modulos || '',
            criacao: r.created_at ? (r.created_at.toISOString ? r.created_at.toISOString() : String(r.created_at)) : '',
            fromLakehouse: true,
        });
    }
    const matrizes = empresas.filter(e => !e.isFilial).length;
    const filiais = empresas.filter(e => e.isFilial).length;
    console.log('  Lakehouse empresas: ' + empresas.length +
        ' (' + matrizes + ' matrizes + ' + filiais + ' filiais, ' +
        empresas.filter(e => e.statusEmpresa === 'Ativa').length + ' Ativa)');
    return empresas;
}

// ===================== LAKEHOUSE: validacao cruzada via MongoDB_Pedidos_Geral =====================
// Compara totais de pedidos/GMV da fonte atual (Merged Pedidos DAX) contra MongoDB_Pedidos_Geral
// agregado por dominioId. Nao bloqueia o build — so loga diferencas grandes.
async function validateAgainstMongoPedidos(sqlToken, empresasList) {
    try {
        const q = `
            SELECT domainId, companyId, COUNT(*) AS qtd, SUM(summary_total) AS valor
            FROM dbo.MongoDB_Pedidos_Geral
            WHERE domainId IS NOT NULL
            GROUP BY domainId, companyId
        `;
        const rows = await runSqlQuery(sqlToken, q, 'MongoDB_Pedidos_Geral (validacao)');
        const mongoByDominio = new Map();
        for (const r of rows) {
            const d = Number(r.domainId);
            if (!Number.isFinite(d)) continue;
            const cur = mongoByDominio.get(d) || { qtd: 0, valor: 0 };
            cur.qtd += Number(r.qtd) || 0;
            cur.valor += Number(r.valor) || 0;
            mongoByDominio.set(d, cur);
        }
        let totalMongoQtd = 0, totalMongoVal = 0;
        for (const v of mongoByDominio.values()) { totalMongoQtd += v.qtd; totalMongoVal += v.valor; }
        let totalBuildQtd = 0, totalBuildVal = 0;
        for (const e of empresasList) { totalBuildQtd += (e.pedidos || 0); totalBuildVal += (e.valTotal || e.gmv || 0); }

        console.log('\n=== Validacao cruzada MongoDB_Pedidos_Geral ===');
        console.log('  Mongo : ' + totalMongoQtd + ' pedidos, R$ ' + totalMongoVal.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, '.'));
        console.log('  Build : ' + totalBuildQtd + ' pedidos, R$ ' + totalBuildVal.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, '.'));
        const deltaQtd = totalMongoQtd > 0 ? ((totalBuildQtd - totalMongoQtd) / totalMongoQtd * 100) : 0;
        const deltaVal = totalMongoVal > 0 ? ((totalBuildVal - totalMongoVal) / totalMongoVal * 100) : 0;
        console.log('  Delta : ' + deltaQtd.toFixed(1) + '% qtd, ' + deltaVal.toFixed(1) + '% valor');

        // Top 5 diffs por dominio (apenas os que temos no build)
        const diffs = [];
        for (const e of empresasList) {
            if (!e.idDominio) continue;
            const d = Number(e.idDominio);
            const m = mongoByDominio.get(d);
            if (!m) continue;
            const bQtd = e.pedidos || 0;
            const bVal = e.valTotal || e.gmv || 0;
            if (m.qtd < 5 && bQtd < 5) continue;
            const diffPct = m.qtd > 0 ? Math.abs(bQtd - m.qtd) / m.qtd : 0;
            if (diffPct > 0.10) {
                diffs.push({ nome: e.nomeFantasia || e.nomeDominio, dominioId: d, buildQtd: bQtd, mongoQtd: m.qtd, buildVal: bVal, mongoVal: m.valor });
            }
        }
        if (diffs.length > 0) {
            console.log('  Empresas com diff >10% em quantidade (top 5):');
            diffs.sort((a, b) => Math.abs(b.mongoQtd - b.buildQtd) - Math.abs(a.mongoQtd - a.buildQtd))
                .slice(0, 5)
                .forEach(d => console.log('   ', d.nome, '(dom ' + d.dominioId + '): build=' + d.buildQtd + ' vs mongo=' + d.mongoQtd));
        }
    } catch (err) {
        console.warn('  AVISO: validacao MongoDB_Pedidos_Geral falhou:', err.message);
    }
}

// ===================== POWER BI DAX QUERY =====================
async function executeDaxQueryOn(accessToken, wsId, dsId, daxQuery, label) {
    console.log(`  Querying: ${label}...`);
    const bodyStr = JSON.stringify({
        queries: [{ query: daxQuery }],
        serializerSettings: { includeNulls: true },
    });

    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: `/v1.0/myorg/groups/${wsId}/datasets/${dsId}/executeQueries`,
        method: 'POST',
        headers: {
            'Authorization': 'Bearer ' + accessToken,
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(bodyStr),
        },
    }, bodyStr);

    if (res.statusCode !== 200) { console.error(`  ERROR ${label}: HTTP ${res.statusCode}`); return []; }
    const data = JSON.parse(res.body);
    if (data.error) { console.error(`  ERROR ${label}: ${JSON.stringify(data.error).substring(0, 300)}`); return []; }
    const rows = (data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows) || [];
    const cleaned = rows.map(row => {
        const obj = {};
        for (const [key, val] of Object.entries(row)) {
            const match = key.match(/\[(.+)\]$/);
            obj[match ? match[1] : key] = val;
        }
        return obj;
    });
    console.log(`  ${label}: ${cleaned.length} rows`);
    return cleaned;
}

async function executeDaxQuery(accessToken, daxQuery, label) {
    console.log(`  Querying: ${label}...`);
    const bodyStr = JSON.stringify({
        queries: [{ query: daxQuery }],
        serializerSettings: { includeNulls: true },
    });

    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: DAX_ENDPOINT,
        method: 'POST',
        headers: {
            'Authorization': 'Bearer ' + accessToken,
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(bodyStr),
        },
    }, bodyStr);

    if (res.statusCode !== 200) {
        console.error(`  ERROR ${label}: HTTP ${res.statusCode} - ${res.body.substring(0, 300)}`);
        return [];
    }

    const data = JSON.parse(res.body);
    if (data.error) {
        console.error(`  ERROR ${label}: ${JSON.stringify(data.error).substring(0, 300)}`);
        return [];
    }

    const rows = (data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows) || [];

    // Strip table prefix from column names: "TableName[Column]" -> "Column"
    const cleaned = rows.map(row => {
        const obj = {};
        for (const [key, val] of Object.entries(row)) {
            const match = key.match(/\[(.+)\]$/);
            obj[match ? match[1] : key] = val;
        }
        return obj;
    });

    console.log(`  ${label}: ${cleaned.length} rows`);
    return cleaned;
}

// ===================== CSV PARSER =====================
function parseCSVLine(line) {
    const fields = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQuotes) {
            if (ch === '"' && line[i + 1] === '"') { current += '"'; i++; }
            else if (ch === '"') { inQuotes = false; }
            else { current += ch; }
        } else {
            if (ch === '"') { inQuotes = true; }
            else if (ch === ',') { fields.push(current.trim()); current = ''; }
            else { current += ch; }
        }
    }
    fields.push(current.trim());
    return fields;
}

async function readCSV(filename, onRow) {
    const filePath = path.join(DIR, filename);
    if (!fs.existsSync(filePath)) { console.log('  SKIP: ' + filename + ' not found'); return; }
    const stream = fs.createReadStream(filePath, { encoding: 'utf-8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    let headers = null;
    let count = 0;
    for await (const line of rl) {
        if (!line.trim()) continue;
        const fields = parseCSVLine(line);
        if (!headers) { headers = fields; continue; }
        const row = {};
        headers.forEach((h, i) => { row[h] = fields[i] || ''; });
        onRow(row);
        count++;
    }
    console.log('  ' + filename + ': ' + count + ' rows');
}

// ===================== HUBSPOT =====================
function hubspotRequest(endpoint, method, body) {
    return new Promise((resolve, reject) => {
        const req = https.request({
            hostname: 'api.hubapi.com',
            path: endpoint,
            method: method || 'GET',
            headers: {
                'Authorization': 'Bearer ' + HUBSPOT_TOKEN,
                'Content-Type': 'application/json',
            },
        }, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
                catch (e) { reject(e); }
            });
        });
        req.on('error', reject);
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

async function fetchOraculoTickets() {
    console.log('  Fetching HubSpot Oráculo tickets...');
    if (!HUBSPOT_TOKEN) {
        console.log('  WARN: HUBSPOT_TOKEN not set, skipping HubSpot');
        return [];
    }
    try {
        const allTickets = [];
        let after = 0;
        let hasMore = true;
        while (hasMore) {
            const body = {
                filterGroups: [{ filters: [{ propertyName: 'hs_pipeline', operator: 'EQ', value: ORACULO_PIPELINE_ID }] }],
                properties: ['subject', 'hs_pipeline_stage', 'createdate', 'hs_lastmodifieddate'],
                limit: 100,
            };
            if (after) body.after = after;
            const data = await hubspotRequest('/crm/v3/objects/tickets/search', 'POST', body);
            const results = data.results || [];
            for (const t of results) {
                const stageId = t.properties.hs_pipeline_stage;
                let companyName = (t.properties.subject || '')
                    .replace(/^[ÓO]R[ÁA]CULO\s*-\s*/i, '').replace(/\s*-\s*[ÓO]r[áa]culo.*/i, '')
                    .replace(/\s*-\s*Agente.*/i, '').replace(/\s*\|.*/, '').replace(/\s*\(.*\)/, '').trim();
                if (companyName.startsWith('Oráculo ')) companyName = companyName.replace('Oráculo ', '').trim();
                if (companyName.startsWith('Óraculo ')) companyName = companyName.replace('Óraculo ', '').trim();
                allTickets.push({
                    id: t.id,
                    subject: t.properties.subject,
                    companyName,
                    stageId,
                    stageName: ORACULO_STAGES[stageId] || stageId,
                    created: t.properties.createdate,
                    modified: t.properties.hs_lastmodifieddate,
                });
            }
            if (data.paging && data.paging.next && data.paging.next.after) {
                after = data.paging.next.after;
            } else {
                hasMore = false;
            }
        }
        console.log('  HubSpot Oráculo: ' + allTickets.length + ' tickets');
        return allTickets;
    } catch (e) {
        console.log('  WARN: HubSpot fetch failed: ' + e.message);
        return [];
    }
}

// ===================== ORÁCULO FABRIC: PAINEL STATS (with monthly data) =====================
async function fetchOraculoPainelStats(accessToken) {
    try {
        console.log('  Listing Oráculo painéis datasets...');
        const dsRes = await httpsRequest({
            hostname: 'api.powerbi.com',
            path: '/v1.0/myorg/groups/' + ORACULO_PAINEIS_WS_ID + '/datasets',
            method: 'GET',
            headers: { 'Authorization': 'Bearer ' + accessToken },
        });
        if (dsRes.statusCode !== 200) { console.log('  WARN: Oráculo painéis list failed HTTP ' + dsRes.statusCode); return new Map(); }
        const datasets = JSON.parse(dsRes.body).value || [];

        // Use direct queries instead of KPI measures (which return null for some datasets)
        const daxPedidos = "EVALUATE SUMMARIZECOLUMNS('f_Pedidos Oraculo'[settings_createdAt_TIMESTAMP], \"pedidos\", COUNTROWS('f_Pedidos Oraculo'), \"vendas\", SUM('f_Pedidos Oraculo'[summary_total]))";
        const daxInteracoes = "EVALUATE SUMMARIZECOLUMNS('f_Interacoes Oraculo Semanal'[DataReferencia], \"interacoes\", COUNTROWS('f_Interacoes Oraculo Semanal'), \"ia\", SUM('f_Interacoes Oraculo Semanal'[IA]))";

        const map = new Map();
        let ok = 0, fail = 0;
        for (const ds of datasets) {
            if (ds.name === 'Report Usage Metrics Model') continue;
            const name = ds.name.replace(' - Oráculo', '').replace(' - Oraculo', '').trim();
            try {
                // Fetch pedidos
                const pBody = JSON.stringify({ queries: [{ query: daxPedidos }], serializerSettings: { includeNulls: true } });
                const pRes = await httpsRequest({
                    hostname: 'api.powerbi.com',
                    path: '/v1.0/myorg/groups/' + ORACULO_PAINEIS_WS_ID + '/datasets/' + ds.id + '/executeQueries',
                    method: 'POST',
                    headers: { 'Authorization': 'Bearer ' + accessToken, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(pBody) },
                }, pBody);
                // Fetch interacoes
                const iBody = JSON.stringify({ queries: [{ query: daxInteracoes }], serializerSettings: { includeNulls: true } });
                const iRes = await httpsRequest({
                    hostname: 'api.powerbi.com',
                    path: '/v1.0/myorg/groups/' + ORACULO_PAINEIS_WS_ID + '/datasets/' + ds.id + '/executeQueries',
                    method: 'POST',
                    headers: { 'Authorization': 'Bearer ' + accessToken, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(iBody) },
                }, iBody);

                const pRows = (pRes.statusCode === 200 ? JSON.parse(pRes.body).results?.[0]?.tables?.[0]?.rows : null) || [];
                const iRows = (iRes.statusCode === 200 ? JSON.parse(iRes.body).results?.[0]?.tables?.[0]?.rows : null) || [];

                // Aggregate by month
                const monthly = {};
                let totalPedidos = 0, totalVendas = 0, totalInteracoes = 0, totalIA = 0;
                pRows.forEach(r => {
                    const dt = r["f_Pedidos Oraculo[settings_createdAt_TIMESTAMP]"] || '';
                    const mes = dt.substring(0, 7);
                    if (!mes) return;
                    const ped = r['[pedidos]'] || 0;
                    const ven = r['[vendas]'] || 0;
                    if (!monthly[mes]) monthly[mes] = { pedidos: 0, vendas: 0, interacoes: 0, ia: 0 };
                    monthly[mes].pedidos += ped;
                    monthly[mes].vendas += ven;
                    totalPedidos += ped;
                    totalVendas += ven;
                });
                iRows.forEach(r => {
                    const dt = r["f_Interacoes Oraculo Semanal[DataReferencia]"] || '';
                    const mes = dt.substring(0, 7);
                    if (!mes) return;
                    const inter = r['[interacoes]'] || 0;
                    const ia = r['[ia]'] || 0;
                    if (!monthly[mes]) monthly[mes] = { pedidos: 0, vendas: 0, interacoes: 0, ia: 0 };
                    monthly[mes].interacoes += inter;
                    monthly[mes].ia += ia;
                    totalInteracoes += inter;
                    totalIA += ia;
                });

                const atendimentos = totalInteracoes; // unique customers approximated by total interactions
                const pctIA = totalInteracoes > 0 ? Math.round((totalIA / totalInteracoes) * 1000) / 10 : 0;

                // Build monthly array sorted desc
                const mensalArr = Object.entries(monthly)
                    .map(([mes, d]) => ({ mes, pedidos: d.pedidos, vendas: Math.round(d.vendas * 100) / 100, interacoes: d.interacoes, ia: d.ia, pctIA: d.interacoes > 0 ? Math.round((d.ia / d.interacoes) * 1000) / 10 : 0 }))
                    .sort((a, b) => b.mes.localeCompare(a.mes))
                    .slice(0, 12);

                if (totalPedidos > 0 || totalInteracoes > 0) {
                    map.set(name.toLowerCase(), {
                        name,
                        pedidosOraculo: totalPedidos,
                        interacoesOraculo: totalInteracoes,
                        atendimentosOraculo: atendimentos,
                        pctIAOraculo: pctIA,
                        vendasOraculo: Math.round(totalVendas * 100) / 100,
                        mensal: mensalArr,
                    });
                    ok++;
                } else { fail++; }
            } catch (e) { fail++; }
        }
        console.log('  Oráculo painéis stats: ' + ok + ' OK, ' + fail + ' failed (of ' + datasets.length + ')');
        return map;
    } catch (e) {
        console.log('  WARN: Oráculo painéis fetch failed: ' + e.message);
        return new Map();
    }
}

// ===================== ORÁCULO FABRIC: CONFIGURATIONS =====================
async function fetchOraculoConfigurations(accessToken) {
    try {
        const query = `EVALUATE SELECTCOLUMNS(
            FILTER(Oraculo_configurations, NOT ISBLANK(Oraculo_configurations[n8n_url])),
            "company_id", Oraculo_configurations[company_id],
            "domain_id", Oraculo_configurations[domain_id],
            "name", Oraculo_configurations[name],
            "n8n_url", Oraculo_configurations[n8n_url],
            "phone_origin", Oraculo_configurations[phone_origin],
            "created_at", Oraculo_configurations[created_at],
            "updated_at", Oraculo_configurations[updated_at],
            "link_report", Oraculo_configurations[link_report],
            "phone_by_vesti", Oraculo_configurations[phone_by_vesti],
            "catalogue_with_price", Oraculo_configurations[catalogue_with_price],
            "agent_retail", Oraculo_configurations[agent_retail],
            "works_with_closed_square", Oraculo_configurations[works_with_closed_square],
            "keep_assigned_seller", Oraculo_configurations[keep_assigned_seller]
        )`;
        const rows = await executeDaxQueryOn(accessToken, ORACULO_WS_ID, ORACULO_DS_ID, query, 'Oráculo Configurations');
        const map = new Map();
        rows.forEach(r => {
            const companyId = r.company_id || '';
            if (companyId) {
                map.set(companyId, {
                    name: r.name || '',
                    domain_id: r.domain_id || '',
                    n8n_url: r.n8n_url || '',
                    phone: r.phone_origin || '',
                    created_at: r.created_at || '',
                    updated_at: r.updated_at || '',
                    link_report: r.link_report || '',
                    phone_by_vesti: r.phone_by_vesti === '1' || r.phone_by_vesti === 1 || r.phone_by_vesti === true,
                    catalogue_with_price: r.catalogue_with_price === '1' || r.catalogue_with_price === 1 || r.catalogue_with_price === true,
                    agent_retail: r.agent_retail === '1' || r.agent_retail === 1 || r.agent_retail === true,
                    works_with_closed_square: r.works_with_closed_square === '1' || r.works_with_closed_square === 1 || r.works_with_closed_square === true,
                    keep_assigned_seller: r.keep_assigned_seller === '1' || r.keep_assigned_seller === 1 || r.keep_assigned_seller === true,
                });
            }
        });
        console.log('  Oráculo configurations: ' + map.size);
        return map;
    } catch (e) {
        console.log('  WARN: Oráculo config fetch failed: ' + e.message);
        return new Map();
    }
}

// ===================== INVOICE TOTAL PARSER =====================
function parseInvoiceTotal(s) {
    if (!s || typeof s !== 'string') return 0;
    s = s.trim();
    if (s.includes('BRL')) return parseFloat(s.replace('BRL', '').trim()) || 0;
    s = s.replace('R$', '').trim().replace(/\./g, '').replace(',', '.');
    return parseFloat(s) || 0;
}

// ===================== FUZZY MATCHING (same as build-data.js) =====================
function normalize(s) {
    return (s || '').toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/\s*(jeans|modas|moda|confeccoes|confecções|oficial|clothing|collection|acessorios|acessórios|tricot|ltda|me|eireli|s\.a\.|sa)\s*/gi, ' ')
        .replace(/[^a-z0-9]/g, ' ')
        .replace(/\s+/g, ' ').trim();
}

function buildMatchStructures(allEmpresas) {
    const empLookup = {};
    const empWords = {};
    allEmpresas.forEach(e => {
        const nome = e.nomeFantasia || e.nomeDominio;
        const n = normalize(nome);
        empLookup[n] = e;
        n.split(' ').filter(w => w.length >= 3).forEach(w => {
            if (!empWords[w]) empWords[w] = [];
            empWords[w].push({ emp: e, nome });
        });
    });
    return { empLookup, empWords };
}

function matchTicketToEmpresa(ticket, empLookup, empWords) {
    const tn = normalize(ticket.companyName);
    if (!tn || tn === 'oraculo' || tn === 'eventos') return null;

    if (empLookup[tn]) return empLookup[tn];

    for (const [en, emp] of Object.entries(empLookup)) {
        if (en.includes(tn) || tn.includes(en)) return emp;
    }

    const ticketWords = tn.split(' ').filter(w => w.length >= 3);
    if (ticketWords.length === 0) return null;

    let bestMatch = null, bestScore = 0;
    const candidates = new Map();
    ticketWords.forEach(tw => {
        for (const [word, emps] of Object.entries(empWords)) {
            if (word === tw || word.startsWith(tw) || tw.startsWith(word)) {
                emps.forEach(({ emp }) => {
                    const key = emp.id;
                    const prev = candidates.get(key) || { emp, score: 0 };
                    prev.score += (word === tw) ? 2 : 1;
                    candidates.set(key, prev);
                });
            }
        }
    });

    for (const [, c] of candidates) {
        if (c.score > bestScore) { bestScore = c.score; bestMatch = c.emp; }
    }

    return bestScore >= 2 ? bestMatch : null;
}

// ===================== MAIN =====================
async function main() {
    console.log('=== CS Dashboard Cloud Build ===\n');

    // ---------- 1. Authenticate with Microsoft ----------
    const accessToken = await getAccessToken();
    const sqlToken = await getSqlAccessToken();

    // ---------- 1b. Fetch empresas ativas do lakehouse (ODBC_Domains + enrichment) ----------
    // Esta eh a fonte de verdade para quais marcas aparecem no dashboard. Substitui o filtro
    // antigo baseado em Marcas e Planos (Excel) + Cadastros Empresas (DAX).
    console.log('\nFetching empresas ativas do Lakehouse (ODBC_Domains vendas)...');
    const lakehouseEmpresas = await fetchLakehouseEmpresas(sqlToken);
    // Index por id (c.id de ODBC_Companies, mesmo GUID de Merged Pedidos[ID Empresa])
    const lakehouseByEmpresaId = new Map();
    lakehouseEmpresas.forEach(e => { if (e.id) lakehouseByEmpresaId.set(e.id, e); });
    // Index por dominio (qualquer linha do dominio serve como fallback de enriquecimento)
    const lakehouseByDominio = new Map();
    lakehouseEmpresas.forEach(e => {
        if (!lakehouseByDominio.has(e.dominioId)) lakehouseByDominio.set(e.dominioId, e);
    });

    // ---------- 2. Query Power BI tables in parallel ----------
    console.log('\nQuerying Power BI DAX API...');

    const daxCadastros = "EVALUATE 'Cadastros Empresas'";
    const daxConfig = "EVALUATE 'Config Empresas'";
    const daxMarcas = "EVALUATE 'Marcas e Planos'";
    const daxProduct = "EVALUATE SUMMARIZECOLUMNS('Product'[Cadastros Users ( Vendedores ).CompanyId], \"LinksEnviados\", COUNTROWS('Product'))";
    const daxRankings = "EVALUATE SUMMARIZECOLUMNS('Rankings'[Cadastros Users ( Vendedores ).CompanyId], \"Cliques\", SUM('Rankings'[rankings.shared_links]))";

    // Links e Cliques mensais (global)
    const daxLinksMonthly = "EVALUATE SUMMARIZECOLUMNS('Product'[product_sent_lists.created_at].[Year], 'Product'[product_sent_lists.created_at].[MonthNo], \"Links\", COUNTROWS('Product'))";
    const daxCliquesMonthly = "EVALUATE SUMMARIZECOLUMNS('Rankings'[rankings.created_at].[Year], 'Rankings'[rankings.created_at].[MonthNo], \"Cliques\", SUM('Rankings'[rankings.shared_links]))";

    // Links e Cliques mensais por empresa
    const daxLinksCompanyMonthly = "EVALUATE SUMMARIZECOLUMNS('Product'[Cadastros Users ( Vendedores ).CompanyId], 'Product'[product_sent_lists.created_at].[Year], 'Product'[product_sent_lists.created_at].[MonthNo], \"Links\", COUNTROWS('Product'))";
    const daxCliquesCompanyMonthly = "EVALUATE SUMMARIZECOLUMNS('Rankings'[Cadastros Users ( Vendedores ).CompanyId], 'Rankings'[rankings.created_at].[Year], 'Rankings'[rankings.created_at].[MonthNo], \"Cliques\", SUM('Rankings'[rankings.shared_links]))";

    // Filtros de pagamento: captura todas as variações de cartão e pix
    const filtroCartao = `(CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "credit") || CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "cartão") || CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "cartao") || CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "crédito") || CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "credito") || CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "débito") || CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "debito"))`;
    const filtroPix = `(CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "pix") || CONTAINSSTRING('Merged Pedidos'[docs.payment.method], "PIX"))`;

    const daxPedidosPerCompany = `EVALUATE SUMMARIZECOLUMNS('Merged Pedidos'[ID Empresa], "TotalPedidos", COUNTROWS('Merged Pedidos'), "TotalPagos", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Pago]=TRUE()), "TotalCancelados", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Cancelado]=TRUE()), "TotalPendentes", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Pendente]=TRUE()), "ValTotal", SUM('Merged Pedidos'[Total]), "ValPagos", CALCULATE(SUM('Merged Pedidos'[Total]), 'Merged Pedidos'[Pago]=TRUE()), "ValCancelados", CALCULATE(SUM('Merged Pedidos'[Total]), 'Merged Pedidos'[Cancelado]=TRUE()), "TransCartao", CALCULATE(COUNTROWS('Merged Pedidos'), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroCartao}), "TransPix", CALCULATE(COUNTROWS('Merged Pedidos'), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroPix}), "ValCartao", CALCULATE(SUM('Merged Pedidos'[Total]), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroCartao}), "ValPix", CALCULATE(SUM('Merged Pedidos'[Total]), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroPix}))`;

    const daxPedidosMonthly = `EVALUATE SUMMARIZECOLUMNS('Merged Pedidos'[Data Criacao].[Year], 'Merged Pedidos'[Data Criacao].[MonthNo], "TotalPedidos", COUNTROWS('Merged Pedidos'), "Pagos", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Pago]=TRUE()), "Cancelados", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Cancelado]=TRUE()), "Pendentes", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Pendente]=TRUE()), "ValTotal", SUM('Merged Pedidos'[Total]), "ValPagos", CALCULATE(SUM('Merged Pedidos'[Total]), 'Merged Pedidos'[Pago]=TRUE()), "Cartao", CALCULATE(COUNTROWS('Merged Pedidos'), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroCartao}), "Pix", CALCULATE(COUNTROWS('Merged Pedidos'), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroPix}), "ValCartao", CALCULATE(SUM('Merged Pedidos'[Total]), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroCartao}), "ValPix", CALCULATE(SUM('Merged Pedidos'[Total]), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroPix}))`;

    // Pedidos per company per month (churn + period filters + payment)
    const daxPedidosCompanyMonthly = `EVALUATE SUMMARIZECOLUMNS('Merged Pedidos'[ID Empresa], 'Merged Pedidos'[Data Criacao].[Year], 'Merged Pedidos'[Data Criacao].[MonthNo], "Qtd", COUNTROWS('Merged Pedidos'), "Pagos", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Pago]=TRUE()), "Cancelados", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Cancelado]=TRUE()), "Pendentes", CALCULATE(COUNTROWS('Merged Pedidos'), 'Merged Pedidos'[Pendente]=TRUE()), "Val", SUM('Merged Pedidos'[Total]), "ValPagos", CALCULATE(SUM('Merged Pedidos'[Total]), 'Merged Pedidos'[Pago]=TRUE()), "TC", CALCULATE(COUNTROWS('Merged Pedidos'), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroCartao}), "TP", CALCULATE(COUNTROWS('Merged Pedidos'), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroPix}), "VC", CALCULATE(SUM('Merged Pedidos'[Total]), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroCartao}), "VP", CALCULATE(SUM('Merged Pedidos'[Total]), NOT(ISBLANK('Merged Pedidos'[docs.payment.method])) && ${filtroPix}))`;

    // Invoices (Iugu) - from Painel CS dataset
    const daxInvoices = `EVALUATE SELECTCOLUMNS(Invoices, "invId", Invoices[id], "dominio", Invoices[Dominio], "marca", Invoices[Marca], "iugu_name", Invoices[Iugu_name], "due", Invoices[due_date_TIMESTAMP], "status", Invoices[status], "valor", Invoices[ValorFatura], "plan", Invoices[Plano])`;

    // Status Empresa + Controle de Estoque - from Confeccao Métricas 2025
    const daxMetricas = `EVALUATE SELECTCOLUMNS(Query1, "id", Query1[Id Empresa], "status", Query1[Status Empresa 2], "estoque", Query1[Controle de Estoque], "cs", Query1[Anjo])`;

    // Frete fonte 1: Relatorio Confeccoes - Agencia (match por nome)
    const daxFrete = `EVALUATE FILTER(SUMMARIZECOLUMNS(Merged[Companies.company_name], Merged[Recebido].[Year], Merged[Recebido].[MonthNo], "TotalFrete", SUM(Merged[Valor Frete])), [TotalFrete] > 0)`;
    // Frete fonte 2: Painel Frete -> OnLog - Fechamento (match por idDominio)
    const daxFrete2 = `EVALUATE FILTER(SUMMARIZECOLUMNS('OnLog - Fechamento'[Dominio], 'OnLog - Fechamento'[Data].[Year], 'OnLog - Fechamento'[Data].[MonthNo], "TotalFrete", SUM('OnLog - Fechamento'[ValorPostagem])), [TotalFrete] > 0)`;

    // Run all queries in parallel (including VestiPago companies from separate dataset)
    const [cadastrosRows, configRows, marcasRows, productRows, rankingsRows, pedidosCompanyRows, pedidosMonthlyRows, pedidosCompanyMonthlyRows, vestiPagoRows, linksMonthlyRows, cliquesMonthlyRows, linksCompanyMonthlyRows, cliquesCompanyMonthlyRows, invoiceRows, metricasRows, freteRows, frete2Rows] = await Promise.all([
        executeDaxQuery(accessToken, daxCadastros, 'Cadastros Empresas'),
        executeDaxQuery(accessToken, daxConfig, 'Config Empresas'),
        executeDaxQuery(accessToken, daxMarcas, 'Marcas e Planos'),
        executeDaxQuery(accessToken, daxProduct, 'Product (links)'),
        executeDaxQuery(accessToken, daxRankings, 'Rankings (cliques)'),
        executeDaxQuery(accessToken, daxPedidosPerCompany, 'Pedidos per Company'),
        executeDaxQuery(accessToken, daxPedidosMonthly, 'Pedidos Monthly'),
        executeDaxQuery(accessToken, daxPedidosCompanyMonthly, 'Pedidos Company Monthly'),
        executeDaxQueryOn(accessToken, VP_WORKSPACE_ID, VP_DATASET_ID, `EVALUATE SELECTCOLUMNS(Companies, "companyId", Companies[data.companyId])`, 'VestiPago Companies'),
        executeDaxQuery(accessToken, daxLinksMonthly, 'Links Monthly'),
        executeDaxQuery(accessToken, daxCliquesMonthly, 'Cliques Monthly'),
        executeDaxQuery(accessToken, daxLinksCompanyMonthly, 'Links Company Monthly'),
        executeDaxQuery(accessToken, daxCliquesCompanyMonthly, 'Cliques Company Monthly'),
        executeDaxQueryOn(accessToken, INV_WORKSPACE_ID, INV_DATASET_ID, daxInvoices, 'Invoices Iugu'),
        executeDaxQueryOn(accessToken, METRICAS_WORKSPACE_ID, METRICAS_DATASET_ID, daxMetricas, 'Métricas (Status/Estoque)'),
        executeDaxQueryOn(accessToken, FRETE_WORKSPACE_ID, FRETE_DATASET_ID, daxFrete, 'Frete (Relatorio Confeccoes)'),
        executeDaxQueryOn(accessToken, FRETE2_WORKSPACE_ID, FRETE2_DATASET_ID, daxFrete2, 'Frete (Painel Frete - OnLog)'),
    ]);

    // Build VestiPago set
    const vestiPagoSet = new Set();
    vestiPagoRows.forEach(r => { if (r.companyId) vestiPagoSet.add(r.companyId); });
    console.log('  VestiPago companies: ' + vestiPagoSet.size);

    // Build Status/Estoque map from Métricas
    const metricasMap = {};
    metricasRows.forEach(r => {
        const id = r.id;
        if (id) metricasMap[id] = { statusEmpresa: r.status || '', controleEstoque: r.estoque || '', cs: r.cs || '' };
    });
    console.log('  Métricas (status/estoque): ' + Object.keys(metricasMap).length);

    // Fallback: o dataset CS 2024 (usado desde 3e3b89f) nao cobre todas as empresas
    // novas — cenario normal ficou em ~35-40% sem status. Reaproveita o dados.js
    // anterior pra backfill quando a empresa ja tinha status conhecido.
    try {
        const prevPath = path.join(DIR, 'dados.js');
        if (fs.existsSync(prevPath)) {
            const prevTxt = fs.readFileSync(prevPath, 'utf-8');
            const prev = JSON.parse(prevTxt.replace(/^const DADOS\s*=\s*/, '').replace(/;\s*$/, ''));
            let backfilled = 0;
            (prev.empresas || []).forEach(e => {
                if (!e || !e.id) return;
                const cur = metricasMap[e.id];
                if (!cur || !cur.statusEmpresa) {
                    if (e.statusEmpresa || e.controleEstoque) {
                        metricasMap[e.id] = {
                            statusEmpresa: e.statusEmpresa || (cur && cur.statusEmpresa) || '',
                            controleEstoque: e.controleEstoque || (cur && cur.controleEstoque) || '',
                            cs: (cur && cur.cs) || '',
                        };
                        if (e.statusEmpresa) backfilled++;
                    }
                }
            });
            if (backfilled > 0) {
                console.log('  Métricas backfill (dados.js anterior): +' + backfilled + ' statusEmpresa reaproveitados');
            }
        }
    } catch (err) {
        console.warn('  AVISO: backfill de statusEmpresa a partir de dados.js anterior falhou:', err.message);
    }

    // Fonte 1: Relatorio Confeccoes - Agencia, mapeada por nome normalizado.
    const freteByCompany = {};
    freteRows.forEach(r => {
        const name = r['Companies.company_name'] || r['Merged[Companies.company_name]'] || '';
        const keys = Object.keys(r);
        const yearKey = keys.find(k => k.includes('Year'));
        const monthKey = keys.find(k => k.includes('MonthNo'));
        const year = yearKey ? r[yearKey] : null;
        const month = monthKey ? r[monthKey] : null;
        const frete = r['[TotalFrete]'] != null ? r['[TotalFrete]'] : (r['TotalFrete'] || 0);
        if (!name || !year || !month) return;
        const mes = year + '-' + String(month).padStart(2, '0');
        const key = normalize(name);
        if (!freteByCompany[key]) freteByCompany[key] = { mensal: [], total: 0 };
        freteByCompany[key].mensal.push({ mes, valor: Math.round(frete * 100) / 100 });
        freteByCompany[key].total += frete;
    });
    for (const c of Object.values(freteByCompany)) {
        c.mensal.sort((a, b) => b.mes.localeCompare(a.mes));
        c.total = Math.round(c.total * 100) / 100;
    }
    console.log('  Frete (Relatorio Confeccoes / nome): ' + Object.keys(freteByCompany).length + ' empresas, R$ ' +
        Object.values(freteByCompany).reduce((s, c) => s + c.total, 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 }));

    // Fonte 2: Painel Frete -> OnLog - Fechamento, mapeada por idDominio.
    const freteByDominio = {};
    (frete2Rows || []).forEach(r => {
        const keys = Object.keys(r);
        const dominioKey = keys.find(k => k.includes('Dominio'));
        const yearKey = keys.find(k => k.includes('Year'));
        const monthKey = keys.find(k => k.includes('MonthNo'));
        const dominio = dominioKey ? r[dominioKey] : null;
        const year = yearKey ? r[yearKey] : null;
        const month = monthKey ? r[monthKey] : null;
        const frete = r['[TotalFrete]'] != null ? r['[TotalFrete]'] : (r['TotalFrete'] || 0);
        if (!dominio || !year || !month) return;
        const mes = year + '-' + String(month).padStart(2, '0');
        const id = String(dominio);
        if (!freteByDominio[id]) freteByDominio[id] = { mensal: [], total: 0 };
        freteByDominio[id].mensal.push({ mes, valor: Math.round(frete * 100) / 100 });
        freteByDominio[id].total += frete;
    });
    for (const c of Object.values(freteByDominio)) {
        c.mensal.sort((a, b) => b.mes.localeCompare(a.mes));
        c.total = Math.round(c.total * 100) / 100;
    }
    console.log('  Frete (Painel Frete / idDominio): ' + Object.keys(freteByDominio).length + ' empresas, R$ ' +
        Object.values(freteByDominio).reduce((s, c) => s + c.total, 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 }));

    // Helper: combina os dois mapas para uma empresa especifica.
    // Soma totais e mensais (mesmo mes vira 1 entrada com soma dos valores).
    function getFreteCombinado(empresa) {
        const a = freteByCompany[normalize(empresa.nome)];
        const b = empresa.idDominio ? freteByDominio[String(empresa.idDominio)] : null;
        if (!a && !b) return null;
        if (a && !b) return a;
        if (b && !a) return b;
        // Merge
        const mensalMap = {};
        a.mensal.forEach(m => { mensalMap[m.mes] = (mensalMap[m.mes] || 0) + m.valor; });
        b.mensal.forEach(m => { mensalMap[m.mes] = (mensalMap[m.mes] || 0) + m.valor; });
        const mensal = Object.entries(mensalMap)
            .map(([mes, valor]) => ({ mes, valor: Math.round(valor * 100) / 100 }))
            .sort((x, y) => y.mes.localeCompare(x.mes));
        return { total: Math.round((a.total + b.total) * 100) / 100, mensal };
    }

    // ---------- 2b. Process Invoices (Painel CS) ----------
    const seenInvIds = new Set();
    const invoices = [];
    invoiceRows.forEach(r => {
        const invId = r.invId;
        if (invId && !seenInvIds.has(invId)) {
            seenInvIds.add(invId);
            const due = (r.due || '').substring(0, 10);
            invoices.push({
                dominio: r.dominio ? String(r.dominio) : '',
                marca: r.marca || '',
                invId,
                due,
                dueMonth: due.substring(0, 7),
                status: r.status || '',
                total: r.valor || 0,
                plan: r.plan || '',
            });
        }
    });
    console.log('  Unique invoices: ' + invoices.length);

    // Group by dominio + by marca (for fallback matching)
    const invoicesByDominio = {};
    const invoicesByMarca = {};
    invoices.forEach(i => {
        function addTo(map, key) {
            if (!key) return;
            if (!map[key]) map[key] = { plan: '', invoices: [], paid: 0, pending: 0, expired: 0, canceled: 0, totalInvoices: 0 };
            const b = map[key];
            if (i.plan && !b.plan) b.plan = i.plan;
            b.totalInvoices++;
            b.invoices.push({ mes: i.dueMonth, status: i.status, total: i.total, due: i.due });
            if (i.status === 'paid' || i.status === 'externally_paid') b.paid += i.total;
            else if (i.status === 'pending') b.pending += i.total;
            else if (i.status === 'expired') b.expired += i.total;
            else if (i.status === 'canceled') b.canceled += i.total;
        }
        addTo(invoicesByDominio, i.dominio);
        addTo(invoicesByMarca, normalize(i.marca));
    });
    console.log('  Invoice domains: ' + Object.keys(invoicesByDominio).length + ' | brands: ' + Object.keys(invoicesByMarca).length);

    // ---------- 3. Fetch HubSpot Oráculo tickets ----------
    console.log('\nFetching HubSpot...');
    const oraculoTickets = await fetchOraculoTickets();

    // ---------- 3b. Fetch Oráculo Fabric data (painel stats + configurations) ----------
    console.log('\nFetching Oráculo Fabric data...');
    const [oraculoPainelStats, oraculoConfigMap] = await Promise.all([
        fetchOraculoPainelStats(accessToken),
        fetchOraculoConfigurations(accessToken),
    ]);

    // ---------- 4. Read Controle Geral Luana CSV (apenas mensalidade, email, senha, etapaHub) ----------
    console.log('\nReading Controle Geral Luana (campos selecionados)...');
    const controleMap = {};
    const controleByNome = {};
    await readCSV('controle_geral_luana_csv.csv', (row) => {
        const companyId = row['Company*ID'] || row['CompanyID'] || '';
        const marca = row['MARCAS'] || '';
        const entry = {
            usuario: row['Usuário'] || row['Usuario'] || '',
            senha: row['Senha'] || '',
            etapaHub: row['ETAPA HUB'] || '',
            mensalidade: row['MENSALIDADE'] || '',
        };
        if (companyId) controleMap[companyId] = entry;
        if (marca) controleByNome[marca.toLowerCase().trim()] = entry;
    });
    console.log('  Controle Luana loaded: ' + Object.keys(controleMap).length + ' companies (email, senha, etapaHub, mensalidade)');

    // ---------- 4b. Load CSAT data from _csat.json ----------
    const csatByEmpresa = {};
    const csatPath = path.join(DIR, '_csat.json');
    if (fs.existsSync(csatPath)) {
        try {
            const csatData = JSON.parse(fs.readFileSync(csatPath, 'utf-8'));
            csatData.forEach(c => {
                const key = (c.empresa || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
                if (!csatByEmpresa[key]) csatByEmpresa[key] = [];
                csatByEmpresa[key].push({ mes: c.mes, nota: c.nota, obs: c.obs || '' });
            });
            console.log('  CSAT loaded: ' + csatData.length + ' entries for ' + Object.keys(csatByEmpresa).length + ' empresas');
        } catch (e) { console.log('  WARN: Failed to read _csat.json: ' + e.message); }
    } else {
        console.log('  SKIP: _csat.json not found');
    }

    // ---------- 4c. Load NPS data from _nps.json ----------
    const npsMap = {};
    const npsPath = path.join(DIR, '_nps.json');
    if (fs.existsSync(npsPath)) {
        try {
            const npsData = JSON.parse(fs.readFileSync(npsPath, 'utf-8'));
            npsData.forEach(n => { if (n.dominio) npsMap[String(n.dominio)] = n.nps; });
            console.log('  NPS loaded: ' + npsData.length + ' entries');
        } catch (e) { console.log('  WARN: Failed to read _nps.json: ' + e.message); }
    } else {
        console.log('  SKIP: _nps.json not found');
    }

    // ---------- 5. Process Power BI data ----------
    console.log('\nProcessing data...');

    // 5a. Cadastros Empresas - build empresa map
    const empresasMap = {};
    const empresasByDominio = {};
    for (const row of cadastrosRows) {
        const id = row['Id Empresa'];
        if (!id) continue;
        empresasMap[id] = {
            id,
            cnpj: row['CNPJ'] || '',
            anjo: row['Anjo'] || '',
            integracao: row['Integração'] || row['Integracao'] || '',
            tags: row['Tags'] || '',
            temIntegracao: row['Tem Integração?'] || '',
            idDominio: row['Id Dominio'] || '',
            nomeDominio: row['Nome do Dominio'] || '',
            nomeFantasia: row['Nome Fantasia'] || '',
            razaoSocial: row['Razao Social'] || '',
            canal: row['Canal de Vendas'] || '',
            modulo: row['Modulo'] || '',
            tipoAtacado: row['Tipo Atacado  Varejo'] || '',
            criacao: row['Criação do Dominio'] || row['Criacao do Dominio'] || '',
            tipoIntegracao: row['Domains.integration_type'] || '',
            dataPrimeiroPedido: row['Data do Primeiro Pedido VESTIPAGO'] || '',
            valorPlano: parseFloat(row['Valor Cobrado Plano']) || 0,
            // Aggregated fields - populated from DAX results
            transCartao: 0, transPix: 0, transTotal: 0,
            valCartao: 0, valPix: 0, valTotal: 0,
            pedidos: 0, pedidosPagos: 0, pedidosCancelados: 0, pedidosPendentes: 0,
            valPedidosPagos: 0, valPedidosCancelados: 0, valPedidosPendentes: 0,
            linksEnviados: 0, cliques: 0,
            cartaoImpl: false, pixImpl: false,
        };
        if (row['Id Dominio']) {
            empresasByDominio[row['Id Dominio']] = empresasMap[id];
        }
    }
    console.log('  Companies loaded: ' + Object.keys(empresasMap).length);

    // 5b. Config Empresas - card/pix flags
    for (const row of configRows) {
        const companyId = row['docs.companyId'];
        if (companyId && empresasMap[companyId]) {
            empresasMap[companyId].cartaoImpl = (row['docs.creditCard.isEnabled'] === true || row['docs.creditCard.isEnabled'] === 'True' || row['docs.creditCard.isEnabled'] === 'true');
            empresasMap[companyId].pixImpl = (row['docs.pix.isEnabled'] === true || row['docs.pix.isEnabled'] === 'True' || row['docs.pix.isEnabled'] === 'true');
        }
    }

    // 5c. Pedidos per company (from DAX aggregated query)
    // Cap: ticket médio máximo razoável = R$ 500.000 por pedido (acima disso é dado corrompido)
    const MAX_TICKET = 50000;
    for (const row of pedidosCompanyRows) {
        const empresaId = row['ID Empresa'];
        const emp = empresasMap[empresaId];
        if (!emp) continue;

        emp.pedidos = parseInt(row['TotalPedidos']) || 0;
        emp.pedidosPagos = parseInt(row['TotalPagos']) || 0;
        emp.pedidosCancelados = parseInt(row['TotalCancelados']) || 0;
        emp.pedidosPendentes = parseInt(row['TotalPendentes']) || 0;
        emp.valTotal = parseFloat(row['ValTotal']) || 0;
        emp.valPedidosPagos = parseFloat(row['ValPagos']) || 0;
        emp.valPedidosCancelados = parseFloat(row['ValCancelados']) || 0;

        // Filtrar outliers: se ticket médio > MAX_TICKET, zerar valores (dado corrompido)
        if (emp.pedidos > 0 && emp.valTotal / emp.pedidos > MAX_TICKET) {
            console.log('  WARN: Outlier detectado - ' + (emp.nomeFantasia || emp.nomeDominio) + ' (ticket médio R$ ' + Math.round(emp.valTotal / emp.pedidos) + ')');
            emp.valTotal = 0;
            emp.valPedidosPagos = 0;
            emp.valPedidosCancelados = 0;
        }
        emp.transCartao = parseInt(row['TransCartao']) || 0;
        emp.transPix = parseInt(row['TransPix']) || 0;
        emp.transTotal = emp.pedidos;
        emp.valCartao = parseFloat(row['ValCartao']) || 0;
        emp.valPix = parseFloat(row['ValPix']) || 0;
        emp.valPedidosPendentes = emp.valTotal - emp.valPedidosPagos - emp.valPedidosCancelados;
    }

    // 5d. Product - links per company
    for (const row of productRows) {
        const companyId = row['Cadastros Users ( Vendedores ).CompanyId'];
        if (companyId && empresasMap[companyId]) {
            empresasMap[companyId].linksEnviados = parseInt(row['LinksEnviados']) || 0;
        }
    }

    // 5e. Rankings - cliques per company
    for (const row of rankingsRows) {
        const companyId = row['Cadastros Users ( Vendedores ).CompanyId'];
        if (companyId && empresasMap[companyId]) {
            empresasMap[companyId].cliques = parseInt(row['Cliques']) || 0;
        }
    }

    // 5f. Marcas e Planos - by CNPJ (all plan breakdown fields)
    // Power BI table only has MARCA, PLANO, CPFCNPJ, TOTAL_COBRADO.
    // Full data (MENSALIDADE, INTEGRAÇÃO, ASSISTENTE, FILIAL, DESCONTOS, etc.) comes from local Excel.
    const marcasMap = {};
    const excelPath = path.join(DIR, 'Marcas e Planos.xlsx');
    let marcasSource = 'PowerBI';
    if (fs.existsSync(excelPath)) {
        try {
            const XLSX = require('xlsx');
            const wb = XLSX.readFile(excelPath);
            function extractSheetDate(name) {
                let m = name.match(/(\d{2})-?(\d{4})/);
                if (m) return m[2] + '-' + m[1];
                m = name.match(/(\d{2})-?(\d{2})$/);
                if (m) return '20' + m[2] + '-' + m[1];
                return '0000-00';
            }
            const vestiSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('vesti') && !s.toLowerCase().includes('starter'));
            const starterSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('starter'));
            vestiSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));
            starterSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));
            // Process Starter first (lower priority), then Vesti (overwrites)
            const sheetsToRead = [];
            if (starterSheets.length > 0) sheetsToRead.push(starterSheets[0]);
            if (vestiSheets.length > 0) sheetsToRead.push(vestiSheets[0]);
            if (sheetsToRead.length === 0) sheetsToRead.push(wb.SheetNames[0]);
            // Collect all lines per CNPJ, then merge (sum numeric fields)
            const allLinesByCnpj = {};
            for (const sheetName of sheetsToRead) {
                const ws = wb.Sheets[sheetName];
                const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
                for (const row of rows) {
                    const cnpj = String(row['CPFCNPJ'] || row['CPF e CNPJ'] || '').replace(/[.\-\/\s]/g, '');
                    if (!cnpj || cnpj.length < 11) continue;
                    if (!allLinesByCnpj[cnpj]) allLinesByCnpj[cnpj] = [];
                    allLinesByCnpj[cnpj].push({
                        marca: row['MARCA'] || '',
                        plano: (row['PLANO'] || '').trim(),
                        setup: parseFloat(row['SETUP']) || 0,
                        mensalidade: parseFloat(row['MENSALIDADE']) || 0,
                        integracao: parseFloat(row['INTEGRAÇÃO'] || row['INTEGRACAO']) || 0,
                        assistente: parseFloat(row['ASSISTENTE']) || 0,
                        filial: parseFloat(row['FILIAL']) || 0,
                        descontos: parseFloat(row['DESCONTOS']) || 0,
                        totalCobrado: parseFloat(row['TOTAL COBRADO'] || row['TOTAL_COBRADO']) || 0,
                        observacoes: row['OBSERVAÇÕES'] || row['OBSERVACOES'] || '',
                        canal: row['CANAL'] || row['CANAL/Agência'] || '',
                        subconta: row['Subconta'] || '',
                    });
                }
                console.log('  Excel sheet "' + sheetName + '": ' + rows.length + ' rows');
            }
            // Keep main plan fields + planos array with all lines
            const isExtra = (p) => /oraculo|oráculo|integração|integracao|pacote/i.test(p);
            for (const [cnpj, lines] of Object.entries(allLinesByCnpj)) {
                const main = lines.find(l => !isExtra(l.plano)) || lines[0];
                const entry = { marca: main.marca, plano: main.plano, setup: main.setup, mensalidade: main.mensalidade, integracao: main.integracao, assistente: main.assistente, filial: main.filial, descontos: main.descontos, totalCobrado: main.totalCobrado, observacoes: main.observacoes, canal: main.canal, subconta: main.subconta };
                if (lines.length > 1) {
                    entry.planos = lines.map(l => ({ plano: l.plano, mensalidade: l.mensalidade, integracao: l.integracao, assistente: l.assistente, filial: l.filial, descontos: l.descontos, totalCobrado: l.totalCobrado, setup: l.setup }));
                }
                marcasMap[cnpj] = entry;
            }
            marcasSource = 'Excel';
            console.log('  Marcas e Planos (Excel merged): ' + Object.keys(marcasMap).length + ' CNPJs');
        } catch (e) {
            console.log('  WARN: Excel read failed: ' + e.message + ', falling back to PowerBI data');
        }
    }
    if (marcasSource === 'PowerBI') {
        // Fallback: use PowerBI data (limited columns)
        for (const row of marcasRows) {
            const cnpj = row['CPFCNPJ'] || '';
            if (cnpj) {
                marcasMap[cnpj] = {
                    marca: row['MARCA'] || '',
                    plano: row['PLANO'] || '',
                    setup: 0,
                    mensalidade: parseFloat(row['MENSALIDADE']) || 0,
                    integracao: parseFloat(row['INTEGRAÇÃO'] || row['INTEGRACAO']) || 0,
                    assistente: parseFloat(row['ASSISTENTE']) || 0,
                    filial: parseFloat(row['FILIAL']) || 0,
                    descontos: parseFloat(row['DESCONTOS']) || 0,
                    totalCobrado: parseFloat(row['TOTAL COBRADO'] || row['TOTAL_COBRADO']) || 0,
                    observacoes: row['OBSERVAÇÕES'] || row['OBSERVACOES'] || '',
                    canal: row['CANAL'] || '',
                    subconta: row['Subconta'] || '',
                };
            }
        }
        console.log('  Marcas e Planos (PowerBI): ' + Object.keys(marcasMap).length + ' CNPJs');
    }

    // 5g. Pedidos per company per month
    const pedidosCompanyMonth = {};
    for (const row of pedidosCompanyMonthlyRows) {
        const empId = row['ID Empresa'];
        const year = row['Year'];
        const month = row['MonthNo'];
        if (!empId || !year || !month) continue;
        const mesKey = String(year) + '-' + String(month).padStart(2, '0');
        if (!pedidosCompanyMonth[empId]) pedidosCompanyMonth[empId] = {};
        const qtd = parseInt(row['Qtd']) || 0;
        const val = parseFloat(row['Val']) || 0;
        // Filtrar outliers mensais
        const valFinal = (qtd > 0 && val / qtd > MAX_TICKET) ? 0 : val;
        const valPagos = (qtd > 0 && val / qtd > MAX_TICKET) ? 0 : (parseFloat(row['ValPagos']) || 0);
        pedidosCompanyMonth[empId][mesKey] = {
            qtd,
            pagos: parseInt(row['Pagos']) || 0,
            cancelados: parseInt(row['Cancelados']) || 0,
            pendentes: parseInt(row['Pendentes']) || 0,
            val: valFinal,
            valPagos,
            tc: parseInt(row['TC']) || 0,
            tp: parseInt(row['TP']) || 0,
            vc: (qtd > 0 && val / qtd > MAX_TICKET) ? 0 : (parseFloat(row['VC']) || 0),
            vp: (qtd > 0 && val / qtd > MAX_TICKET) ? 0 : (parseFloat(row['VP']) || 0),
        };
    }
    console.log('  Company monthly data: ' + Object.keys(pedidosCompanyMonth).length + ' companies');

    // Links per company per month
    const linksCompanyMonth = {};
    for (const row of linksCompanyMonthlyRows) {
        const cid = row['Cadastros Users ( Vendedores ).CompanyId'];
        const year = row['Year']; const month = row['MonthNo'];
        if (!cid || !year || !month) continue;
        const mk = String(year) + '-' + String(month).padStart(2, '0');
        if (!linksCompanyMonth[cid]) linksCompanyMonth[cid] = {};
        linksCompanyMonth[cid][mk] = parseInt(row['Links']) || 0;
    }

    // Cliques per company per month
    const cliquesCompanyMonth = {};
    for (const row of cliquesCompanyMonthlyRows) {
        const cid = row['Cadastros Users ( Vendedores ).CompanyId'];
        const year = row['Year']; const month = row['MonthNo'];
        if (!cid || !year || !month) continue;
        const mk = String(year) + '-' + String(month).padStart(2, '0');
        if (!cliquesCompanyMonth[cid]) cliquesCompanyMonth[cid] = {};
        cliquesCompanyMonth[cid][mk] = parseInt(row['Cliques']) || 0;
    }

    // ---------- 6. HubSpot fuzzy matching ----------
    console.log('\nMatching Oráculo tickets...');
    const allEmpresas = Object.values(empresasMap).filter(e => e.nomeFantasia || e.nomeDominio);
    const { empLookup, empWords } = buildMatchStructures(allEmpresas);

    const oraculoByEmpId = {};
    let oraculoMatched = 0, oraculoUnmatched = 0;
    for (const t of oraculoTickets) {
        const emp = matchTicketToEmpresa(t, empLookup, empWords);
        if (emp) {
            oraculoMatched++;
            if (!oraculoByEmpId[emp.id] || t.modified > oraculoByEmpId[emp.id].modified) {
                oraculoByEmpId[emp.id] = t;
            }
        } else {
            oraculoUnmatched++;
        }
    }
    console.log('  Oráculo matched: ' + oraculoMatched + '/' + oraculoTickets.length + ' (' + oraculoUnmatched + ' unmatched)');

    // ---------- 7. Build monthly global data from DAX ----------
    console.log('\nBuilding monthly data...');
    const pedidosMensais = {};
    for (const row of pedidosMonthlyRows) {
        const year = row['Year'];
        const month = row['MonthNo'];
        if (!year || !month) continue;
        const mesKey = String(year) + '-' + String(month).padStart(2, '0');
        pedidosMensais[mesKey] = {
            cartao: parseInt(row['Cartao']) || 0,
            pix: parseInt(row['Pix']) || 0,
            total: parseInt(row['TotalPedidos']) || 0,
            valCartao: parseFloat(row['ValCartao']) || 0,
            valPix: parseFloat(row['ValPix']) || 0,
            valTotal: parseFloat(row['ValTotal']) || 0,
            pagos: parseInt(row['Pagos']) || 0,
            cancelados: parseInt(row['Cancelados']) || 0,
            pendentes: parseInt(row['Pendentes']) || 0,
            valPagos: parseFloat(row['ValPagos']) || 0,
        };
    }

    const sortedMonths = Object.keys(pedidosMensais).sort();
    const recentMonths = sortedMonths.slice(-18);
    const allMonths = sortedMonths;

    // Build links/cliques monthly maps
    const linksMensais = {};
    for (const row of linksMonthlyRows) {
        const year = row['Year']; const month = row['MonthNo'];
        if (!year || !month) continue;
        const mk = String(year) + '-' + String(month).padStart(2, '0');
        linksMensais[mk] = parseInt(row['Links']) || 0;
    }
    const cliquesMensais = {};
    for (const row of cliquesMonthlyRows) {
        const year = row['Year']; const month = row['MonthNo'];
        if (!year || !month) continue;
        const mk = String(year) + '-' + String(month).padStart(2, '0');
        cliquesMensais[mk] = parseInt(row['Cliques']) || 0;
    }

    const monthlyData = recentMonths.map(m => ({
        mes: m,
        ...pedidosMensais[m],
        links: linksMensais[m] || 0,
        cliques: cliquesMensais[m] || 0,
    }));

    // ---------- 7b. Filial detection: agora vem do lakehouse (ROW_NUMBER PARTITION BY domain_id)
    // Inicializa estruturas vazias — populadas apos o loop empresasAtivas (linha ~1495).
    const filialGroups = {};
    const matrizIds = new Set();
    function ufFind(x) { return x; } // identidade — cada empresa eh seu proprio "root"

    // ---------- 8. Build final empresas list ----------
    console.log('\nBuilding empresas list...');

    // FONTE DE VERDADE: lakehouse retorna 1 linha por (dominio × company) — matrizes + filiais
    // expandidas via ROW_NUMBER. Cada linha vira uma "empresa" no dashboard, com id = c.id
    // (= [Id Empresa] do Cadastros DAX e do Merged Pedidos, ja validado). Para cada lakehouse
    // empresa, usamos a entry do empresasMap (vinda do Cadastros DAX) se existir, senao
    // criamos um stub. Em ambos casos enriquecemos com dados do lakehouse.
    const empresasAtivas = [];
    let addedLakehouse = 0;
    let enrichedFromCadastros = 0;
    for (const lh of lakehouseEmpresas) {
        let emp = empresasMap[lh.id];
        if (!emp) {
            // Cria stub mantendo a mesma shape do empresasMap original
            emp = {
                id: lh.id,
                cnpj: lh.cnpj,
                anjo: lh.anjo,
                integracao: lh.integracao,
                tags: lh.tags,
                temIntegracao: lh.integracao ? 'Sim' : '',
                idDominio: lh.dominioId,
                nomeDominio: lh.nomeDominio,
                nomeFantasia: lh.nomeFantasia,
                razaoSocial: lh.razaoSocial,
                canal: lh.canal,
                modulo: lh.modulos,
                tipoAtacado: lh.tipoAtacado,
                criacao: lh.criacao,
                tipoIntegracao: '',
                dataPrimeiroPedido: '',
                valorPlano: 0,
                statusEmpresa: lh.statusEmpresa || '',
                transCartao: 0, transPix: 0, transTotal: 0,
                valCartao: 0, valPix: 0, valTotal: 0,
                pedidos: 0, pedidosPagos: 0, pedidosCancelados: 0, pedidosPendentes: 0,
                valPedidosPagos: 0, valPedidosCancelados: 0, valPedidosPendentes: 0,
                linksEnviados: 0, cliques: 0,
                cartaoImpl: false, pixImpl: false,
            };
            empresasMap[lh.id] = emp;
            addedLakehouse++;
        } else {
            enrichedFromCadastros++;
            // Cadastros DAX existe — preenche campos vazios com lakehouse mas mantem
            // os dados do Cadastros como prioridade (ele tem mais campos)
            if (!emp.idDominio) emp.idDominio = lh.dominioId;
            if (!emp.cnpj) emp.cnpj = lh.cnpj;
            if (!emp.nomeFantasia) emp.nomeFantasia = lh.nomeFantasia;
            if (!emp.nomeDominio) emp.nomeDominio = lh.nomeDominio;
            if (!emp.razaoSocial) emp.razaoSocial = lh.razaoSocial;
            if (!emp.canal) emp.canal = lh.canal;
            if (!emp.anjo) emp.anjo = lh.anjo;
            if (!emp.integracao) emp.integracao = lh.integracao;
        }
        // Lakehouse Q1 statusEmpresa eh sempre prioritario (mais atualizado que DAX Metricas)
        if (lh.statusEmpresa) emp.statusEmpresa = lh.statusEmpresa;
        // Marcadores de matriz/filial vindos do ROW_NUMBER do lakehouse
        emp.isMatriz = lh.isMatriz;
        emp.isFilial = lh.isFilial;
        emp.lakehouseRn = lh.rn;
        empresasAtivas.push(emp);
    }
    console.log('  Empresas do lakehouse: ' + empresasAtivas.length +
        ' (' + enrichedFromCadastros + ' enriched do Cadastros DAX, ' +
        addedLakehouse + ' stubs novos)');

    // Popula filialGroups/matrizIds usando o ROW_NUMBER do lakehouse: empresas com mesmo
    // dominioId formam grupo, rn=1 (mais antiga em ODBC_Companies) eh a matriz.
    empresasAtivas.forEach(e => {
        if (!e.idDominio) return;
        const key = String(e.idDominio);
        if (!filialGroups[key]) filialGroups[key] = [];
        filialGroups[key].push(e);
        if (e.isMatriz) matrizIds.add(e.id);
    });
    // Para cada grupo, garante que tem pelo menos 1 matriz (caso o lakehouse nao tenha marcado)
    Object.values(filialGroups).forEach(group => {
        if (!group.some(e => matrizIds.has(e.id))) {
            // Fallback: marca a primeira como matriz
            const sorted = [...group].sort((a, b) => (a.lakehouseRn || 999) - (b.lakehouseRn || 999));
            if (sorted[0]) matrizIds.add(sorted[0].id);
        }
    });
    const groupsWithFiliais = Object.values(filialGroups).filter(g => g.length > 1);
    console.log('  Filial groups (lakehouse): ' + groupsWithFiliais.length +
        ' grupos com filial, ' + groupsWithFiliais.reduce((s, g) => s + g.length, 0) + ' empresas total nos grupos');

    // Excel Marcas e Planos NAO eh mais usado como fonte de empresas — apenas pra
    // enriquecimento de plano/mensalidade no map abaixo (marcasMap[cnpjNum]). A fonte
    // unica de marcas ativas eh ODBC_Domains do lakehouse.
    const addedExcel = 0;
    console.log('  Empresas ativas: ' + empresasAtivas.length + ' (fonte unica: lakehouse ODBC_Domains)');

    let empIndex = 0;
    let empresasList = empresasAtivas
        .map(e => {
            const cnpjNum = (e.cnpj || '').replace(/[.\-\/]/g, '');
            let marca = marcasMap[cnpjNum];
            // Fallback: match by CNPJ root (first 8 digits)
            if (!marca && cnpjNum.length >= 8) {
                for (const [mcnpj, mdata] of Object.entries(marcasMap)) {
                    if (mcnpj.substring(0, 8) === cnpjNum.substring(0, 8)) { marca = mdata; break; }
                }
            }
            // Fallback: match by name
            if (!marca) {
                const nomeEmp = (e.nomeFantasia || e.nomeDominio || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
                for (const [, mdata] of Object.entries(marcasMap)) {
                    const nMarca = (mdata.marca || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
                    if (nMarca && nomeEmp && nMarca === nomeEmp) { marca = mdata; break; }
                }
                if (!marca && nomeEmp.length >= 5) {
                    for (const [, mdata] of Object.entries(marcasMap)) {
                        const nMarca = (mdata.marca || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
                        if (nMarca.length >= 5 && (nomeEmp.includes(nMarca) || nMarca.includes(nomeEmp))) { marca = mdata; break; }
                    }
                }
            }
            const idx = empIndex++;

            const nome = e.nomeFantasia || e.nomeDominio;
            const ctrl = controleMap[e.id] || controleByNome[(nome || '').toLowerCase().trim()];
            const oracTkt = oraculoByEmpId[e.id];

            // Mensalidade (CSV > Marcas e Planos > valorPlano)
            let mensalidade = '';
            if (ctrl && ctrl.mensalidade) {
                mensalidade = ctrl.mensalidade;
            } else if (marca && marca.totalCobrado) {
                mensalidade = 'R$ ' + marca.totalCobrado.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            } else if (e.valorPlano > 0) {
                mensalidade = 'R$ ' + e.valorPlano.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            }

            // Etapa Hub (do CSV)
            const etapaHub = ctrl ? ctrl.etapaHub : '';

            // Oráculo etapa (only from HubSpot)
            const oraculoEtapa = oracTkt ? oracTkt.stageName : '';

            // Per-company monthly data
            const empMonthly = pedidosCompanyMonth[e.id] || {};
            const empLinks = linksCompanyMonth[e.id] || {};
            const empCliques = cliquesCompanyMonth[e.id] || {};

            // Collect all months with any data
            const allEmpMonths = new Set([...Object.keys(empMonthly), ...Object.keys(empLinks), ...Object.keys(empCliques)]);
            const empMonthKeys = [...allEmpMonths].sort();

            // Build m (sparse monthly data per company)
            const m = {};
            for (const mk of empMonthKeys) {
                const md = empMonthly[mk];
                const lk = empLinks[mk] || 0;
                const ck = empCliques[mk] || 0;
                if (md || lk || ck) {
                    m[mk] = [
                        md ? md.qtd : 0, md ? md.pagos : 0, md ? md.cancelados : 0, md ? md.pendentes : 0,
                        md ? Math.round(md.val * 100) / 100 : 0,
                        md ? Math.round(md.valPagos * 100) / 100 : 0,
                        0, // valCancelados (not available per month in cloud build)
                        md ? Math.round((md.val - md.valPagos) * 100) / 100 : 0,
                        md ? md.tc : 0, md ? md.tp : 0,
                        md ? Math.round(md.vc * 100) / 100 : 0,
                        md ? Math.round(md.vp * 100) / 100 : 0,
                        lk, ck,
                    ];
                }
            }

            // Churn prediction using real monthly data
            let churnScore = 0;
            let churnMotivos = [];

            // Get last 6 months of data for trend analysis
            const now = new Date();
            const recentMonthKeys = [];
            for (let i = 0; i < 6; i++) {
                const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
                recentMonthKeys.push(d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0'));
            }

            const last3 = recentMonthKeys.slice(0, 3).reduce((s, k) => s + (empMonthly[k] ? empMonthly[k].qtd : 0), 0);
            const prev3 = recentMonthKeys.slice(3, 6).reduce((s, k) => s + (empMonthly[k] ? empMonthly[k].qtd : 0), 0);

            // 1. Order drop >50% (last 3 months vs prior 3 months)
            if (prev3 > 5 && last3 < prev3 * 0.5) {
                churnScore += 30;
                churnMotivos.push('Queda >50% nos pedidos');
            }
            // 2. Order drop >30%
            else if (prev3 > 5 && last3 < prev3 * 0.7) {
                churnScore += 15;
                churnMotivos.push('Queda >30% nos pedidos');
            }
            // 3. Zero orders in current month
            const currentMonth = recentMonthKeys[0];
            if (e.pedidos > 0 && (!empMonthly[currentMonth] || empMonthly[currentMonth].qtd === 0)) {
                churnScore += 25;
                churnMotivos.push('Zero pedidos no mês atual');
            }
            // 4. High cancellation rate
            if (e.pedidos > 10 && e.pedidosCancelados > e.pedidosPagos * 0.3) {
                churnScore += 15;
                churnMotivos.push('Alto cancelamento');
            }
            // 5. No integration
            if (e.temIntegracao !== 'Sim') {
                churnScore += 10;
                churnMotivos.push('Sem integração');
            }
            // 6. Oráculo status
            if (oraculoEtapa === 'Churn') {
                churnScore += 30;
                churnMotivos.push('Oráculo: Churn');
            } else if (oraculoEtapa === 'Parado') {
                churnScore += 20;
                churnMotivos.push('Oráculo: Parado');
            }

            churnScore = Math.min(churnScore, 100);
            const churnRisco = churnScore >= 60 ? 'Alto' : churnScore >= 30 ? 'Médio' : 'Baixo';

            const temVestiPago = vestiPagoSet.has(e.id);

            // Oráculo Fabric (configurations + painel stats)
            const oraculoConfig = oraculoConfigMap.get(e.id) || null;
            let oraculoStats = oraculoPainelStats.get(nome.toLowerCase()) || null;
            if (!oraculoStats && oraculoConfig) {
                const ocName = (oraculoConfig.name || '').toLowerCase().replace(/^churn\s*-\s*/i, '').replace(/^chrun\s*-\s*/i, '').trim();
                if (ocName) oraculoStats = oraculoPainelStats.get(ocName) || null;
                if (!oraculoStats) {
                    const nNorm = normalize(nome);
                    for (const [pName, pStats] of oraculoPainelStats) {
                        const pNorm = normalize(pName);
                        if (pNorm && nNorm && (nNorm.includes(pNorm) || pNorm.includes(nNorm))) {
                            oraculoStats = pStats; break;
                        }
                    }
                }
            }

            // Filiais: agrupadas por idDominio (lakehouse), nao mais por UF
            const groupRoot = String(e.idDominio || e.id);
            const filiaisGroup = filialGroups[groupRoot] || [];
            const isMatriz = matrizIds.has(e.id);
            const matrizEmp = filiaisGroup.find(f => matrizIds.has(f.id));
            const matrizId = matrizEmp ? matrizEmp.id : e.id;
            const filiais = filiaisGroup
                .filter(f => f.id !== e.id)
                .map(f => ({
                    nome: f.nomeFantasia || f.nomeDominio,
                    idDominio: f.idDominio,
                    id: f.id,
                    temVestiPago: vestiPagoSet.has(f.id),
                    isMatriz: matrizIds.has(f.id),
                }))
                .sort((a, b) => {
                    if (a.isMatriz && !b.isMatriz) return -1;
                    if (!a.isMatriz && b.isMatriz) return 1;
                    return a.nome.localeCompare(b.nome);
                });

            return {
                i: idx,
                id: e.id,
                idDominio: e.idDominio,
                nome,
                canal: e.canal,
                cartao: e.cartaoImpl ? 'Sim' : 'Não',
                pix: e.pixImpl ? 'Sim' : 'Não',
                cnpj: e.cnpj,
                temVestiPago,
                transCartao: e.transCartao,
                transPix: e.transPix,
                transTotal: e.transTotal,
                valCartao: Math.round(e.valCartao * 100) / 100,
                valPix: Math.round(e.valPix * 100) / 100,
                valTotal: Math.round(e.valTotal * 100) / 100,
                gmv: Math.round(e.valTotal * 100) / 100,
                pedidos: e.pedidos,
                pedidosPagos: e.pedidosPagos,
                pedidosCancelados: e.pedidosCancelados,
                pedidosPendentes: e.pedidosPendentes,
                valPedidosPagos: Math.round(e.valPedidosPagos * 100) / 100,
                valPedidosCancelados: Math.round(e.valPedidosCancelados * 100) / 100,
                valPedidosPendentes: Math.round(e.valPedidosPendentes * 100) / 100,
                linksEnviados: e.linksEnviados,
                cliques: e.cliques,
                anjo: (metricasMap[e.id] && metricasMap[e.id].cs) || e.anjo,
                modulo: e.modulo,
                tags: e.tags,
                temIntegracao: e.temIntegracao,
                integracao: e.integracao || '',
                tipoIntegracao: e.tipoIntegracao,
                criacao: e.criacao,
                valorPlano: e.valorPlano,
                plano: marca ? marca.plano : '',
                planoMensalidade: marca ? marca.mensalidade : 0,
                planoIntegracao: marca ? marca.integracao : 0,
                planoAssistente: marca ? marca.assistente : 0,
                planoFilial: marca ? marca.filial : 0,
                planoDescontos: marca ? marca.descontos : 0,
                planoTotalCobrado: marca ? marca.totalCobrado : 0,
                planoSetup: marca ? marca.setup : 0,
                planoObservacoes: marca ? marca.observacoes : '',
                planoSubconta: marca ? marca.subconta : '',
                planos: marca && marca.planos ? marca.planos : undefined,
                marcaAtiva: (e.statusEmpresa === 'Ativa' || (metricasMap[e.id] && metricasMap[e.id].statusEmpresa === 'Ativa')) ? 'Sim' : (e.statusEmpresa === 'Desativada' || (metricasMap[e.id] && metricasMap[e.id].statusEmpresa === 'Desativada')) ? 'Não' : '',
                mensalidade,
                etapaHub,
                oraculoEtapa,
                temOraculoFabric: !!(oraculoStats || oraculoConfig),
                oraculoFabric: (oraculoStats || oraculoConfig) ? {
                    ...(oraculoConfig || {}),
                    pedidosOraculo: oraculoStats ? oraculoStats.pedidosOraculo : 0,
                    interacoesOraculo: oraculoStats ? oraculoStats.interacoesOraculo : 0,
                    atendimentosOraculo: oraculoStats ? oraculoStats.atendimentosOraculo : 0,
                    pctIAOraculo: oraculoStats ? oraculoStats.pctIAOraculo : 0,
                    vendasOraculo: oraculoStats ? oraculoStats.vendasOraculo : 0,
                    mensal: oraculoStats ? oraculoStats.mensal : undefined,
                } : undefined,
                usuario: ctrl ? ctrl.usuario : '',
                senha: ctrl ? ctrl.senha : '',
                churnScore,
                churnRisco,
                churnMotivos: churnMotivos.length > 0 ? churnMotivos.join('; ') : '',
                // Lakehouse (ODBC_Domains -> Confeccao2025_Query1) tem prioridade sobre o
                // DAX Metricas (CS 2024, incompleto). Usa lakehouse se estiver preenchido.
                statusEmpresa: e.statusEmpresa || (metricasMap[e.id] ? metricasMap[e.id].statusEmpresa : ''),
                controleEstoque: metricasMap[e.id] ? metricasMap[e.id].controleEstoque : '',
                ...(function () {
                    const f = getFreteCombinado(e);
                    return {
                        freteAtivo: !!f,
                        freteTotal: f ? f.total : 0,
                        freteMensal: f ? f.mensal.slice(0, 12) : undefined,
                    };
                })(),
                naoPagos: e.pedidosPendentes,
                csat: (() => {
                    const nk = nome.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
                    if (csatByEmpresa[nk]) return csatByEmpresa[nk];
                    for (const [ck, cv] of Object.entries(csatByEmpresa)) {
                        if (ck.length >= 4 && nk.startsWith(ck)) return cv;
                    }
                })(),
                nps: npsMap[String(e.idDominio)] != null ? npsMap[String(e.idDominio)] : undefined,
                isMatriz: filiaisGroup.length > 1 ? isMatriz : undefined,
                matrizId: filiaisGroup.length > 1 && !isMatriz ? matrizId : undefined,
                filiais: filiais.length > 0 ? filiais : undefined,
                m,
            };
        });

    // ---------- 8b. Match Invoices (Painel CS) to empresas ----------
    let invoiceMatched = 0;
    for (const emp of empresasList) {
        let data = null;
        // 1. Match by idDominio
        if (emp.idDominio) data = invoicesByDominio[String(emp.idDominio)];
        // 2. Fallback: match by nome -> marca
        if (!data) {
            const nomeNorm = normalize(emp.nome);
            data = invoicesByMarca[nomeNorm];
            if (!data) {
                for (const [mk, md] of Object.entries(invoicesByMarca)) {
                    if (mk.length >= 4 && (nomeNorm.startsWith(mk) || mk.startsWith(nomeNorm))) {
                        data = md; break;
                    }
                }
            }
        }
        if (data) {
            emp.faturamento = {
                planoIugu: data.plan,
                totalPago: Math.round(data.paid * 100) / 100,
                totalPendente: Math.round(data.pending * 100) / 100,
                totalVencido: Math.round(data.expired * 100) / 100,
                totalCancelado: Math.round(data.canceled * 100) / 100,
                qtdFaturas: data.totalInvoices,
                faturas: data.invoices.sort((a, b) => b.due.localeCompare(a.due)).slice(0, 12).map(f => ({
                    mes: f.mes,
                    status: f.status,
                    total: f.total,
                })),
            };
            invoiceMatched++;
        }
    }
    console.log('  Invoices matched to empresas: ' + invoiceMatched + '/' + empresasList.length);

    // Validacao cruzada MongoDB_Pedidos_Geral (lakehouse). Nao bloqueia o build.
    await validateAgainstMongoPedidos(sqlToken, empresasList);

    // Filtro de empresas ativas aplicado via ODBC_Domains (modulos LIKE '%vendas%')

    // ---------- 9. Build output ----------
    const oraculoSummary = {};
    for (const t of oraculoTickets) {
        oraculoSummary[t.stageName] = (oraculoSummary[t.stageName] || 0) + 1;
    }

    const churnAlto = empresasList.filter(e => e.churnRisco === 'Alto').length;
    const churnMedio = empresasList.filter(e => e.churnRisco === 'Médio').length;

    const output = {
        empresas: empresasList,
        mensal: monthlyData,
        meses: allMonths,
        totalEmpresas: empresasList.length,
        oraculoSummary,
        oraculoTickets: oraculoTickets.map(t => ({ nome: t.companyName, etapa: t.stageName, criado: t.created, atualizado: t.modified })),
        churnStats: { alto: churnAlto, medio: churnMedio, total: empresasList.length },
        geradoEm: new Date().toISOString(),
    };

    // Sanity check: comparar contra a versão anterior do dados.js. Se as queries DAX
    // falharem parcialmente (Fabric devolvendo subconjunto), os totais despencam mas não
    // chegam a zero — então a checagem precisa ser relativa, não absoluta.
    const totalGMV = empresasList.reduce((s, e) => s + e.gmv, 0);
    const totalPedidos = empresasList.reduce((s, e) => s + e.pedidos, 0);
    const semStatus = empresasList.filter(e => !e.statusEmpresa).length;
    const pctSemStatus = empresasList.length > 0 ? semStatus / empresasList.length : 0;

    function abort(motivo) {
        console.error('\n*** ABORTING BUILD: ' + motivo + ' ***');
        console.error('*** dados.js NÃO foi sobrescrito — versão anterior preservada. ***');
        process.exit(1);
    }

    if (totalPedidos === 0 && monthlyData.length === 0 && empresasList.length > 0) {
        abort('queries DAX retornaram sem dados (0 pedidos, 0 meses)');
    }
    // Threshold era 20%, mas o dataset CS 2024 (usado desde 3e3b89f) nao cobre todas as
    // empresas novas — cenario normal ficou em ~35-40% sem status. So aborta se for
    // realmente catastrofico (>60%, dataset devolveu quase nada).
    if (pctSemStatus > 0.60) {
        abort('mais de 60% das empresas vieram sem statusEmpresa (' + (pctSemStatus * 100).toFixed(1) + '% — provável falha do dataset CS)');
    }
    if (pctSemStatus > 0.20) {
        console.warn('  AVISO: ' + (pctSemStatus * 100).toFixed(1) + '% das empresas sem statusEmpresa (dataset CS 2024 nao cobre empresas novas — esperado)');
    }

    // Comparar com versão anterior do dados.js (se existir) para detectar regressão grande.
    try {
        const prevPath = path.join(DIR, 'dados.js');
        if (fs.existsSync(prevPath)) {
            const prevTxt = fs.readFileSync(prevPath, 'utf-8');
            const prev = JSON.parse(prevTxt.replace(/^const DADOS\s*=\s*/, '').replace(/;\s*$/, ''));
            const prevEmp = (prev.empresas || []).length;
            const prevGMV = (prev.empresas || []).reduce((s, e) => s + (e.gmv || 0), 0);
            const prevPed = (prev.empresas || []).reduce((s, e) => s + (e.pedidos || 0), 0);

            // Thresholds relaxados: 10%->20% empresas, 25%->50% GMV/Pedidos.
            // Os baselines anteriores foram calibrados contra um dados.js restaurado
            // manualmente (06/04) com totais inflados, disparando falso positivo.
            if (prevEmp > 0 && empresasList.length < prevEmp * 0.80) {
                abort('queda de empresas: ' + empresasList.length + ' vs ' + prevEmp + ' anterior (>20%)');
            }
            if (prevGMV > 0 && totalGMV < prevGMV * 0.50) {
                abort('queda de GMV: ' + totalGMV.toFixed(0) + ' vs ' + prevGMV.toFixed(0) + ' anterior (>50%)');
            }
            if (prevPed > 0 && totalPedidos < prevPed * 0.50) {
                abort('queda de Pedidos: ' + totalPedidos + ' vs ' + prevPed + ' anterior (>50%)');
            }
        }
    } catch (err) {
        console.warn('AVISO: nao foi possivel comparar com dados.js anterior:', err.message);
    }

    const jsonStr = JSON.stringify(output);
    const jsContent = 'const DADOS = ' + jsonStr + ';';
    fs.writeFileSync(path.join(DIR, 'dados.js'), jsContent, 'utf-8');

    // ---------- 10. Print summary ----------
    console.log('\n=== RESULT ===');
    console.log('Empresas: ' + empresasList.length);
    console.log('Meses (global): ' + monthlyData.length);
    console.log('Output: dados.js (' + (jsContent.length / 1024).toFixed(0) + ' KB)');

    const totalLinks = empresasList.reduce((s, e) => s + e.linksEnviados, 0);
    console.log('Total GMV: R$ ' + totalGMV.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
    console.log('Total Pedidos: ' + totalPedidos.toLocaleString('pt-BR'));
    console.log('Total Links: ' + totalLinks.toLocaleString('pt-BR'));
    console.log('Churn: ' + churnAlto + ' alto, ' + churnMedio + ' médio');
    console.log('Oráculo: ' + oraculoTickets.length + ' tickets (' + oraculoMatched + ' matched)');
    console.log('\nDone.');
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
