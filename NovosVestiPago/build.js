// Painel "Novos VestiPago": clientes cujo PRIMEIRO pedido VestiPago (payment_method
// PIX ou CREDIT_CARD) aconteceu a partir de 01/03/2026. Para cada um, valor transacionado
// no periodo (marco ate agora), nome da marca e CS responsavel.
//
// Fonte: lakehouse VestiHouse (dbo.MongoDB_Pedidos_Geral + ODBC_Domains + ODBC_Companies
//        + Confeccao2025_Query1 pra anjo/CS).
// Output: dados.js (const DADOS = {...}).

const { Connection, Request } = require('tedious');
const https = require('https');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

const DIR = __dirname;
const SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com';
const SQL_DATABASE = 'VestiHouse';
const START_DATE = '2026-03-01'; // inicio da janela "novos VestiPago"

// ----- env loading (local .env OR CI env vars) -----
const ENV = {};
const envPath = path.join(DIR, '.env');
if (fs.existsSync(envPath)) {
    fs.readFileSync(envPath, 'utf-8').split('\n').forEach(l => {
        const m = l.match(/^([^#=]+)=(.*)$/); if (m) ENV[m[1].trim()] = m[2].trim();
    });
}
['FABRIC_TENANT_ID', 'FABRIC_REFRESH_TOKEN', 'FABRIC_CLIENT_ID'].forEach(k => {
    if (!ENV[k] && process.env[k]) ENV[k] = process.env[k];
});
if (!ENV.FABRIC_TENANT_ID || !ENV.FABRIC_REFRESH_TOKEN) {
    console.error('ERRO: FABRIC_TENANT_ID e FABRIC_REFRESH_TOKEN sao obrigatorios (.env ou env vars).');
    process.exit(1);
}

// ----- HTTP helper -----
function httpsRequest(opts, body) {
    return new Promise((resolve, reject) => {
        const r = https.request(opts, res => {
            const c = [];
            res.on('data', d => c.push(d));
            res.on('end', () => resolve({ statusCode: res.statusCode, body: Buffer.concat(c).toString() }));
        });
        r.on('error', reject);
        if (body) r.write(body);
        r.end();
    });
}

async function getSqlToken() {
    console.log('Auth SQL (Fabric lakehouse)...');
    const body = querystring.stringify({
        // azure-cli client_id — precisa pra escopo database.windows.net
        client_id: '04b07795-8ddb-461a-bbee-02f9e1bf7b46',
        grant_type: 'refresh_token',
        refresh_token: ENV.FABRIC_REFRESH_TOKEN,
        scope: 'https://database.windows.net//.default offline_access',
    });
    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: `/${ENV.FABRIC_TENANT_ID}/oauth2/v2.0/token`,
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    const data = JSON.parse(res.body);
    if (!data.access_token) {
        console.error('Falha token:', res.body.substring(0, 400));
        throw new Error('SQL token request falhou');
    }
    // rotaciona refresh token pra nao expirar
    if (data.refresh_token) {
        fs.writeFileSync(path.join(DIR, '.new_refresh_token'), data.refresh_token, 'utf-8');
        if (fs.existsSync(envPath)) {
            let env = fs.readFileSync(envPath, 'utf-8');
            env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + data.refresh_token);
            fs.writeFileSync(envPath, env, 'utf-8');
        }
    }
    return data.access_token;
}

function runSql(token, sql, label) {
    return new Promise((resolve, reject) => {
        console.log(`  SQL: ${label}...`);
        const rows = [];
        const conn = new Connection({
            server: SQL_SERVER,
            authentication: { type: 'azure-active-directory-access-token', options: { token } },
            options: { database: SQL_DATABASE, encrypt: true, port: 1433, connectTimeout: 30000, requestTimeout: 180000 },
        });
        conn.on('connect', err => {
            if (err) { reject(err); return; }
            const req = new Request(sql, (err2) => {
                if (err2) reject(err2);
                else { conn.close(); resolve(rows); }
            });
            req.on('row', cols => {
                const o = {};
                cols.forEach(c => { o[c.metadata.colName] = c.value; });
                rows.push(o);
            });
            conn.execSql(req);
        });
        conn.on('error', reject);
        conn.connect();
    });
}

async function main() {
    const token = await getSqlToken();

    // 1. Empresas NOVAS no VestiPago (primeiro pedido VP >= START_DATE) + valor mes atual
    const qNovos = `
        WITH vp AS (
            SELECT domainId, settings_createdAt_TIMESTAMP, summary_total, payment_method
            FROM dbo.MongoDB_Pedidos_Geral
            WHERE payment_method IN ('PIX','CREDIT_CARD')
              AND domainId IS NOT NULL
              AND TRY_CAST(domainId AS BIGINT) IS NOT NULL
              AND summary_total IS NOT NULL AND summary_total > 0 AND summary_total < 50000
              AND settings_createdAt_TIMESTAMP IS NOT NULL
        ),
        first_vp AS (
            SELECT domainId, MIN(settings_createdAt_TIMESTAMP) AS first_at
            FROM vp
            GROUP BY domainId
        ),
        vp_periodo AS (
            SELECT domainId,
                   SUM(CASE WHEN payment_method = 'PIX' THEN summary_total ELSE 0 END) AS val_pix,
                   SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN summary_total ELSE 0 END) AS val_cartao,
                   SUM(summary_total) AS val_total,
                   SUM(CASE WHEN payment_method = 'PIX' THEN 1 ELSE 0 END) AS qt_pix,
                   SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN 1 ELSE 0 END) AS qt_cartao,
                   COUNT(*) AS qt_total
            FROM vp
            WHERE settings_createdAt_TIMESTAMP >= '${START_DATE}'
            GROUP BY domainId
        )
        SELECT fv.domainId,
               fv.first_at,
               vp.val_pix,
               vp.val_cartao,
               vp.val_total,
               vp.qt_pix,
               vp.qt_cartao,
               vp.qt_total
        FROM first_vp fv
        LEFT JOIN vp_periodo vp ON vp.domainId = fv.domainId
        WHERE fv.first_at >= '${START_DATE}'
        ORDER BY vp.val_total DESC
    `;
    const novos = await runSql(token, qNovos, 'Novos VestiPago (first_at >= ' + START_DATE + ')');
    console.log(`  ${novos.length} empresas com primeiro pedido VP a partir de ${START_DATE}`);

    // 2. Empresas do lakehouse (nome marca + anjo/CS) - so matriz (rn=1)
    const qEmpresas = `
        WITH ranked AS (
            SELECT c.id, c.domain_id, c.tax_document, c.social_name, c.company_name,
                   c.scheme_url,
                   ROW_NUMBER() OVER (PARTITION BY c.domain_id ORDER BY c.created_at ASC) AS rn
            FROM dbo.ODBC_Companies c
        )
        SELECT rc.domain_id AS dominio_id,
               rc.id AS empresa_id,
               rc.company_name,
               rc.tax_document AS cnpj,
               d.name AS domain_name,
               q.[Nome Fantasia] AS nome_fantasia,
               q.[Canal de Vendas] AS canal,
               q.Anjo AS anjo,
               q.[Status Empresa] AS status_empresa
        FROM ranked rc
        INNER JOIN dbo.ODBC_Domains d ON d.id = rc.domain_id
        LEFT JOIN dbo.Confeccao2025_Query1 q ON q.[Id Empresa] = rc.id
        WHERE rc.rn = 1
    `;
    const empresas = await runSql(token, qEmpresas, 'Empresas (matriz) do lakehouse');
    console.log(`  ${empresas.length} matrizes no lakehouse`);

    // Index empresas por domainId (string, consistente com novos.domainId)
    const empByDominio = new Map();
    for (const e of empresas) {
        const k = String(e.dominio_id);
        empByDominio.set(k, {
            dominioId: k,
            empresaId: e.empresa_id,
            cnpj: e.cnpj || '',
            // nomeFantasia priority: Query1 > domain_name > company_name
            marca: e.nome_fantasia || e.domain_name || e.company_name || '',
            canal: e.canal || '',
            cs: e.anjo || '',
            statusEmpresa: e.status_empresa === 1 ? 'Ativa' : e.status_empresa === 2 ? 'Desativada' : '',
        });
    }

    // Join
    const clientes = [];
    let semMatch = 0;
    for (const n of novos) {
        const k = String(n.domainId);
        const emp = empByDominio.get(k);
        if (!emp) { semMatch++; continue; }
        clientes.push({
            dominioId: k,
            empresaId: emp.empresaId,
            marca: emp.marca,
            cs: emp.cs || '(sem CS)',
            canal: emp.canal,
            cnpj: emp.cnpj,
            statusEmpresa: emp.statusEmpresa,
            primeiroPedidoVp: n.first_at ? (n.first_at.toISOString ? n.first_at.toISOString().substring(0, 10) : String(n.first_at).substring(0, 10)) : '',
            valPix: Math.round((Number(n.val_pix) || 0) * 100) / 100,
            valCartao: Math.round((Number(n.val_cartao) || 0) * 100) / 100,
            valTotal: Math.round((Number(n.val_total) || 0) * 100) / 100,
            qtPix: Number(n.qt_pix) || 0,
            qtCartao: Number(n.qt_cartao) || 0,
            qtTotal: Number(n.qt_total) || 0,
        });
    }
    console.log(`  Joined: ${clientes.length} com match, ${semMatch} domain_id sem empresa no lakehouse`);
    clientes.sort((a, b) => b.valTotal - a.valTotal);

    // Lista unica de CS pra dropdown
    const csSet = new Set(clientes.map(c => c.cs).filter(Boolean));
    const csList = [...csSet].sort((a, b) => a.localeCompare(b, 'pt-BR'));

    const totalPix = clientes.reduce((s, c) => s + c.valPix, 0);
    const totalCartao = clientes.reduce((s, c) => s + c.valCartao, 0);
    const totalGeral = clientes.reduce((s, c) => s + c.valTotal, 0);
    const totalPedidos = clientes.reduce((s, c) => s + c.qtTotal, 0);

    const output = {
        inicio: START_DATE,
        geradoEm: new Date().toISOString(),
        clientes,
        csList,
        resumo: {
            nClientes: clientes.length,
            totalValor: Math.round(totalGeral * 100) / 100,
            totalPix: Math.round(totalPix * 100) / 100,
            totalCartao: Math.round(totalCartao * 100) / 100,
            totalPedidos,
        },
    };

    const outPath = path.join(DIR, 'dados.js');
    fs.writeFileSync(outPath, 'window.DADOS = ' + JSON.stringify(output) + ';\n', 'utf-8');
    console.log(`\nOK: dados.js escrito (${clientes.length} clientes, R$ ${totalGeral.toLocaleString('pt-BR', { minimumFractionDigits: 2 })})`);
}

main().catch(e => { console.error('ERRO:', e.message); console.error(e.stack); process.exit(1); });
