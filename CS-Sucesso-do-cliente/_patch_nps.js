/**
 * Busca NPS de sheets_pesquisanps do Lakehouse via SQL (TDS) endpoint
 * e injeta no dados.js como fallback quando não tem CSAT.
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const qs = require('querystring');
const { Connection, Request } = require('tedious');

const DIR = __dirname;
function loadEnv() {
    const f = fs.readFileSync(path.join(DIR, '.env'), 'utf-8');
    const E = {};
    f.split('\n').forEach(l => { const m = l.match(/^([^#=]+)=(.*)$/); if (m) E[m[1].trim()] = m[2].trim(); });
    return E;
}
function hr(o, b) {
    return new Promise((r, j) => {
        const q = https.request(o, res => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => r({ s: res.statusCode, b: Buffer.concat(c).toString() })); });
        q.on('error', j); if (b) q.write(b); q.end();
    });
}

async function getToken() {
    const ENV = loadEnv();
    // Need database scope for SQL endpoint
    const body = qs.stringify({
        client_id: ENV.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e',
        grant_type: 'refresh_token',
        refresh_token: ENV.FABRIC_REFRESH_TOKEN,
        scope: 'https://database.windows.net//.default offline_access',
    });
    const tr = await hr({
        hostname: 'login.microsoftonline.com',
        path: '/' + ENV.FABRIC_TENANT_ID + '/oauth2/v2.0/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    const td = JSON.parse(tr.b);
    if (td.refresh_token) {
        let env = fs.readFileSync(path.join(DIR, '.env'), 'utf-8');
        env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + td.refresh_token);
        fs.writeFileSync(path.join(DIR, '.env'), env, 'utf-8');
    }
    return td.access_token;
}

function querySQL(token, sql) {
    return new Promise((resolve, reject) => {
        const rows = [];
        const config = {
            server: 'x6eps4ew6epu3ioiyo6gy2yy3m-r3fd3f7tyheupkfapsl5a7vgxm.datawarehouse.fabric.microsoft.com',
            authentication: { type: 'azure-active-directory-access-token', options: { token } },
            options: {
                database: 'Oraculo_configurations',
                encrypt: true,
                port: 1433,
                requestTimeout: 30000,
                trustServerCertificate: false,
            },
        };
        const connection = new Connection(config);
        connection.on('connect', (err) => {
            if (err) { reject(err); return; }
            const request = new Request(sql, (err) => {
                if (err) reject(err);
                else resolve(rows);
                connection.close();
            });
            request.on('row', (columns) => {
                const row = {};
                columns.forEach(c => { row[c.metadata.colName] = c.value; });
                rows.push(row);
            });
            connection.execSql(request);
        });
        connection.on('error', reject);
        connection.connect();
    });
}

async function main() {
    console.log('=== Patch NPS ===\n');

    const token = await getToken();
    if (!token) { console.error('No token'); process.exit(1); }

    // Query NPS from sheets_pesquisanps
    console.log('Querying sheets_pesquisanps...');
    let npsRows;
    try {
        npsRows = await querySQL(token, `
            SELECT
                Dominio,
                (
                    (SUM(CASE WHEN Nota >= 9 THEN 1 ELSE 0 END) - SUM(CASE WHEN Nota <= 6 THEN 1 ELSE 0 END)) * 100.0
                ) / COUNT(*) AS NPS
            FROM sheets_pesquisanps
            GROUP BY Dominio
        `);
    } catch (e) {
        console.error('SQL failed:', e.message);
        // Try alternative: Capivara table
        console.log('Trying Capivara table...');
        try {
            npsRows = await querySQL(token, 'SELECT Dominio, NPS FROM Capivara WHERE NPS IS NOT NULL');
        } catch (e2) {
            console.error('Capivara also failed:', e2.message);
            process.exit(1);
        }
    }
    console.log('NPS rows:', npsRows.length);

    // Build map by dominio
    const npsMap = {};
    npsRows.forEach(r => {
        const dom = String(r.Dominio || r.domain_id || '');
        const nps = r.NPS != null ? Math.round(r.NPS * 10) / 10 : null;
        if (dom && nps != null) npsMap[dom] = nps;
    });
    console.log('Domains with NPS:', Object.keys(npsMap).length);

    // Load dados.js
    console.log('\nCarregando dados.js...');
    const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();

    // Match: always set NPS (even if empresa has CSAT)
    let matched = 0, withCsat = 0;
    for (const e of DADOS.empresas) {
        const dom = String(e.idDominio || '');
        if (npsMap[dom] != null) {
            e.nps = npsMap[dom];
            matched++;
            if (e.csat && e.csat.length > 0) withCsat++;
        }
    }
    console.log('NPS matched:', matched, '| Also have CSAT:', withCsat);

    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(path.join(DIR, 'dados.js'), output, 'utf-8');
    console.log('dados.js salvo');
}
main().catch(e => { console.error('ERRO:', e.message); process.exit(1); });
