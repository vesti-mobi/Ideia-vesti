/**
 * Busca dados de Frete do dataset "Relatorio Confeccoes - Agencia" e injeta no dados.js.
 * - freteAtivo: true/false (empresa tem pedidos com frete > 0)
 * - freteMensal: [{mes, valor}] (totais de frete por mês)
 * - freteTotal: valor total de frete
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const qs = require('querystring');
const DIR = __dirname;

function loadEnv() {
    const envFile = fs.readFileSync(path.join(DIR, '.env'), 'utf-8');
    const ENV = {};
    envFile.split('\n').forEach(l => { const m = l.match(/^([^#=]+)=(.*)$/); if (m) ENV[m[1].trim()] = m[2].trim(); });
    return ENV;
}

function hr(o, b) {
    return new Promise((r, j) => {
        const q = https.request(o, res => {
            const c = [];
            res.on('data', d => c.push(d));
            res.on('end', () => r({ s: res.statusCode, b: Buffer.concat(c).toString() }));
        });
        q.on('error', j);
        if (b) q.write(b);
        q.end();
    });
}

function normalize(s) { return (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim(); }

async function main() {
    console.log('=== Patch Frete ===\n');

    // 1. Get token
    const ENV = loadEnv();
    const body = qs.stringify({
        client_id: ENV.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e',
        grant_type: 'refresh_token',
        refresh_token: ENV.FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default',
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
    const token = td.access_token;
    if (!token) { console.error('No token:', td.error_description); process.exit(1); }

    // 2. Fetch freight per company per month
    const WS = '0f5bd202-471f-482d-bf3d-38295044d7db';
    const DS = '92a0cf18-2bfd-4b02-873f-615df3ce2d7f';

    console.log('Buscando frete por empresa/mês...');
    const dax = "EVALUATE FILTER(SUMMARIZECOLUMNS(Merged[Companies.company_name], Merged[Recebido].[Year], Merged[Recebido].[MonthNo], \"TotalFrete\", SUM(Merged[Valor Frete])), [TotalFrete] > 0)";
    const qBody = JSON.stringify({ queries: [{ query: dax }], serializerSettings: { includeNulls: true } });
    const qRes = await hr({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + WS + '/datasets/' + DS + '/executeQueries',
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(qBody) },
    }, qBody);
    const qData = JSON.parse(qRes.b);
    if (!qData.results) { console.error('Query failed:', JSON.stringify(qData).substring(0, 500)); process.exit(1); }
    const rows = qData.results[0].tables[0].rows;
    console.log('Rows with freight:', rows.length);

    // 3. Group by company
    const byCompany = {};
    rows.forEach(r => {
        const name = r['Merged[Companies.company_name]'] || '';
        const frete = r['[TotalFrete]'] || 0;
        // Keys have dynamic LocalDateTable names - find year/month by value type
        const keys = Object.keys(r);
        const yearKey = keys.find(k => k.includes('[Year]'));
        const monthKey = keys.find(k => k.includes('[MonthNo]'));
        const year = yearKey ? r[yearKey] : null;
        const month = monthKey ? r[monthKey] : null;
        if (!name || !year || !month) return;
        const mes = year + '-' + String(month).padStart(2, '0');
        const key = normalize(name);
        if (!byCompany[key]) byCompany[key] = { name, mensal: [], total: 0 };
        byCompany[key].mensal.push({ mes, valor: Math.round(frete * 100) / 100 });
        byCompany[key].total += frete;
    });

    // Sort monthly data desc
    for (const c of Object.values(byCompany)) {
        c.mensal.sort((a, b) => b.mes.localeCompare(a.mes));
        c.total = Math.round(c.total * 100) / 100;
    }
    console.log('Companies with freight:', Object.keys(byCompany).length);

    // 4. Load dados.js
    console.log('\nCarregando dados.js...');
    const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();
    console.log('Empresas:', DADOS.empresas.length);

    // 5. Match and patch
    let matched = 0;
    for (const e of DADOS.empresas) {
        const nNorm = normalize(e.nome);
        let data = byCompany[nNorm];
        if (!data) {
            for (const [ck, cv] of Object.entries(byCompany)) {
                if (ck.length >= 4 && (nNorm.startsWith(ck) || ck.startsWith(nNorm))) {
                    data = cv; break;
                }
            }
        }
        if (data) {
            e.freteAtivo = true;
            e.freteTotal = data.total;
            e.freteMensal = data.mensal.slice(0, 12);
            matched++;
            console.log('  MATCH:', e.nome, '-> R$', data.total);
        }
    }
    console.log('\nMatched:', matched + '/' + DADOS.empresas.length);

    // 6. Save
    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(path.join(DIR, 'dados.js'), output, 'utf-8');
    console.log('dados.js salvo (' + (output.length / 1024 / 1024).toFixed(1) + ' MB)');
}
main().catch(e => { console.error('ERRO:', e.message); process.exit(1); });
