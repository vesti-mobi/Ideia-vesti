/**
 * Busca Invoices do dataset "Painel CS" no Fabric e injeta no dados.js.
 * Workspace: 2929476c-7b92-4366-9236-ccd13ffbd917
 * Dataset: 583e34d7-6dd1-467b-86aa-3b74cfe1ca56
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

const DIR = __dirname;
const envFile = fs.readFileSync(path.join(DIR, '.env'), 'utf-8');
const ENV = {};
envFile.split('\n').forEach(l => { const m = l.match(/^([^#=]+)=(.*)$/); if (m) ENV[m[1].trim()] = m[2].trim(); });

function hr(o, b) { return new Promise((r, j) => { const q = https.request(o, res => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => r({ s: res.statusCode, b: Buffer.concat(c).toString() })); }); q.on('error', j); if (b) q.write(b); q.end(); }); }
function normalize(s) { return (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim(); }

const WS = '2929476c-7b92-4366-9236-ccd13ffbd917';
const DS = '583e34d7-6dd1-467b-86aa-3b74cfe1ca56';

async function main() {
    console.log('=== Patch Invoices (Painel CS) ===\n');

    // 1. Get token
    const body = querystring.stringify({
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
        console.log('Refresh token atualizado');
    }
    const token = td.access_token;
    if (!token) { console.error('No token:', td.error_description); process.exit(1); }

    // 2. Fetch all invoices
    console.log('Buscando invoices do Painel CS...');
    const dax = 'EVALUATE SELECTCOLUMNS(Invoices, "id", Invoices[id], "dominio", Invoices[Dominio], "marca", Invoices[Marca], "iugu_name", Invoices[Iugu_name], "due", Invoices[due_date_TIMESTAMP], "status", Invoices[status], "valor", Invoices[ValorFatura], "plano", Invoices[Plano], "produto", Invoices[Produto])';
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
    console.log('Raw rows:', rows.length);

    // 3. Deduplicate by invoice id
    const seen = new Set();
    const invoices = [];
    rows.forEach(r => {
        const id = r['[id]'];
        if (!id || seen.has(id)) return;
        seen.add(id);
        const due = (r['[due]'] || '').substring(0, 10);
        invoices.push({
            dominio: r['[dominio]'] ? String(r['[dominio]']) : '',
            marca: r['[marca]'] || '',
            iugu_name: r['[iugu_name]'] || '',
            due,
            dueMonth: due.substring(0, 7),
            status: r['[status]'] || '',
            valor: r['[valor]'] || 0,
            plano: r['[plano]'] || '',
        });
    });
    console.log('Unique invoices:', invoices.length);

    // Status distribution
    const statuses = {};
    invoices.forEach(i => { statuses[i.status] = (statuses[i.status] || 0) + 1; });
    console.log('Statuses:', JSON.stringify(statuses));

    // 4. Group by dominio + by marca (fallback)
    const byDominio = {};
    const byMarca = {};
    invoices.forEach(i => {
        function addTo(map, key) {
            if (!key) return;
            if (!map[key]) map[key] = { plano: '', invoices: [], paid: 0, pending: 0, expired: 0, canceled: 0, total: 0 };
            const b = map[key];
            if (i.plano && !b.plano) b.plano = i.plano;
            b.total++;
            b.invoices.push({ mes: i.dueMonth, status: i.status, total: i.valor, due: i.due });
            if (i.status === 'paid' || i.status === 'externally_paid') b.paid += i.valor;
            else if (i.status === 'pending') b.pending += i.valor;
            else if (i.status === 'expired') b.expired += i.valor;
            else if (i.status === 'canceled') b.canceled += i.valor;
        }
        addTo(byDominio, i.dominio);
        addTo(byMarca, normalize(i.marca));
    });
    console.log('Domains:', Object.keys(byDominio).length, '| Brands:', Object.keys(byMarca).length);

    // 5. Load dados.js
    console.log('\nCarregando dados.js...');
    const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();
    console.log('Empresas:', DADOS.empresas.length);

    // 6. Match and patch
    let matched = 0;
    for (const e of DADOS.empresas) {
        let data = null;
        // Match by idDominio
        if (e.idDominio) data = byDominio[String(e.idDominio)];
        // Fallback: match by nome -> marca
        if (!data) {
            const nNorm = normalize(e.nome);
            data = byMarca[nNorm];
            if (!data) {
                for (const [mk, md] of Object.entries(byMarca)) {
                    if (mk.length >= 4 && (nNorm.startsWith(mk) || mk.startsWith(nNorm))) {
                        data = md; break;
                    }
                }
            }
        }
        if (data) {
            e.faturamento = {
                planoIugu: data.plano,
                totalPago: Math.round(data.paid * 100) / 100,
                totalPendente: Math.round(data.pending * 100) / 100,
                totalVencido: Math.round(data.expired * 100) / 100,
                totalCancelado: Math.round(data.canceled * 100) / 100,
                qtdFaturas: data.total,
                faturas: data.invoices.sort((a, b) => b.due.localeCompare(a.due)).slice(0, 12).map(f => ({
                    mes: f.mes, status: f.status, total: f.total,
                })),
            };
            matched++;
        }
    }
    console.log('\nMatched:', matched + '/' + DADOS.empresas.length);

    // 7. Save
    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(path.join(DIR, 'dados.js'), output, 'utf-8');
    console.log('dados.js salvo (' + (output.length / 1024 / 1024).toFixed(1) + ' MB)');
}
main().catch(e => { console.error('ERRO:', e.message); process.exit(1); });
