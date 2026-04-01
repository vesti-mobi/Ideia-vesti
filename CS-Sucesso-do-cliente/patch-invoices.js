/**
 * Busca dados de Invoices (faturas Iugu) do Power BI e atualiza dados.js.
 * Substitui dados de Marcas e Planos na aba Financeiro.
 * SEMPRE usa groupby por invoice ID para evitar duplicatas.
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const DIR = __dirname;
const ENV = {};
const envPath = path.join(DIR, '.env');
if (fs.existsSync(envPath)) {
    fs.readFileSync(envPath, 'utf-8').split('\n').forEach(l => { const m = l.match(/^([^#=]+)=(.*)$/); if (m) ENV[m[1].trim()] = m[2].trim(); });
}
// Fallback to process.env
['FABRIC_TENANT_ID','FABRIC_REFRESH_TOKEN','FABRIC_CLIENT_ID'].forEach(k => { if (!ENV[k] && process.env[k]) ENV[k] = process.env[k]; });

const token_body = require('querystring').stringify({ client_id: ENV.FABRIC_CLIENT_ID, grant_type: 'refresh_token', refresh_token: ENV.FABRIC_REFRESH_TOKEN, scope: 'https://analysis.windows.net/powerbi/api/.default' });

function hr(o, b) { return new Promise((r, j) => { const q = https.request(o, res => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => r({ s: res.statusCode, b: Buffer.concat(c).toString() })); }); q.on('error', j); if (b) q.write(b); q.end(); }); }

function parseTotal(s) {
    if (!s || typeof s !== 'string') return 0;
    s = s.trim();
    // Format: "980.00 BRL" (international - dot is decimal)
    if (s.includes('BRL')) {
        return parseFloat(s.replace('BRL', '').trim()) || 0;
    }
    // Format: "R$ 1.234,56" (Brazilian - dot is thousands, comma is decimal)
    s = s.replace('R$', '').trim();
    s = s.replace(/\./g, '').replace(',', '.');
    return parseFloat(s) || 0;
}

function normalize(s) { return s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim(); }

async function main() {
    console.log('=== Patch Invoices ===\n');

    const tr = await hr({ hostname: 'login.microsoftonline.com', path: '/' + ENV.FABRIC_TENANT_ID + '/oauth2/v2.0/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(token_body) } }, token_body);
    const td = JSON.parse(tr.b);
    if (td.refresh_token) {
        const envUpdatePath = path.join(DIR, '.env');
        if (fs.existsSync(envUpdatePath)) {
            let env = fs.readFileSync(envUpdatePath, 'utf-8');
            env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + td.refresh_token);
            fs.writeFileSync(envUpdatePath, env, 'utf-8');
        }
    }
    const token = td.access_token;
    if (!token) { console.error('No token'); process.exit(1); }

    // Fetch ALL Invoices from Confecção - Assinaturas dataset
    const WS = 'aced753a-0f0e-4bcf-9264-72f6496cf2cf';
    const DS = 'becfc71d-0794-41fd-abdb-38bf9e0f2fd0';

    console.log('Buscando Invoices...');
    const dax = `EVALUATE SELECTCOLUMNS(Invoices, "invId", Invoices[id], "custId", Invoices[customer_id], "status", Invoices[status], "plan", Invoices[plano], "priceCents", Invoices[items_price_cents], "qty", Invoices[items_quantity])`;
    const body = JSON.stringify({ queries: [{ query: dax }], serializerSettings: { includeNulls: true } });
    const res = await hr({ hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + WS + '/datasets/' + DS + '/executeQueries', method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } }, body);
    const data = JSON.parse(res.b);
    if (data.error) { console.error('Error:', JSON.stringify(data.error).substring(0, 300)); process.exit(1); }
    const rows = data.results[0].tables[0].rows;
    console.log('Raw rows:', rows.length);

    // Fetch Customers table to get brand names
    console.log('Buscando Customers...');
    const daxCust = `EVALUATE SELECTCOLUMNS(Customers, "custId", Customers[id], "name", Customers[name])`;
    const bodyCust = JSON.stringify({ queries: [{ query: daxCust }], serializerSettings: { includeNulls: true } });
    const resCust = await hr({ hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + WS + '/datasets/' + DS + '/executeQueries', method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyCust) } }, bodyCust);
    const dataCust = JSON.parse(resCust.b);
    const custMap = {};
    if (!dataCust.error) {
        dataCust.results[0].tables[0].rows.forEach(r => {
            const id = r['[custId]'];
            const name = (r['[name]'] || '').split(' = ')[0].trim();
            if (id) custMap[id] = name;
        });
    }
    console.log('Customers loaded:', Object.keys(custMap).length);

    // Deduplicate invoices by ID
    const seen = new Set();
    const invoices = [];
    rows.forEach(r => {
        const invId = r['[invId]'];
        if (invId && !seen.has(invId)) {
            seen.add(invId);
            const custId = r['[custId]'] || '';
            const brandName = custMap[custId] || '';
            const priceCents = r['[priceCents]'] || 0;
            const total = Math.abs(priceCents) / 100; // centavos -> reais
            invoices.push({
                brand: brandName,
                invId,
                due: '', // not available in this dataset
                dueMonth: '',
                status: r['[status]'] || '',
                total,
                plan: r['[plan]'] || '',
                custId,
            });
        }
    });
    console.log('Unique invoices:', invoices.length);

    // Status distribution
    const statuses = {};
    invoices.forEach(i => { statuses[i.status] = (statuses[i.status] || 0) + 1; });
    console.log('Statuses:', JSON.stringify(statuses));

    // Group by customer brand name
    const byBrand = {};
    invoices.forEach(i => {
        if (!i.brand) return;
        if (!byBrand[i.brand]) byBrand[i.brand] = { brand: i.brand, plan: '', invoices: [], paid: 0, pending: 0, expired: 0, canceled: 0, totalInvoices: 0 };
        const b = byBrand[i.brand];
        if (i.plan && !b.plan) b.plan = i.plan;
        b.totalInvoices++;
        b.invoices.push({ mes: i.dueMonth, status: i.status, total: i.total, due: i.due });
        if (i.status === 'paid' || i.status === 'externally_paid') b.paid += i.total;
        else if (i.status === 'pending') b.pending += i.total;
        else if (i.status === 'expired') b.expired += i.total;
        else if (i.status === 'canceled') b.canceled += i.total;
    });
    console.log('Brands:', Object.keys(byBrand).length);

    // Top 10
    const sorted = Object.values(byBrand).sort((a, b) => b.paid - a.paid);
    console.log('\nTop 10 paid:');
    sorted.slice(0, 10).forEach(b => console.log('  ' + b.brand + ' | plan:' + b.plan + ' | paid:' + b.paid.toFixed(2) + ' | pending:' + b.pending.toFixed(2) + ' | invoices:' + b.totalInvoices));

    // Load dados.js
    console.log('\nCarregando dados.js...');
    const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();

    // Match brands to empresas by name
    let matched = 0;
    for (const e of DADOS.empresas) {
        const nomeNorm = normalize(e.nome);
        let brandData = null;
        for (const [brand, data] of Object.entries(byBrand)) {
            const brandNorm = normalize(brand);
            if (brandNorm.length < 4) continue;
            if (nomeNorm === brandNorm) { brandData = data; break; }
            // Partial: require min 5 chars and the shorter must be significant portion
            const shorter = Math.min(nomeNorm.length, brandNorm.length);
            if (shorter >= 5 && (nomeNorm.startsWith(brandNorm) || brandNorm.startsWith(nomeNorm))) {
                brandData = data; break;
            }
        }
        if (brandData) {
            e.faturamento = {
                planoIugu: brandData.plan,
                totalPago: Math.round(brandData.paid * 100) / 100,
                totalPendente: Math.round(brandData.pending * 100) / 100,
                totalVencido: Math.round(brandData.expired * 100) / 100,
                totalCancelado: Math.round(brandData.canceled * 100) / 100,
                qtdFaturas: brandData.totalInvoices,
                faturas: brandData.invoices.sort((a, b) => b.due.localeCompare(a.due)).slice(0, 12).map(f => ({
                    mes: f.mes,
                    status: f.status,
                    total: f.total,
                })),
            };
            // Atualizar mensalidade com valor da ultima fatura (mais recente, nao cancelada)
            const sortedInvs = brandData.invoices.sort((a, b) => b.due.localeCompare(a.due));
            const ultimaFatura = sortedInvs.find(f => f.status !== 'canceled') || sortedInvs[0];
            if (ultimaFatura && ultimaFatura.total > 0) {
                e.planoMensalidade = Math.round(ultimaFatura.total * 100) / 100;
                e.mensalidade = 'R$ ' + ultimaFatura.total.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            }
            // Status da ultima fatura para exibicao no painel
            if (ultimaFatura) {
                e.faturaStatus = ultimaFatura.status;
                const statusCount = { pagas: 0, pendentes: 0, vencidas: 0 };
                sortedInvs.forEach(f => {
                    if (f.status === 'paid' || f.status === 'externally_paid') statusCount.pagas++;
                    else if (f.status === 'pending') statusCount.pendentes++;
                    else if (f.status === 'expired') statusCount.vencidas++;
                });
                e.faturasPagas = statusCount.pagas;
                e.faturasPendentes = statusCount.pendentes;
                e.faturasVencidas = statusCount.vencidas;
            }
            matched++;
        }
    }
    console.log('Matched:', matched);

    // Sample
    const sample = DADOS.empresas.filter(e => e.faturamento).slice(0, 3);
    sample.forEach(e => console.log('  ' + e.nome + ' | plan:' + e.faturamento.planoIugu + ' | paid:' + e.faturamento.totalPago + ' | faturas:' + e.faturamento.qtdFaturas));

    // Save
    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(path.join(DIR, 'dados.js'), output, 'utf-8');
    console.log('\ndados.js salvo (' + (output.length / 1024 / 1024).toFixed(1) + ' MB)');
}
main().catch(e => { console.error(e); process.exit(1); });
