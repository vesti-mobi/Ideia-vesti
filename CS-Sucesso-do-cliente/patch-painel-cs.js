/**
 * Busca dados mensais de vendas/pedidos do Oráculo do dataset "Painel CS"
 * e atualiza o dados.js com mensal por empresa.
 * Uso: node patch-painel-cs.js
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const DIR = __dirname;

const ENV = {};
const envPath = path.join(DIR, '.env');
if (fs.existsSync(envPath)) {
    fs.readFileSync(envPath, 'utf-8').split('\n').forEach(l => {
        const m = l.match(/^([^#=]+)=(.*)$/);
        if (m) ENV[m[1].trim()] = m[2].trim();
    });
}
['FABRIC_TENANT_ID','FABRIC_REFRESH_TOKEN','FABRIC_CLIENT_ID'].forEach(k => { if (!ENV[k] && process.env[k]) ENV[k] = process.env[k]; });

const PAINEL_CS_WS = '2929476c-7b92-4366-9236-ccd13ffbd917';
const PAINEL_CS_DS = '583e34d7-6dd1-467b-86aa-3b74cfe1ca56';

function httpsRequest(options, body) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve({ statusCode: res.statusCode, body: Buffer.concat(chunks).toString() }));
        });
        req.on('error', reject);
        if (body) req.write(body);
        req.end();
    });
}

async function getToken() {
    const qs = require('querystring');
    const postBody = qs.stringify({
        client_id: ENV.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e',
        grant_type: 'refresh_token',
        refresh_token: ENV.FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default',
    });
    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: '/' + ENV.FABRIC_TENANT_ID + '/oauth2/v2.0/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postBody) },
    }, postBody);
    const data = JSON.parse(res.body);
    if (data.refresh_token && fs.existsSync(envPath)) {
        let env = fs.readFileSync(envPath, 'utf-8');
        env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + data.refresh_token);
        fs.writeFileSync(envPath, env, 'utf-8');
        console.log('  Refresh token atualizado');
    }
    if (data.refresh_token) {
        fs.writeFileSync(path.join(DIR, '.new_refresh_token'), data.refresh_token, 'utf-8');
    }
    return data.access_token;
}

async function daxQuery(token, query, label) {
    const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + PAINEL_CS_WS + '/datasets/' + PAINEL_CS_DS + '/executeQueries',
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    if (res.statusCode !== 200) { console.log('  ERRO ' + label + ': HTTP ' + res.statusCode); return []; }
    const data = JSON.parse(res.body);
    if (data.error) { console.log('  ERRO ' + label + ': ' + JSON.stringify(data.error).substring(0, 200)); return []; }
    const rows = data.results[0].tables[0].rows || [];
    console.log('  ' + label + ': ' + rows.length + ' rows');
    return rows;
}

function normalize(s) { return (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim(); }

async function main() {
    console.log('=== Patch Painel CS ===\n');

    if (!ENV.FABRIC_REFRESH_TOKEN || !ENV.FABRIC_TENANT_ID) {
        console.error('FABRIC_REFRESH_TOKEN e FABRIC_TENANT_ID são obrigatórios');
        process.exit(1);
    }

    const token = await getToken();
    if (!token) { console.error('Falha ao obter token'); process.exit(1); }
    console.log('  Token obtido\n');

    // 1. Buscar vendas mensais por empresa do Consulta1 (oráculo)
    console.log('Buscando vendas mensais por empresa...');
    const vendasRows = await daxQuery(token,
        `EVALUATE SUMMARIZECOLUMNS(Consulta1[company_name], Consulta1[companyId], Consulta1[DataMês], "vendas", SUM(Consulta1[summary_total]), "pedidos", COUNTROWS(Consulta1))`,
        'Vendas mensais'
    );

    // Agrupar por companyId E por nome -> {mes: {vendas, pedidos}}
    const vendasByCompany = {};
    const vendasByName = {};
    vendasRows.forEach(r => {
        const companyId = r['Consulta1[companyId]'] || '';
        const companyName = r['Consulta1[company_name]'] || '';
        const dt = r['Consulta1[DataMês]'] || '';
        const mes = dt.substring(0, 7);
        const vendas = r['[vendas]'] || 0;
        const pedidos = r['[pedidos]'] || 0;
        if (!mes || mes.length !== 7) return;
        // Index by companyId
        if (companyId) {
            if (!vendasByCompany[companyId]) vendasByCompany[companyId] = { name: companyName, mensal: {} };
            if (!vendasByCompany[companyId].mensal[mes]) vendasByCompany[companyId].mensal[mes] = { vendas: 0, pedidos: 0 };
            vendasByCompany[companyId].mensal[mes].vendas += Math.round(vendas * 100) / 100;
            vendasByCompany[companyId].mensal[mes].pedidos += pedidos;
        }
        // Index by name (aggregate all companyIds with same name)
        const n = normalize(companyName);
        if (n) {
            if (!vendasByName[n]) vendasByName[n] = { name: companyName, mensal: {} };
            if (!vendasByName[n].mensal[mes]) vendasByName[n].mensal[mes] = { vendas: 0, pedidos: 0 };
            vendasByName[n].mensal[mes].vendas += Math.round(vendas * 100) / 100;
            vendasByName[n].mensal[mes].pedidos += pedidos;
        }
    });
    console.log('  Empresas com dados mensais (por ID): ' + Object.keys(vendasByCompany).length);
    console.log('  Empresas com dados mensais (por nome): ' + Object.keys(vendasByName).length);

    // 2. Buscar invoices com mês
    console.log('\nBuscando invoices...');
    const invoicesRows = await daxQuery(token,
        `EVALUATE SELECTCOLUMNS(Invoices, "marca", Invoices[Marca], "iugu_name", Invoices[Iugu_name], "dominio", Invoices[Dominio], "mes", Invoices[due_date_TIMESTAMP], "status", Invoices[status], "valor", Invoices[ValorFatura], "plano", Invoices[Plano])`,
        'Invoices'
    );

    // Agrupar invoices por marca/domínio
    const invoicesByDominio = {};
    const invoicesByMarca = {};
    invoicesRows.forEach(r => {
        const dominio = r['[dominio]'] || '';
        const marca = r['[marca]'] || '';
        const iuguName = r['[iugu_name]'] || '';
        const dt = r['[mes]'] || '';
        const mes = dt.substring(0, 7);
        const status = r['[status]'] || '';
        const valor = r['[valor]'] || 0;
        const plano = r['[plano]'] || '';
        const inv = { mes, status, total: Math.round(valor * 100) / 100, plano };

        if (dominio) {
            if (!invoicesByDominio[dominio]) invoicesByDominio[dominio] = [];
            invoicesByDominio[dominio].push(inv);
        }
        const key = normalize(marca || iuguName.split(' = ')[0]);
        if (key) {
            if (!invoicesByMarca[key]) invoicesByMarca[key] = [];
            invoicesByMarca[key].push(inv);
        }
    });
    console.log('  Invoices por domínio: ' + Object.keys(invoicesByDominio).length);
    console.log('  Invoices por marca: ' + Object.keys(invoicesByMarca).length);

    // 3. Load dados.js
    console.log('\nCarregando dados.js...');
    const dadosPath = path.join(DIR, 'dados.js');
    const content = fs.readFileSync(dadosPath, 'utf-8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();
    console.log('  ' + DADOS.empresas.length + ' empresas');

    // 4. Match and patch
    let matchedVendas = 0, matchedInvoices = 0;
    for (const e of DADOS.empresas) {
        // Match vendas mensais by companyId or name
        let vData = vendasByCompany[e.id];
        if (!vData) {
            const n = normalize(e.nome);
            const match = vendasByName[n];
            if (match) vData = match;
            else {
                // Partial match
                for (const [vn, vd] of Object.entries(vendasByName)) {
                    if (n.length >= 5 && vn.length >= 5 && (n.includes(vn) || vn.includes(n))) { vData = vd; break; }
                }
            }
        }
        if (vData && Object.keys(vData.mensal).length > 0) {
            if (!e.oraculoFabric) e.oraculoFabric = {};
            e.oraculoFabric.vendasMensal = {};
            e.oraculoFabric.pedidosMensal = {};
            let totalVendas = 0, totalPedidos = 0;
            Object.entries(vData.mensal).forEach(([mes, d]) => {
                e.oraculoFabric.vendasMensal[mes] = d.vendas;
                e.oraculoFabric.pedidosMensal[mes] = d.pedidos;
                totalVendas += d.vendas;
                totalPedidos += d.pedidos;
            });
            e.oraculoFabric.vendasOraculo = Math.round(totalVendas * 100) / 100;
            e.oraculoFabric.pedidosOraculo = totalPedidos;
            matchedVendas++;
        }

        // Match invoices by domínio ID or name
        const domId = String(e.idDominio || '');
        let invs = invoicesByDominio[domId];
        if (!invs) {
            const n = normalize(e.nome);
            invs = invoicesByMarca[n];
            if (!invs) {
                for (const [mn, mi] of Object.entries(invoicesByMarca)) {
                    if (n.length >= 5 && mn.length >= 5 && (n.includes(mn) || mn.includes(n))) { invs = mi; break; }
                }
            }
        }
        if (invs && invs.length > 0) {
            // Sort by date desc
            invs.sort((a, b) => b.mes.localeCompare(a.mes));
            let paid = 0, pending = 0, expired = 0;
            invs.forEach(inv => {
                if (inv.status === 'paid' || inv.status === 'externally_paid') paid += inv.total;
                else if (inv.status === 'pending') pending += inv.total;
                else if (inv.status === 'expired') expired += inv.total;
            });
            e.faturamento = {
                planoIugu: invs[0].plano || '',
                totalPago: Math.round(paid * 100) / 100,
                totalPendente: Math.round(pending * 100) / 100,
                totalVencido: Math.round(expired * 100) / 100,
                qtdFaturas: invs.length,
                faturas: invs, // todas as faturas, sem corte
            };
            // Update faturaStatus
            const uf = invs.find(i => i.status !== 'canceled') || invs[0];
            e.faturaStatus = uf.status;
            matchedInvoices++;
        }
    }

    console.log('\nVendas mensais matched: ' + matchedVendas);
    console.log('Invoices matched: ' + matchedInvoices);

    // 5. Save
    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(dadosPath, output, 'utf-8');
    console.log('\ndados.js salvo (' + (output.length / 1024 / 1024).toFixed(1) + ' MB)');
    console.log('\n=== CONCLUÍDO ===');
}

main().catch(e => { console.error('ERRO:', e.message); process.exit(1); });
