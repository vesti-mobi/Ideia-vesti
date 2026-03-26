/**
 * Cloud build for Pedidos por Marca - fetches from Fabric DAX API + Lakehouse SQL.
 * Combines: Merged Pedidos (2025+) + ODBC_Quotes + OBDC_Quotes_Anterior2023 (historical)
 * Required env vars: FABRIC_REFRESH_TOKEN, FABRIC_TENANT_ID
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const querystring = require('querystring');
const { Connection, Request } = require('tedious');

const DIR = __dirname;
const WORKSPACE_ID = 'aced753a-0f0e-4bcf-9264-72f6496cf2cf';
const DATASET_ID = 'e6c74524-e355-4447-9eb4-baae76b84dc4';
const FABRIC_REFRESH_TOKEN = process.env.FABRIC_REFRESH_TOKEN || '';
const FABRIC_TENANT_ID = process.env.FABRIC_TENANT_ID || '';

// VestiLake SQL endpoint
const SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com';
const DB_NAME = 'VestiHouse';

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

function getToken(scope) {
    return async function() {
        const postBody = querystring.stringify({
            client_id: '1950a258-227b-4e31-a9cf-717495945fc2',
            grant_type: 'refresh_token',
            refresh_token: FABRIC_REFRESH_TOKEN,
            scope: scope + ' offline_access',
        });
        const res = await httpsRequest({
            hostname: 'login.microsoftonline.com',
            path: '/' + FABRIC_TENANT_ID + '/oauth2/v2.0/token',
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postBody) },
        }, postBody);
        const data = JSON.parse(res.body);
        if (!data.access_token) throw new Error('Token failed: ' + (data.error_description || ''));
        if (data.refresh_token) {
            fs.writeFileSync(path.join(DIR, '..', 'CS-Sucesso-do-cliente', '.new_refresh_token'), data.refresh_token, 'utf-8');
        }
        return data.access_token;
    };
}

async function daxQuery(token, query, label) {
    console.log('  ' + label + '...');
    const bodyStr = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets/' + DATASET_ID + '/executeQueries',
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
    }, bodyStr);
    if (res.statusCode !== 200) { console.error('  ERROR ' + label + ': HTTP ' + res.statusCode); return []; }
    const data = JSON.parse(res.body);
    if (data.error) { console.error('  ERROR: ' + JSON.stringify(data.error).substring(0, 300)); return []; }
    const rows = (data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows) || [];
    const cleaned = rows.map(row => {
        const obj = {};
        for (const [key, val] of Object.entries(row)) {
            const m = key.match(/\[(.+)\]$/);
            obj[m ? m[1] : key] = val;
        }
        return obj;
    });
    console.log('  ' + label + ': ' + cleaned.length + ' rows');
    return cleaned;
}

function runSQL(token, query, label) {
    return new Promise((resolve, reject) => {
        console.log('  ' + label + '...');
        const config = {
            server: SQL_SERVER,
            authentication: { type: 'azure-active-directory-access-token', options: { token } },
            options: { database: DB_NAME, encrypt: true, port: 1433, requestTimeout: 300000 },
        };
        const conn = new Connection(config);
        const rows = [];
        conn.on('connect', err => {
            if (err) { reject(err); return; }
            const request = new Request(query, (err) => {
                if (err) reject(err);
                conn.close();
            });
            request.on('row', columns => {
                const row = {};
                columns.forEach(col => { row[col.metadata.colName] = col.value; });
                rows.push(row);
            });
            request.on('requestCompleted', () => {
                console.log('  ' + label + ': ' + rows.length + ' rows');
                resolve(rows);
            });
            conn.execSql(request);
        });
        conn.connect();
    });
}

async function main() {
    console.log('=== Pedidos por Marca - Cloud Build (com histórico) ===\n');

    if (!FABRIC_REFRESH_TOKEN || !FABRIC_TENANT_ID) throw new Error('FABRIC_REFRESH_TOKEN and FABRIC_TENANT_ID required');

    // Get tokens for both APIs
    const pbiToken = await getToken('https://analysis.windows.net/powerbi/api/.default')();
    console.log('PBI token OK.');
    const sqlToken = await getToken('https://database.windows.net/.default')();
    console.log('SQL token OK.\n');

    // --- 1. DAX queries (Cadastros, Marcas, Merged Pedidos) ---
    console.log('Fetching from Power BI DAX...');
    const [cadastros, marcas] = await Promise.all([
        daxQuery(pbiToken, "EVALUATE 'Cadastros Empresas'", 'Cadastros'),
        daxQuery(pbiToken, "EVALUATE 'Marcas e Planos'", 'Marcas e Planos'),
    ]);

    // --- 2. SQL queries (Lakehouse historical) ---
    console.log('\nFetching from VestiLake SQL...');
    const [quotesRows, anteriorRows, customersRows, mongoPedidosNames] = await Promise.all([
        runSQL(sqlToken,
            "SELECT id, company_id, domain_id, customer_id, total_price, created_at, status_payment, status, app FROM dbo.ODBC_Quotes",
            'ODBC_Quotes'),
        runSQL(sqlToken,
            "SELECT id, company_id, domain_id, customer_id, total_price, created_at FROM dbo.OBDC_Quotes_Anterior2023",
            'OBDC_Quotes_Anterior2023'),
        runSQL(sqlToken,
            "SELECT c.id as customer_id, u.name, u.lastname FROM dbo.ODBC_Costumers c INNER JOIN dbo.ODBC_Users u ON c.user_id = u.id",
            'Customer names (join)'),
        runSQL(sqlToken,
            "SELECT _id, companyId, customer_name, summary_total, payment_consolidatedPaymentStatus, status_consolidatedOrderStatus, status_canceled_isCanceled, settings_source, settings_createdAt, orderNumber FROM dbo.MongoDB_Pedidos_Geral",
            'MongoDB_Pedidos_Geral'),
    ]);

    // --- 3. Build empresas ---
    // Build customer name map
    console.log('\nProcessing...');
    const customerNames = {};
    for (const r of customersRows) {
        if (r.customer_id) {
            const fullName = ((r.name || '') + ' ' + (r.lastname || '')).trim();
            if (fullName) customerNames[r.customer_id] = fullName;
        }
    }
    console.log('  Customer names (ODBC): ' + Object.keys(customerNames).length);

    console.log('  MongoDB_Pedidos_Geral: ' + mongoPedidosNames.length + ' rows');

    const empresas = {};
    const empresasByDom = {}; // domain_id -> empresa id
    for (const r of cadastros) {
        const id = r['Id Empresa']; if (!id) continue;
        empresas[id] = { id, nome: r['Nome Fantasia'] || r['Nome do Dominio'] || '', cnpj: r['CNPJ'] || '', anjo: r['Anjo'] || '', canal: r['Canal de Vendas'] || '' };
        const domId = r['Id Dominio'];
        if (domId) empresasByDom[String(domId)] = id;
    }

    const marcasByCnpj = {};
    for (const r of marcas) {
        const cnpj = r['CPFCNPJ'] || '';
        if (cnpj) marcasByCnpj[cnpj] = { marca: r['MARCA'] || '', plano: r['PLANO'] || '' };
    }
    for (const e of Object.values(empresas)) {
        const cnpjNum = (e.cnpj || '').replace(/[.\-\/]/g, '');
        const m = marcasByCnpj[cnpjNum];
        e.marca = m ? m.marca : '';
        e.plano = m ? m.plano : '';
    }

    // --- 4. Build pedidos por empresa ---
    const pedidosPorEmp = {};

    // 4a. MongoDB_Pedidos_Geral (pedidos recentes com customer_name - preenche gap 2023-2026)
    // Format: [dt, val, status, metodo, pedidoId, nomeCliente]
    const seenKeys = new Set(); // track composite key to avoid duplicates
    const orderLookup = {}; // companyId+date+val -> {orderNum, custName} (for ODBC_Quotes)
    let countMongo = 0;
    for (const r of mongoPedidosNames) {
        const empId = r.companyId && empresas[r.companyId] ? r.companyId : null;
        if (!empId) continue;
        if (!pedidosPorEmp[empId]) pedidosPorEmp[empId] = [];
        const payStatus = (r.payment_consolidatedPaymentStatus || '').toString().toUpperCase();
        const orderStatus = (r.status_consolidatedOrderStatus || '').toString().toUpperCase();
        const isCanceled = r.status_canceled_isCanceled === true || r.status_canceled_isCanceled === 'True';
        let status;
        if (payStatus === 'PAID') status = 'P';
        else if (isCanceled || orderStatus === 'CANCELED') status = 'C';
        else if (payStatus === 'PENDING' || payStatus === 'WAITING' || orderStatus === 'PENDING') status = 'E';
        else if (payStatus === 'REJECTED' || payStatus === 'REFUSED') status = 'C';
        else if (payStatus === 'APPROVED' || payStatus === 'AUTHORIZED') status = 'P';
        else status = 'O';
        const dtRaw = r.settings_createdAt || '';
        const dt = dtRaw ? dtRaw.toString().substring(0, 10) : '';
        const met = (r.settings_source || '').toString().substring(0, 15);
        const orderNum = (r.orderNumber || '').toString();
        const custName = (r.customer_name || '').toString();
        const pid = (r._id || '').toString();
        const val = Math.round((parseFloat(r.summary_total) || 0) * 100) / 100;
        pedidosPorEmp[empId].push([dt, val, status, met, orderNum, custName]);
        // Build composite key for dedup and lookup
        const key = empId + '|' + dt + '|' + val;
        seenKeys.add(key);
        if (orderNum || custName) orderLookup[key] = { orderNum, custName };
        countMongo++;
    }
    console.log('  MongoDB_Pedidos_Geral processed: ' + countMongo);

    // Helper: resolve company_id or fallback to domain_id
    function resolveEmpId(companyId, domainId) {
        if (companyId && empresas[companyId]) return companyId;
        if (domainId && empresasByDom[String(domainId)]) return empresasByDom[String(domainId)];
        return null;
    }

    // 4b. ODBC_Quotes (historical - up to 2024, skip duplicates already in MongoDB)
    let countQuotes = 0, countQuotesDom = 0, countSkipped = 0;
    for (const r of quotesRows) {
        const empId = resolveEmpId(r.company_id, r.domain_id);
        if (!empId) continue;
        if (!pedidosPorEmp[empId]) pedidosPorEmp[empId] = [];
        const sp = (r.status_payment || '').toString().toUpperCase();
        const stNum = r.status;
        // status_payment: PAGO, AUTORIZADO = Pago; CANCELADA, REJEITADO = Cancelado; ANÁLISE = Pendente
        // status (numeric): 1 = ativo/pago, 3 = cancelado
        let status;
        if (sp === 'PAGO' || sp === 'AUTORIZADO') status = 'P';
        else if (sp.includes('CANCEL') || sp === 'REJEITADO') status = 'C';
        else if (sp.includes('ANALISE') || sp.includes('ANÁLISE')) status = 'E';
        else if (stNum === 3) status = 'C';
        else if (stNum === 1) status = 'P';
        else status = 'O';
        const dt = r.created_at ? new Date(r.created_at).toISOString().substring(0, 10) : '';
        const val = Math.round((parseFloat(r.total_price) || 0) * 100) / 100;
        const key = empId + '|' + dt + '|' + val;
        if (seenKeys.has(key)) { countSkipped++; continue; }
        seenKeys.add(key);
        const met = (r.app || '').toString().substring(0, 15);
        const lookup = orderLookup[key];
        const orderNum = lookup ? lookup.orderNum : '';
        const custName = lookup ? lookup.custName : (r.customer_id ? (customerNames[r.customer_id] || '') : '');
        pedidosPorEmp[empId].push([dt, val, status, met, orderNum, custName]);
        if (!(r.company_id && empresas[r.company_id])) countQuotesDom++;
        countQuotes++;
    }
    console.log('  ODBC_Quotes processed: ' + countQuotes + ' (via domain_id: ' + countQuotesDom + ', skipped dupes: ' + countSkipped + ')');

    // 4c. OBDC_Quotes_Anterior2023 (oldest, skip duplicates)
    let countAnterior = 0, countAnteriorDom = 0, countSkipped2 = 0;
    for (const r of anteriorRows) {
        const empId = resolveEmpId(r.company_id, r.domain_id);
        if (!empId) continue;
        if (!pedidosPorEmp[empId]) pedidosPorEmp[empId] = [];
        const dt = r.created_at ? new Date(r.created_at).toISOString().substring(0, 10) : '';
        const val = Math.round((parseFloat(r.total_price) || 0) * 100) / 100;
        const key = empId + '|' + dt + '|' + val;
        if (seenKeys.has(key)) { countSkipped2++; continue; }
        seenKeys.add(key);
        const lookup = orderLookup[key];
        const orderNum = lookup ? lookup.orderNum : '';
        const custName = lookup ? lookup.custName : (r.customer_id ? (customerNames[r.customer_id] || '') : '');
        pedidosPorEmp[empId].push([dt, val, 'O', '', orderNum, custName]);
        if (!(r.company_id && empresas[r.company_id])) countAnteriorDom++;
        countAnterior++;
    }
    console.log('  OBDC_Quotes_Anterior2023 processed: ' + countAnterior + ' (via domain_id: ' + countAnteriorDom + ')');

    // Sort by date desc
    for (const id of Object.keys(pedidosPorEmp)) {
        pedidosPorEmp[id].sort((a, b) => b[0].localeCompare(a[0]));
    }

    // Empresa list
    const empList = Object.values(empresas)
        .filter(e => pedidosPorEmp[e.id] && pedidosPorEmp[e.id].length > 0)
        .map(e => ({ id: e.id, nome: e.nome, cnpj: e.cnpj, marca: e.marca, plano: e.plano, anjo: e.anjo, canal: e.canal, qtd: pedidosPorEmp[e.id].length }))
        .sort((a, b) => b.qtd - a.qtd);

    // Save dados.js
    fs.writeFileSync(path.join(DIR, 'dados.js'), 'const DADOS=' + JSON.stringify({ empresas: empList, gerado: new Date().toISOString() }) + ';', 'utf-8');

    // Save chunks
    const dataDir = path.join(DIR, 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);
    for (const f of fs.readdirSync(dataDir)) { if (f.startsWith('chunk_')) fs.unlinkSync(path.join(dataDir, f)); }

    const BATCH = 50;
    const empIds = empList.map(e => e.id);
    let totalPed = 0;
    for (let i = 0; i < empIds.length; i += BATCH) {
        const batch = {};
        for (let j = i; j < Math.min(i + BATCH, empIds.length); j++) {
            const id = empIds[j];
            batch[id] = pedidosPorEmp[id] || [];
            totalPed += batch[id].length;
        }
        fs.writeFileSync(path.join(dataDir, 'chunk_' + Math.floor(i / BATCH) + '.js'), 'loadChunk(' + JSON.stringify(batch) + ');', 'utf-8');
    }

    const chunkMap = {};
    empIds.forEach((id, i) => { chunkMap[id] = Math.floor(i / BATCH); });
    fs.writeFileSync(path.join(DIR, 'chunks.js'), 'const CHUNKS=' + JSON.stringify(chunkMap) + ';', 'utf-8');

    console.log('\n=== RESULTADO ===');
    console.log('Empresas: ' + empList.length);
    console.log('Pedidos MongoDB: ' + countMongo);
    console.log('Pedidos ODBC_Quotes: ' + countQuotes);
    console.log('Pedidos Anterior2023: ' + countAnterior);
    console.log('Total pedidos: ' + totalPed);
    console.log('Chunks: ' + Math.ceil(empIds.length / BATCH));
    console.log('Done.');
}
main().catch(e => { console.error('FATAL:', e); process.exit(1); });
