/**
 * Script para agregar dados dos CSVs do Power BI e gerar dados.js para o dashboard.
 * Inclui dados do HubSpot (Oráculo), controle_geral_luana, Marcas e Planos (Excel) e Fabric Oráculo.
 * Executa com: node build-data.js
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const https = require('https');
const XLSX = require('xlsx');

const DIR = __dirname;

// Load token from .env file
function loadEnv() {
    const envPath = path.join(DIR, '.env');
    if (!fs.existsSync(envPath)) return {};
    const env = {};
    fs.readFileSync(envPath, 'utf-8').split('\n').forEach(line => {
        const m = line.match(/^([^=]+)=(.*)$/);
        if (m) env[m[1].trim()] = m[2].trim();
    });
    return env;
}
const ENV = loadEnv();
const HUBSPOT_TOKEN = ENV.HUBSPOT_TOKEN || process.env.HUBSPOT_TOKEN || '';
const FABRIC_REFRESH_TOKEN = ENV.FABRIC_REFRESH_TOKEN || process.env.FABRIC_REFRESH_TOKEN || '';
const FABRIC_TENANT_ID = ENV.FABRIC_TENANT_ID || process.env.FABRIC_TENANT_ID || '';
const VP_WORKSPACE_ID = ENV.VP_WORKSPACE_ID || 'f80301c2-8735-40d2-8662-1f8a627d3f61';
const VP_DATASET_ID = ENV.VP_DATASET_ID || '606be0ee-2c8c-4f43-8ad6-0be04f95d616';
// Oráculo Fabric - pode ser o mesmo workspace/dataset ou diferente
const ORACULO_WORKSPACE_ID = ENV.ORACULO_WORKSPACE_ID || VP_WORKSPACE_ID;
const ORACULO_DATASET_ID = ENV.ORACULO_DATASET_ID || VP_DATASET_ID;
const ORACULO_PIPELINE_ID = '794686264';
const ORACULO_STAGES = {
    '1165541427':'Fila','1165361278':'Grupo de Implementação','1165350737':'Reunião 1',
    '1165350738':'Configurações Iniciais','1273974154':'Link de relatório',
    '1199622545':'Problema conta Meta ou YCloud','1180878228':'Acompanhamento e melhorias prompt',
    '1165350742':'Eventos Vesti','1216864772':'Agente Aquecimento de leads',
    '1204236378':'Integração','1183765142':'Agente Inativos','1269319857':'Campanhas',
    '1165361281':'Concluído','1238455699':'Parado','1249275660':'Churn'
};

// ===================== CSV PARSER (streaming, handles quotes) =====================
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

async function readCSV(filename, onRow, limit) {
    const filePath = path.join(DIR, filename);
    if (!fs.existsSync(filePath)) { console.log('  SKIP: ' + filename + ' not found'); return []; }
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
        if (limit && count >= limit) break;
    }
    console.log('  ' + filename + ': ' + count + ' rows');
}

// ===================== HUBSPOT API =====================
function hubspotRequest(endpoint, method, body) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'api.hubapi.com',
            path: endpoint,
            method: method || 'GET',
            headers: {
                'Authorization': 'Bearer ' + HUBSPOT_TOKEN,
                'Content-Type': 'application/json',
            },
        };
        const req = https.request(options, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
                catch(e) { reject(e); }
            });
        });
        req.on('error', reject);
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

async function fetchOraculoTickets() {
    console.log('  Fetching HubSpot Oráculo tickets...');
    try {
        const data = await hubspotRequest('/crm/v3/objects/tickets/search', 'POST', {
            filterGroups: [{ filters: [{ propertyName: 'hs_pipeline', operator: 'EQ', value: ORACULO_PIPELINE_ID }] }],
            properties: ['subject', 'hs_pipeline_stage', 'createdate', 'hs_lastmodifieddate'],
            limit: 100,
        });
        const tickets = (data.results || []).map(t => {
            const stageId = t.properties.hs_pipeline_stage;
            // Extract company name from subject (formats: "ÓRACULO - Company - ...", "Company - Oráculo", etc.)
            let companyName = (t.properties.subject || '').replace(/^[ÓO]R[ÁA]CULO\s*-\s*/i, '').replace(/\s*-\s*[ÓO]r[áa]culo.*/i, '').replace(/\s*-\s*Agente.*/i, '').replace(/\s*\|.*/, '').replace(/\s*\(.*\)/, '').trim();
            if (companyName.startsWith('Oráculo ')) companyName = companyName.replace('Oráculo ', '').trim();
            if (companyName.startsWith('Óraculo ')) companyName = companyName.replace('Óraculo ', '').trim();
            return {
                id: t.id,
                subject: t.properties.subject,
                companyName,
                stageId,
                stageName: ORACULO_STAGES[stageId] || stageId,
                created: t.properties.createdate,
                modified: t.properties.hs_lastmodifieddate,
            };
        });
        console.log('  HubSpot Oráculo: ' + tickets.length + ' tickets');
        return tickets;
    } catch(e) {
        console.log('  WARN: HubSpot fetch failed: ' + e.message);
        return [];
    }
}

// ===================== FABRIC API (VestiPago) =====================
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

async function fetchVestiPagoCompanies() {
    if (!FABRIC_REFRESH_TOKEN || !FABRIC_TENANT_ID) {
        console.log('  SKIP: FABRIC_REFRESH_TOKEN/FABRIC_TENANT_ID not set, skipping VestiPago');
        return new Set();
    }
    try {
        // Get access token
        const querystring = require('querystring');
        const postBody = querystring.stringify({
            client_id: '1950a258-227b-4e31-a9cf-717495945fc2',
            grant_type: 'refresh_token',
            refresh_token: FABRIC_REFRESH_TOKEN,
            scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
        });
        const tokenRes = await httpsRequest({
            hostname: 'login.microsoftonline.com',
            path: '/' + FABRIC_TENANT_ID + '/oauth2/v2.0/token',
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postBody) },
        }, postBody);
        const tokenData = JSON.parse(tokenRes.body);
        if (!tokenData.access_token) { console.log('  WARN: Failed to get Fabric token'); return new Set(); }

        // Save new refresh token if returned
        if (tokenData.refresh_token) {
            fs.writeFileSync(path.join(DIR, '.new_refresh_token'), tokenData.refresh_token, 'utf-8');
        }

        // Query VestiPago dataset
        const daxBody = JSON.stringify({
            queries: [{ query: 'EVALUATE SELECTCOLUMNS(Companies, "companyId", Companies[data.companyId])' }],
            serializerSettings: { includeNulls: true },
        });
        const daxRes = await httpsRequest({
            hostname: 'api.powerbi.com',
            path: '/v1.0/myorg/groups/' + VP_WORKSPACE_ID + '/datasets/' + VP_DATASET_ID + '/executeQueries',
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + tokenData.access_token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(daxBody) },
        }, daxBody);
        if (daxRes.statusCode !== 200) { console.log('  WARN: VestiPago query failed: HTTP ' + daxRes.statusCode); return new Set(); }
        const data = JSON.parse(daxRes.body);
        const rows = (data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows) || [];
        const set = new Set();
        rows.forEach(r => {
            const key = Object.keys(r).find(k => k.includes('companyId'));
            if (key && r[key]) set.add(r[key]);
        });
        console.log('  VestiPago companies from Fabric: ' + set.size);
        return set;
    } catch (e) {
        console.log('  WARN: VestiPago fetch failed: ' + e.message);
        return new Set();
    }
}

// ===================== FABRIC API (Oráculo Configurations) =====================
const FABRIC_CLIENT_ID = ENV.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e';
const ORACULO_WS_ID = '2929476c-7b92-4366-9236-ccd13ffbd917';
const ORACULO_DS_ID = 'c6a480e9-2db4-45f7-ba67-b489407f59e6'; // SQL analytics endpoint

async function getFabricToken(scope) {
    const querystring = require('querystring');
    const postBody = querystring.stringify({
        client_id: FABRIC_CLIENT_ID,
        grant_type: 'refresh_token',
        refresh_token: FABRIC_REFRESH_TOKEN,
        scope: scope || 'https://analysis.windows.net/powerbi/api/.default',
    });
    const tokenRes = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: '/' + FABRIC_TENANT_ID + '/oauth2/v2.0/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postBody) },
    }, postBody);
    const tokenData = JSON.parse(tokenRes.body);
    // Save new refresh token if returned
    if (tokenData.refresh_token) {
        const envPath = path.join(DIR, '.env');
        let env = fs.readFileSync(envPath, 'utf-8');
        env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + tokenData.refresh_token);
        fs.writeFileSync(envPath, env, 'utf-8');
    }
    return tokenData.access_token || null;
}

async function fabricDAX(token, query) {
    const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + ORACULO_WS_ID + '/datasets/' + ORACULO_DS_ID + '/executeQueries',
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    if (res.statusCode !== 200) return [];
    const data = JSON.parse(res.body);
    return (data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows) || [];
}

const ORACULO_PAINEIS_WS_ID = '63a65f3e-d96b-446e-a01d-f219132e1144';

async function fetchOraculoPainelStats() {
    if (!FABRIC_REFRESH_TOKEN || !FABRIC_TENANT_ID) return new Map();
    try {
        const token = await getFabricToken();
        if (!token) return new Map();

        // List all datasets in Oráculo painéis workspace
        const dsRes = await httpsRequest({
            hostname: 'api.powerbi.com',
            path: '/v1.0/myorg/groups/' + ORACULO_PAINEIS_WS_ID + '/datasets',
            method: 'GET',
            headers: { 'Authorization': 'Bearer ' + token },
        });
        if (dsRes.statusCode !== 200) { console.log('  WARN: Oráculo painéis list failed'); return new Map(); }
        const datasets = JSON.parse(dsRes.body).value || [];

        // For each dataset, query KPIs + counts
        const dax = "EVALUATE ROW(\"pedidos\", COUNTROWS('f_Pedidos Oraculo'), \"interacoes\", COUNTROWS('f_Interacoes Oraculo Semanal'), \"atendimentos\", [KPI Atendimentos Oraculo], \"pctIA\", [KPI % Atendimento Oraculo], \"vendas\", [KPI Vendas Totais])";
        const map = new Map(); // company name (lowercase) -> stats
        let ok = 0, fail = 0;
        for (const ds of datasets) {
            if (ds.name === 'Report Usage Metrics Model') continue;
            const name = ds.name.replace(' - Oráculo', '').trim();
            const body = JSON.stringify({ queries: [{ query: dax }], serializerSettings: { includeNulls: true } });
            try {
                const res = await httpsRequest({
                    hostname: 'api.powerbi.com',
                    path: '/v1.0/myorg/groups/' + ORACULO_PAINEIS_WS_ID + '/datasets/' + ds.id + '/executeQueries',
                    method: 'POST',
                    headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
                }, body);
                if (res.statusCode === 200) {
                    const val = JSON.parse(res.body).results?.[0]?.tables?.[0]?.rows?.[0] || {};
                    map.set(name.toLowerCase(), {
                        name,
                        pedidosOraculo: val['[pedidos]'] || 0,
                        interacoesOraculo: val['[interacoes]'] || 0,
                        atendimentosOraculo: val['[atendimentos]'] || 0,
                        pctIAOraculo: val['[pctIA]'] != null ? Math.round(val['[pctIA]'] * 1000) / 10 : 0,
                        vendasOraculo: val['[vendas]'] != null ? Math.round(val['[vendas]'] * 100) / 100 : 0,
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

async function fetchOraculoConfigurations() {
    if (!FABRIC_REFRESH_TOKEN || !FABRIC_TENANT_ID) {
        console.log('  SKIP: FABRIC tokens not set, skipping Oráculo Configurations');
        return new Map();
    }
    try {
        const token = await getFabricToken();
        if (!token) { console.log('  WARN: Failed to get Fabric token for Oráculo'); return new Map(); }

        const rows = await fabricDAX(token, `EVALUATE SELECTCOLUMNS(
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
        )`);

        const map = new Map();
        rows.forEach(r => {
            const companyId = r['[company_id]'] || '';
            if (companyId) {
                map.set(companyId, {
                    name: r['[name]'] || '',
                    domain_id: r['[domain_id]'] || '',
                    n8n_url: r['[n8n_url]'] || '',
                    phone: r['[phone_origin]'] || '',
                    created_at: r['[created_at]'] || '',
                    updated_at: r['[updated_at]'] || '',
                    link_report: r['[link_report]'] || '',
                    phone_by_vesti: r['[phone_by_vesti]'] === '1' || r['[phone_by_vesti]'] === 1,
                    catalogue_with_price: r['[catalogue_with_price]'] === '1' || r['[catalogue_with_price]'] === 1,
                    agent_retail: r['[agent_retail]'] === '1' || r['[agent_retail]'] === 1,
                    works_with_closed_square: r['[works_with_closed_square]'] === '1' || r['[works_with_closed_square]'] === 1,
                    keep_assigned_seller: r['[keep_assigned_seller]'] === '1' || r['[keep_assigned_seller]'] === 1,
                });
            }
        });
        console.log('  Oráculo configurations from Fabric: ' + map.size);
        return map;
    } catch (e) {
        console.log('  WARN: Oráculo config fetch failed: ' + e.message);
        return new Map();
    }
}

// ===================== EXCEL READER (Marcas e Planos) =====================
function readMarcasPlanos() {
    const filePath = path.join(DIR, 'Marcas e Planos.xlsx');
    if (!fs.existsSync(filePath)) {
        console.log('  SKIP: Marcas e Planos.xlsx not found, falling back to CSV');
        return null;
    }
    try {
        const wb = XLSX.readFile(filePath);
        const marcasMap = {}; // by CNPJ (clean numbers only)

        // Find the most recent sheets by extracting dates from names
        function extractSheetDate(name) {
            // Try patterns: "04-2026", "032026", "0126", "08-25"
            let m = name.match(/(\d{2})-?(\d{4})/);
            if (m) return m[2] + '-' + m[1]; // YYYY-MM
            m = name.match(/(\d{2})-?(\d{2})$/);
            if (m) return '20' + m[2] + '-' + m[1]; // 20YY-MM
            return '0000-00';
        }
        const vestiSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('vesti') && !s.toLowerCase().includes('starter'));
        const starterSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('starter'));

        // Sort by date descending and pick the most recent
        vestiSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));
        starterSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));

        const sheetsToRead = [];
        if (vestiSheets.length > 0) sheetsToRead.push(vestiSheets[0]); // most recent Vesti
        if (starterSheets.length > 0) sheetsToRead.push(starterSheets[0]); // most recent Starter
        if (sheetsToRead.length === 0) sheetsToRead.push(wb.SheetNames[0]);

        for (const sheetName of sheetsToRead) {
            const ws = wb.Sheets[sheetName];
            const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
            for (const row of rows) {
                const cnpjRaw = String(row['CPFCNPJ'] || row['CPF e CNPJ'] || '').replace(/[.\-\/\s]/g, '');
                if (!cnpjRaw || cnpjRaw.length < 11) continue;
                const marca = row['MARCA'] || '';
                marcasMap[cnpjRaw] = {
                    marca,
                    plano: row['PLANO'] || '',
                    setup: parseFloat(row['SETUP']) || 0,
                    mensalidadeInicial: parseFloat(row['MENSALIDADE INICIAL']) || 0,
                    valorReajuste: parseFloat(row['VALOR REAJUSTE']) || 0,
                    mensalidade: parseFloat(row['MENSALIDADE']) || 0,
                    integracao: parseFloat(row['INTEGRAÇÃO'] || row['INTEGRACAO']) || 0,
                    assistente: parseFloat(row['ASSISTENTE']) || 0,
                    filial: parseFloat(row['FILIAL']) || 0,
                    descontos: parseFloat(row['DESCONTOS']) || 0,
                    totalCobrado: parseFloat(row['TOTAL COBRADO'] || row['TOTAL_COBRADO']) || 0,
                    observacoes: row['OBSERVAÇÕES'] || row['OBSERVACOES'] || '',
                    canal: row['CANAL'] || row['CANAL/Agência'] || '',
                    subconta: row['Subconta'] || '',
                };
            }
            console.log('  Excel sheet "' + sheetName + '": ' + rows.length + ' rows');
        }
        console.log('  Marcas e Planos (Excel): ' + Object.keys(marcasMap).length + ' unique CNPJs');
        return marcasMap;
    } catch (e) {
        console.log('  WARN: Excel read failed: ' + e.message + ', falling back to CSV');
        return null;
    }
}

// ===================== MAIN =====================
async function main() {
    console.log('Aggregating data...\n');

    // 1. Cadastros Empresas - base company data
    const empresasMap = {}; // keyed by Id Empresa
    const empresasByDominio = {}; // keyed by Id Dominio
    await readCSV('Cadastros Empresas.csv', (row) => {
        const id = row['Id Empresa'];
        if (!id) return;
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
            criacao: row['Criação do Dominio'] || '',
            tipoIntegracao: row['Domains.integration_type'] || '',
            dataPrimeiroPedido: row['Data do Primeiro Pedido VESTIPAGO'] || '',
            valorPlano: parseFloat(row['Valor Cobrado Plano']) || 0,
            // Will be aggregated
            transCartao: 0, transPix: 0, transTotal: 0,
            valCartao: 0, valPix: 0, valTotal: 0,
            pedidos: 0, pedidosPagos: 0, pedidosCancelados: 0, pedidosPendentes: 0,
            valPedidosPagos: 0, valPedidosCancelados: 0, valPedidosPendentes: 0,
            linksEnviados: 0, cliques: 0,
            cartaoImpl: false, pixImpl: false,
            marcaImpl: '', marcaConfig: '',
        };
        if (row['Id Dominio']) {
            empresasByDominio[row['Id Dominio']] = empresasMap[id];
        }
    });
    console.log('  Companies loaded: ' + Object.keys(empresasMap).length);

    // 2. Config Empresas - card/pix implementation
    await readCSV('Config Empresas.csv', (row) => {
        const companyId = row['docs.companyId'];
        if (companyId && empresasMap[companyId]) {
            empresasMap[companyId].cartaoImpl = row['docs.creditCard.isEnabled'] === 'True';
            empresasMap[companyId].pixImpl = row['docs.pix.isEnabled'] === 'True';
        }
    });

    // 3. Merged Pedidos - aggregate orders per company AND per company+month
    const pedidosMensais = {}; // global {mes: {...}}
    const pedidosMensaisEmp = {}; // per company {companyId: {mes: {...}}}
    await readCSV('Merged Pedidos.csv', (row) => {
        const empresaId = row['ID Empresa'];
        const domId = row['ID Dominio'];
        const emp = empresasMap[empresaId] || empresasByDominio[domId];
        if (!emp) return;

        const total = parseFloat(row['Total']) || 0;
        const isPago = row['Pago'] === 'True';
        const isCancelado = row['Cancelado'] === 'True';
        const isPendente = row['Pendente'] === 'True';
        const method = (row['docs.payment.method'] || '').toLowerCase();
        const isCartao = method.includes('credit') || method.includes('card') || method.includes('cartao') || method.includes('credito');
        const isPix = method.includes('pix');

        emp.pedidos++;
        if (isPago) { emp.pedidosPagos++; emp.valPedidosPagos += total; }
        if (isCancelado) { emp.pedidosCancelados++; emp.valPedidosCancelados += total; }
        if (isPendente) { emp.pedidosPendentes++; emp.valPedidosPendentes += total; }

        if (isCartao) { emp.transCartao++; emp.valCartao += total; }
        else if (isPix) { emp.transPix++; emp.valPix += total; }
        emp.transTotal++;
        emp.valTotal += total;

        // Monthly aggregation (global + per company)
        const dataCriacao = row['Data Criacao'] || '';
        const match = dataCriacao.match(/(\d{4})-(\d{2})/);
        if (match) {
            const mesKey = match[1] + '-' + match[2];
            // Global
            if (!pedidosMensais[mesKey]) pedidosMensais[mesKey] = { cartao: 0, pix: 0, total: 0, valCartao: 0, valPix: 0, valTotal: 0, pagos: 0, cancelados: 0, pendentes: 0, valPagos: 0 };
            const m = pedidosMensais[mesKey];
            m.total++;
            if (isCartao) { m.cartao++; m.valCartao += total; }
            else if (isPix) { m.pix++; m.valPix += total; }
            m.valTotal += total;
            if (isPago) { m.pagos++; m.valPagos += total; }
            if (isCancelado) m.cancelados++;
            if (isPendente) m.pendentes++;
            // Per company
            if (!pedidosMensaisEmp[emp.id]) pedidosMensaisEmp[emp.id] = {};
            if (!pedidosMensaisEmp[emp.id][mesKey]) pedidosMensaisEmp[emp.id][mesKey] = { pedidos: 0, pagos: 0, canc: 0, pend: 0, valT: 0, valPag: 0, valCanc: 0, valPend: 0, tC: 0, tP: 0, vC: 0, vP: 0 };
            const pm = pedidosMensaisEmp[emp.id][mesKey];
            pm.pedidos++; pm.valT += total;
            if (isPago) { pm.pagos++; pm.valPag += total; }
            if (isCancelado) { pm.canc++; pm.valCanc += total; }
            if (isPendente) { pm.pend++; pm.valPend += total; }
            if (isCartao) { pm.tC++; pm.vC += total; }
            else if (isPix) { pm.tP++; pm.vP += total; }
        }
    });

    // 3b. Filtrar outliers de GMV (ticket médio > R$ 500.000 = dado corrompido)
    const MAX_TICKET = 500000;
    for (const emp of Object.values(empresasMap)) {
        if (emp.pedidos > 0 && emp.valTotal / emp.pedidos > MAX_TICKET) {
            console.log('  WARN: Outlier detectado - ' + (emp.nomeFantasia || emp.nomeDominio) + ' (ticket médio R$ ' + Math.round(emp.valTotal / emp.pedidos) + ')');
            emp.valTotal = 0; emp.valPedidosPagos = 0; emp.valPedidosCancelados = 0; emp.valPedidosPendentes = 0;
            emp.valCartao = 0; emp.valPix = 0;
        }
        // Outliers mensais
        const empM = pedidosMensaisEmp[emp.id];
        if (empM) {
            for (const [mes, pm] of Object.entries(empM)) {
                if (pm.pedidos > 0 && pm.valT / pm.pedidos > MAX_TICKET) {
                    pm.valT = 0; pm.valPag = 0; pm.valCanc = 0; pm.valPend = 0; pm.vC = 0; pm.vP = 0;
                }
            }
        }
    }

    // 4. Product - count links sent per company AND per company+month
    // Deduplicate by domain: vendedores are duplicated across filiais sharing same domain
    // Canonical = the matriz of the group (determined later, but we can use Union-Find root for now)
    // For domains with multiple companies, all links go to ONE canonical ID to avoid double counting
    const domainCanonical = {}; // domainId -> canonical companyId
    // First pass: for each domain, find which company is in matrizIds (computed later)
    // Since matrizIds is computed after this section, we need to move dedup to after matriz detection
    // ALTERNATIVE: just use domain-based dedup by picking consistent canonical per domain
    // We'll pick by: highest pedidos count within the domain group
    const domainCompanies = {}; // domainId -> [empresas]
    Object.values(empresasMap).forEach(e => {
        if (e.idDominio && e.id) {
            if (!domainCompanies[e.idDominio]) domainCompanies[e.idDominio] = [];
            domainCompanies[e.idDominio].push(e);
        }
    });
    Object.entries(domainCompanies).forEach(([dom, emps]) => {
        if (emps.length === 1) {
            domainCanonical[dom] = emps[0].id;
        } else {
            // Pick the one with most pedidos as canonical (likely the matriz)
            emps.sort((a, b) => (b.pedidos || 0) - (a.pedidos || 0));
            domainCanonical[dom] = emps[0].id;
        }
    });
    // Map: companyId -> canonical companyId
    const canonicalCompany = {};
    Object.values(empresasMap).forEach(e => {
        if (e.id && e.idDominio && domainCanonical[e.idDominio]) {
            canonicalCompany[e.id] = domainCanonical[e.idDominio];
        } else if (e.id) {
            canonicalCompany[e.id] = e.id;
        }
    });

    const linksByCompany = {};
    const linksMensais = {};
    const linksMensaisEmp = {}; // {canonicalCompanyId: {mes: count}}
    await readCSV('Product.csv', (row) => {
        const companyId = row['Cadastros Users ( Vendedores ).CompanyId'];
        if (!companyId) return;
        // Map to canonical company to avoid double counting across filiais
        const canonical = canonicalCompany[companyId] || companyId;
        linksByCompany[canonical] = (linksByCompany[canonical] || 0) + 1;
        const dt = row['product_sent_lists.created_at'] || '';
        const match = dt.match(/(\d{4})-(\d{2})/);
        if (match) {
            const mk = match[1] + '-' + match[2];
            linksMensais[mk] = (linksMensais[mk] || 0) + 1;
            if (!linksMensaisEmp[canonical]) linksMensaisEmp[canonical] = {};
            linksMensaisEmp[canonical][mk] = (linksMensaisEmp[canonical][mk] || 0) + 1;
        }
    });
    for (const [cid, count] of Object.entries(linksByCompany)) {
        if (empresasMap[cid]) empresasMap[cid].linksEnviados = count;
    }

    // 5. Rankings - sum shared_links per company AND per company+month (same dedup logic)
    const cliquesByCompany = {};
    const cliquesMensais = {};
    const cliquesMensaisEmp = {}; // {canonicalCompanyId: {mes: count}}
    await readCSV('Rankings.csv', (row) => {
        const companyId = row['Cadastros Users ( Vendedores ).CompanyId'];
        if (!companyId) return;
        const canonical = canonicalCompany[companyId] || companyId;
        const links = parseInt(row['rankings.shared_links']) || 0;
        cliquesByCompany[canonical] = (cliquesByCompany[canonical] || 0) + links;
        const dt = row['rankings.created_at'] || '';
        const match = dt.match(/(\d{4})-(\d{2})/);
        if (match) {
            const mk = match[1] + '-' + match[2];
            cliquesMensais[mk] = (cliquesMensais[mk] || 0) + links;
            if (!cliquesMensaisEmp[canonical]) cliquesMensaisEmp[canonical] = {};
            cliquesMensaisEmp[canonical][mk] = (cliquesMensaisEmp[canonical][mk] || 0) + links;
        }
    });
    for (const [cid, count] of Object.entries(cliquesByCompany)) {
        if (empresasMap[cid]) empresasMap[cid].cliques = count;
    }

    // 6. Marcas e Planos (prefer Excel, fallback to CSV)
    let marcasMap = readMarcasPlanos();
    if (!marcasMap) {
        marcasMap = {};
        await readCSV('Marcas e Planos.csv', (row) => {
            const cnpj = row['CPFCNPJ'] || '';
            if (cnpj) marcasMap[cnpj] = { marca: row['MARCA'], plano: row['PLANO'], totalCobrado: parseFloat(row['TOTAL_COBRADO']) || 0, mensalidade: parseFloat(row['TOTAL_COBRADO']) || 0, integracao: 0, assistente: 0, filial: 0, descontos: 0, setup: 0, mensalidadeInicial: 0, valorReajuste: 0, observacoes: '', canal: '', subconta: '' };
        });
    }

    // 7. Controle Geral Luana - etapa hub, mensalidade, oráculo, pedidos mensais
    const controleMap = {}; // by companyId
    const controleByNome = {}; // by marca name (lowercase)
    await readCSV('controle_geral_luana_csv.csv', (row) => {
        const companyId = row['Company*ID'] || row['CompanyID'] || '';
        const marca = row['MARCAS'] || '';
        const entry = {
            marca,
            companyId,
            usuario: row['Usuário'] || row['Usuario'] || '',
            etapaHub: row['ETAPA HUB'] || '',
            mensalidade: row['MENSALIDADE'] || '',
            gmvControle: row['GMV'] || '',
            filial: row['FILIAL'] || '',
            oraculo: row['ORÁCULO'] || row['ORACULO'] || '',
            pix: row['PIX'] || '',
            cc: row['CC'] || '',
            frete: row['FRETE'] || '',
            jan: parseInt(row['JAN']) || 0,
            fev: parseInt(row['FEV']) || 0,
            mar: parseInt(row['MAR']) || 0,
            naoPagos: parseInt(row['NÃO PAGOS'] || row['NAO PAGOS']) || 0,
        };
        if (companyId) controleMap[companyId] = entry;
        if (marca) controleByNome[marca.toLowerCase().trim()] = entry;
    });
    console.log('  Controle Luana loaded: ' + Object.keys(controleMap).length + ' companies');

    // 8. HubSpot Oráculo tickets - with fuzzy matching
    const oraculoTickets = await fetchOraculoTickets();

    // 9. VestiPago companies from Fabric
    const vestiPagoSet = await fetchVestiPagoCompanies();

    // 10. Oráculo configurations from Fabric (Oraculo_configurations INNER JOIN company_id WHERE n8n_url IS NOT NULL)
    const oraculoConfigMap = await fetchOraculoConfigurations();

    // 10b. Oráculo painéis stats (pedidos + interações por empresa)
    const oraculoPainelStats = await fetchOraculoPainelStats();

    // 11. Build filiais map using two strategies:
    //   a) Same CNPJ root (first 8 digits) = same legal entity, different branches
    //   b) Same Domínio ID = same domain, different sub-companies
    // Use Union-Find to merge groups from both strategies

    // Union-Find
    const parent = {};
    function find(x) { if (parent[x] !== x) parent[x] = find(parent[x]); return parent[x]; }
    function union(a, b) { const pa = find(a), pb = find(b); if (pa !== pb) parent[pa] = pb; }

    const allEmpIds = Object.values(empresasMap).filter(e => e.id).map(e => e.id);
    allEmpIds.forEach(id => { parent[id] = id; });

    // Strategy A: group by CNPJ root (first 8 digits, only for valid 14-digit CNPJs)
    const cnpjRootMap = {};
    Object.values(empresasMap).forEach(e => {
        const clean = (e.cnpj || '').replace(/[.\-\/]/g, '');
        if (clean.length >= 14 && e.id) {
            const root = clean.substring(0, 8);
            if (!cnpjRootMap[root]) cnpjRootMap[root] = [];
            cnpjRootMap[root].push(e.id);
        }
    });
    Object.values(cnpjRootMap).forEach(ids => {
        if (ids.length > 1 && ids.length <= 20) { // skip if too many (likely bad data)
            for (let i = 1; i < ids.length; i++) union(ids[0], ids[i]);
        }
    });

    // Strategy B: group by Domínio ID (skip very large groups which are test/shared domains)
    const domIdMap = {};
    Object.values(empresasMap).forEach(e => {
        if (e.idDominio && e.id) {
            if (!domIdMap[e.idDominio]) domIdMap[e.idDominio] = [];
            domIdMap[e.idDominio].push(e.id);
        }
    });
    Object.values(domIdMap).forEach(ids => {
        if (ids.length > 1 && ids.length <= 15) { // skip shared test domains (>15 = likely test)
            for (let i = 1; i < ids.length; i++) union(ids[0], ids[i]);
        }
    });

    // Build final groups: parentId -> [empresas in group]
    const filialGroups = {};
    Object.values(empresasMap).forEach(e => {
        if (!e.id) return;
        const root = find(e.id);
        if (!filialGroups[root]) filialGroups[root] = [];
        filialGroups[root].push(e);
    });
    const groupsWithFiliais = Object.values(filialGroups).filter(g => g.length > 1);
    console.log('  Filial groups detected: ' + groupsWithFiliais.length + ' (total companies in groups: ' + groupsWithFiliais.reduce((s, g) => s + g.length, 0) + ')');

    // Identify matriz in each group:
    // 1. Unique CNPJ root (first 8 digits) with branch 0001 and highest orders = legal headquarters
    // 2. If multiple share the same CNPJ, pick the one with most orders
    // 3. Tiebreaker: shorter name (matriz usually has no location suffix)
    const matrizIds = new Set();
    for (const group of groupsWithFiliais) {
        const withInfo = group.map(e => {
            const clean = (e.cnpj || '').replace(/[.\-\/]/g, '');
            const branchNum = clean.length >= 12 ? clean.substring(8, 12) : '9999';
            const cnpjRoot = clean.length >= 8 ? clean.substring(0, 8) : '';
            const nome = e.nomeFantasia || e.nomeDominio || '';
            return { emp: e, branchNum, cnpjRoot, orders: e.pedidos || 0, nameLen: nome.length };
        });
        // Count how many unique CNPJ roots have branch 0001
        const roots0001 = new Set(withInfo.filter(x => x.branchNum === '0001').map(x => x.cnpjRoot));
        // Sort: branch 0001 with unique root first, then by orders desc, then shorter name
        withInfo.sort((a, b) => {
            const aIs0001 = a.branchNum === '0001';
            const bIs0001 = b.branchNum === '0001';
            // If only one has 0001 with a unique root, prioritize it
            if (aIs0001 && !bIs0001 && roots0001.size === 1) return -1;
            if (bIs0001 && !aIs0001 && roots0001.size === 1) return 1;
            // By orders descending
            if (b.orders !== a.orders) return b.orders - a.orders;
            // Shorter name (matriz usually has simpler name)
            return a.nameLen - b.nameLen;
        });
        matrizIds.add(withInfo[0].emp.id);
    }

    // Normalize a name for matching: lowercase, remove accents, remove common suffixes/noise
    function normalize(s) {
        return (s || '').toLowerCase()
            .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // remove accents
            .replace(/\s*(jeans|modas|moda|confeccoes|confecções|oficial|clothing|collection|acessorios|acessórios|tricot|ltda|me|eireli|s\.a\.|sa)\s*/gi, ' ')
            .replace(/[^a-z0-9]/g, ' ') // remove special chars
            .replace(/\s+/g, ' ').trim();
    }

    // Build empresa lookup structures for matching
    const allEmpresas = Object.values(empresasMap).filter(e => e.nomeFantasia || e.nomeDominio);
    const empLookup = {}; // normalized name -> empresa
    const empWords = {};  // each word of 3+ chars -> [empresas]
    allEmpresas.forEach(e => {
        const nome = e.nomeFantasia || e.nomeDominio;
        const n = normalize(nome);
        empLookup[n] = e;
        // Also index by each significant word
        n.split(' ').filter(w => w.length >= 3).forEach(w => {
            if (!empWords[w]) empWords[w] = [];
            empWords[w].push({ emp: e, nome });
        });
    });

    // Match ticket to empresa using multiple strategies
    function matchTicketToEmpresa(ticket) {
        const tn = normalize(ticket.companyName);
        if (!tn || tn === 'oraculo' || tn === 'eventos') return null;

        // 1. Exact normalized match
        if (empLookup[tn]) return empLookup[tn];

        // 2. Check if ticket name is contained in any empresa name or vice-versa
        for (const [en, emp] of Object.entries(empLookup)) {
            if (en.includes(tn) || tn.includes(en)) return emp;
        }

        // 3. Word-based scoring: how many words from ticket match empresa words
        const ticketWords = tn.split(' ').filter(w => w.length >= 3);
        if (ticketWords.length === 0) return null;

        let bestMatch = null, bestScore = 0;
        const candidates = new Map();
        ticketWords.forEach(tw => {
            // Check exact word match and prefix match (3+ chars)
            for (const [word, emps] of Object.entries(empWords)) {
                if (word === tw || word.startsWith(tw) || tw.startsWith(word)) {
                    emps.forEach(({ emp, nome }) => {
                        const key = emp.id;
                        const prev = candidates.get(key) || { emp, nome, score: 0 };
                        prev.score += (word === tw) ? 2 : 1;
                        candidates.set(key, prev);
                    });
                }
            }
        });

        for (const [, c] of candidates) {
            if (c.score > bestScore) { bestScore = c.score; bestMatch = c.emp; }
        }

        // Only accept if score is decent (at least one strong match)
        return bestScore >= 2 ? bestMatch : null;
    }

    // Build map: empresa id -> most recent oraculo ticket
    const oraculoByEmpId = {};
    let oraculoMatched = 0, oraculoUnmatched = 0;
    for (const t of oraculoTickets) {
        const emp = matchTicketToEmpresa(t);
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

    // Collect all months that appear across all data sources
    const allMonthsSet = new Set([
        ...Object.keys(pedidosMensais),
        ...Object.keys(linksMensais),
        ...Object.keys(cliquesMensais),
    ]);
    const allMonths = [...allMonthsSet].sort();

    // Build final empresa list with per-company monthly data
    let empIndex = 0;
    const empresasList = Object.values(empresasMap)
        .filter(e => e.nomeFantasia || e.nomeDominio)
        .map(e => {
            const cnpjNum = e.cnpj.replace(/[.\-\/]/g, '');
            let marca = marcasMap[cnpjNum];
            // Fallback: match by company name if CNPJ didn't match
            if (!marca) {
                const nomeEmp = normalize(e.nomeFantasia || e.nomeDominio || '');
                for (const [, m] of Object.entries(marcasMap)) {
                    if (m.marca && normalize(m.marca) === nomeEmp) { marca = m; break; }
                }
                // Partial match: empresa name contains marca name or vice-versa
                if (!marca) {
                    for (const [, m] of Object.entries(marcasMap)) {
                        const nMarca = normalize(m.marca || '');
                        if (nMarca && nomeEmp && nMarca.length >= 3 && (nomeEmp.includes(nMarca) || nMarca.includes(nomeEmp))) { marca = m; break; }
                    }
                }
            }
            const idx = empIndex++;

            // Build sparse monthly arrays for this company
            // pedidos monthly: {mes: {pedidos, pagos, canc, pend, valT, valPag, valCanc, valPend, tC, tP, vC, vP}}
            const pm = pedidosMensaisEmp[e.id] || {};
            const lm = linksMensaisEmp[e.id] || {};
            const cm = cliquesMensaisEmp[e.id] || {};

            // Only store months that have data (sparse)
            const mData = {};
            for (const mes of allMonths) {
                const p = pm[mes]; const l = lm[mes]; const c = cm[mes];
                if (p || l || c) {
                    mData[mes] = [
                        p ? p.pedidos : 0, p ? p.pagos : 0, p ? p.canc : 0, p ? p.pend : 0,
                        p ? Math.round(p.valT) : 0, p ? Math.round(p.valPag) : 0,
                        p ? Math.round(p.valCanc) : 0, p ? Math.round(p.valPend) : 0,
                        p ? p.tC : 0, p ? p.tP : 0,
                        p ? Math.round(p.vC) : 0, p ? Math.round(p.vP) : 0,
                        l || 0, c || 0,
                    ];
                }
            }

            // Match with controle_geral_luana data
            const nome = e.nomeFantasia || e.nomeDominio;
            const ctrl = controleMap[e.id] || controleByNome[(nome || '').toLowerCase().trim()];

            // Match with Oráculo HubSpot ticket (by empresa id)
            const oracTkt = oraculoByEmpId[e.id];

            // Mensalidade: from controle first, fallback to marcas e planos
            let mensalidade = '';
            if (ctrl && ctrl.mensalidade) {
                mensalidade = ctrl.mensalidade;
            } else if (marca && marca.totalCobrado) {
                mensalidade = 'R$ ' + marca.totalCobrado.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            }

            // Etapa HubSpot (from controle_geral_luana)
            const etapaHub = ctrl ? ctrl.etapaHub : '';

            // Oráculo: stage from HubSpot API (live), fallback to controle
            let oraculoEtapa = '';
            if (oracTkt) {
                oraculoEtapa = oracTkt.stageName;
            } else if (ctrl && ctrl.oraculo) {
                oraculoEtapa = ctrl.oraculo;
            }

            // Previsão de churn - score simples baseado em sinais
            let churnScore = 0;
            let churnMotivos = [];
            // 1. Queda de pedidos (últimos 3 meses vs 3 anteriores)
            const sortedMeses = Object.keys(mData).sort();
            if (sortedMeses.length >= 4) {
                const recent3 = sortedMeses.slice(-3);
                const prev3 = sortedMeses.slice(-6, -3);
                const sumRecent = recent3.reduce((s, m) => s + (mData[m] ? mData[m][0] : 0), 0);
                const sumPrev = prev3.reduce((s, m) => s + (mData[m] ? mData[m][0] : 0), 0);
                if (sumPrev > 0 && sumRecent < sumPrev * 0.5) { churnScore += 30; churnMotivos.push('Queda >50% pedidos'); }
                else if (sumPrev > 0 && sumRecent < sumPrev * 0.7) { churnScore += 15; churnMotivos.push('Queda >30% pedidos'); }
            }
            // 2. Zero pedidos no último mês
            if (sortedMeses.length > 0) {
                const lastMonth = mData[sortedMeses[sortedMeses.length - 1]];
                if (lastMonth && lastMonth[0] === 0) { churnScore += 25; churnMotivos.push('Zero pedidos mês atual'); }
            }
            // 3. Muitos cancelados vs pagos
            if (e.pedidos > 10 && e.pedidosCancelados > e.pedidosPagos * 0.3) { churnScore += 15; churnMotivos.push('Alto cancelamento'); }
            // 4. Sem integração
            if (e.temIntegracao !== 'Sim') { churnScore += 10; churnMotivos.push('Sem integração'); }
            // 5. Oráculo em Churn ou Parado
            if (oraculoEtapa === 'Churn') { churnScore += 30; churnMotivos.push('Oráculo: Churn'); }
            else if (oraculoEtapa === 'Parado') { churnScore += 20; churnMotivos.push('Oráculo: Parado'); }
            // Cap at 100
            churnScore = Math.min(churnScore, 100);
            const churnRisco = churnScore >= 60 ? 'Alto' : churnScore >= 30 ? 'Médio' : 'Baixo';

            // VestiPago: vem do dataset Fabric "VestiPago - Vendas por empresa"
            const temVestiPago = vestiPagoSet.has(e.id);

            // Oráculo from Fabric (Oraculo_configurations with n8n_url NOT NULL)
            const oraculoConfig = oraculoConfigMap.get(e.id) || null;
            // Oráculo painéis stats (fuzzy match by name)
            let oraculoStats = oraculoPainelStats.get(nome.toLowerCase()) || null;
            if (!oraculoStats && oraculoConfig) {
                // Try matching: Oráculo config name -> painel name
                const ocName = (oraculoConfig.name || '').toLowerCase().replace(/^churn\s*-\s*/i, '').replace(/^chrun\s*-\s*/i, '').trim();
                if (ocName) oraculoStats = oraculoPainelStats.get(ocName) || null;
                // Try partial: empresa name contains painel name or vice-versa
                if (!oraculoStats) {
                    const nNorm = normalize(nome);
                    for (const [pName, pStats] of oraculoPainelStats) {
                        const pNorm = normalize(pName);
                        if (pNorm && nNorm && (nNorm.includes(pNorm) || pNorm.includes(nNorm))) {
                            oraculoStats = pStats;
                            break;
                        }
                    }
                }
            }

            // Filiais: find other companies in the same group (by CNPJ root or domain ID)
            const groupRoot = find(e.id);
            const filiaisGroup = filialGroups[groupRoot] || [];
            const isMatriz = matrizIds.has(e.id);
            // Find the matriz of this group
            const matrizEmp = filiaisGroup.find(f => matrizIds.has(f.id));
            const matrizId = matrizEmp ? matrizEmp.id : e.id;
            const filiais = filiaisGroup
                .filter(f => f.id !== e.id)
                .map(f => {
                    const fIsMatriz = matrizIds.has(f.id);
                    return {
                        nome: f.nomeFantasia || f.nomeDominio,
                        idDominio: f.idDominio,
                        id: f.id,
                        temVestiPago: vestiPagoSet.has(f.id),
                        isMatriz: fIsMatriz,
                    };
                })
                // Sort: matriz first, then alphabetically
                .sort((a, b) => {
                    if (a.isMatriz && !b.isMatriz) return -1;
                    if (!a.isMatriz && b.isMatriz) return 1;
                    return a.nome.localeCompare(b.nome);
                });

            return {
                i: idx,
                id: e.id,
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
                anjo: e.anjo,
                modulo: e.modulo,
                tags: e.tags,
                temIntegracao: e.temIntegracao,
                integracao: e.integracao,
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
                marcaAtiva: e.transCartao >= 250 ? 'Sim' : 'Não',
                // New fields
                mensalidade,
                etapaHub,
                oraculoEtapa,
                churnScore,
                churnRisco,
                churnMotivos: churnMotivos.length > 0 ? churnMotivos.join('; ') : '',
                // Controle Geral Luana fields
                usuario: ctrl ? ctrl.usuario : '',
                naoPagos: ctrl ? ctrl.naoPagos : 0,
                filial: ctrl ? ctrl.filial : '',
                pixOraculo: ctrl ? ctrl.pix : '',
                ccOraculo: ctrl ? ctrl.cc : '',
                freteOraculo: ctrl ? ctrl.frete : '',
                pedJan: ctrl ? ctrl.jan : 0,
                pedFev: ctrl ? ctrl.fev : 0,
                pedMar: ctrl ? ctrl.mar : 0,
                // Oráculo Fabric - só tem Oráculo se tem painel no workspace
                temOraculoFabric: !!oraculoStats,
                oraculoFabric: oraculoStats ? {
                    ...(oraculoConfig || {}),
                    pedidosOraculo: oraculoStats.pedidosOraculo,
                    interacoesOraculo: oraculoStats.interacoesOraculo,
                    atendimentosOraculo: oraculoStats.atendimentosOraculo,
                    pctIAOraculo: oraculoStats.pctIAOraculo,
                    vendasOraculo: oraculoStats.vendasOraculo,
                } : undefined,
                // Filiais (empresas com mesmo CNPJ raiz ou domínio ID)
                isMatriz: filiaisGroup.length > 1 ? isMatriz : undefined,
                matrizId: filiaisGroup.length > 1 && !isMatriz ? matrizId : undefined,
                filiais: filiais.length > 0 ? filiais : undefined,
                idDominio: e.idDominio,
                m: Object.keys(mData).length > 0 ? mData : undefined,
            };
        });

    // Global monthly data
    const sortedMonths = Object.keys(pedidosMensais).sort();
    const recentMonths = sortedMonths.slice(-18);

    const monthlyData = recentMonths.map(m => ({
        mes: m,
        ...pedidosMensais[m],
        links: linksMensais[m] || 0,
        cliques: cliquesMensais[m] || 0,
    }));

    // Oráculo summary for dashboard
    const oraculoSummary = {};
    for (const t of oraculoTickets) {
        oraculoSummary[t.stageName] = (oraculoSummary[t.stageName] || 0) + 1;
    }

    // Churn stats
    const churnAlto = empresasList.filter(e => e.churnRisco === 'Alto').length;
    const churnMedio = empresasList.filter(e => e.churnRisco === 'Médio').length;

    // Output
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

    const jsonStr = JSON.stringify(output);
    const jsContent = 'const DADOS = ' + jsonStr + ';';
    fs.writeFileSync(path.join(DIR, 'dados.js'), jsContent, 'utf-8');

    console.log('\n=== RESULT ===');
    console.log('Empresas: ' + empresasList.length);
    console.log('Meses: ' + monthlyData.length);
    console.log('Output: dados.js (' + (jsContent.length / 1024).toFixed(0) + ' KB)');

    // Quick stats
    const totalGMV = empresasList.reduce((s, e) => s + e.gmv, 0);
    const totalPedidos = empresasList.reduce((s, e) => s + e.pedidos, 0);
    const totalLinks = empresasList.reduce((s, e) => s + e.linksEnviados, 0);
    console.log('Total GMV: R$ ' + totalGMV.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
    console.log('Total Pedidos: ' + totalPedidos.toLocaleString('pt-BR'));
    console.log('Total Links: ' + totalLinks.toLocaleString('pt-BR'));
}

main().catch(err => { console.error(err); process.exit(1); });
