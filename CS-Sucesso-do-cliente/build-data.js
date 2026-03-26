/**
 * Script para agregar dados dos CSVs do Power BI e gerar dados.js para o dashboard.
 * Inclui dados do HubSpot (Oráculo) e controle_geral_luana.
 * Executa com: node build-data.js
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const https = require('https');

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
const VP_WORKSPACE_ID = 'f80301c2-8735-40d2-8662-1f8a627d3f61';
const VP_DATASET_ID = '606be0ee-2c8c-4f43-8ad6-0be04f95d616';
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
    const linksByCompany = {};
    const linksMensais = {};
    const linksMensaisEmp = {}; // {companyId: {mes: count}}
    await readCSV('Product.csv', (row) => {
        const companyId = row['Cadastros Users ( Vendedores ).CompanyId'];
        if (!companyId) return;
        linksByCompany[companyId] = (linksByCompany[companyId] || 0) + 1;
        const dt = row['product_sent_lists.created_at'] || '';
        const match = dt.match(/(\d{4})-(\d{2})/);
        if (match) {
            const mk = match[1] + '-' + match[2];
            linksMensais[mk] = (linksMensais[mk] || 0) + 1;
            if (!linksMensaisEmp[companyId]) linksMensaisEmp[companyId] = {};
            linksMensaisEmp[companyId][mk] = (linksMensaisEmp[companyId][mk] || 0) + 1;
        }
    });
    for (const [cid, count] of Object.entries(linksByCompany)) {
        if (empresasMap[cid]) empresasMap[cid].linksEnviados = count;
    }

    // 5. Rankings - sum shared_links per company AND per company+month
    const cliquesByCompany = {};
    const cliquesMensais = {};
    const cliquesMensaisEmp = {}; // {companyId: {mes: count}}
    await readCSV('Rankings.csv', (row) => {
        const companyId = row['Cadastros Users ( Vendedores ).CompanyId'];
        if (!companyId) return;
        const links = parseInt(row['rankings.shared_links']) || 0;
        cliquesByCompany[companyId] = (cliquesByCompany[companyId] || 0) + links;
        const dt = row['rankings.created_at'] || '';
        const match = dt.match(/(\d{4})-(\d{2})/);
        if (match) {
            const mk = match[1] + '-' + match[2];
            cliquesMensais[mk] = (cliquesMensais[mk] || 0) + links;
            if (!cliquesMensaisEmp[companyId]) cliquesMensaisEmp[companyId] = {};
            cliquesMensaisEmp[companyId][mk] = (cliquesMensaisEmp[companyId][mk] || 0) + links;
        }
    });
    for (const [cid, count] of Object.entries(cliquesByCompany)) {
        if (empresasMap[cid]) empresasMap[cid].cliques = count;
    }

    // 6. Marcas e Planos
    const marcasMap = {}; // by CNPJ
    await readCSV('Marcas e Planos.csv', (row) => {
        const cnpj = row['CPFCNPJ'] || '';
        if (cnpj) marcasMap[cnpj] = { marca: row['MARCA'], plano: row['PLANO'], totalCobrado: parseFloat(row['TOTAL_COBRADO']) || 0 };
    });

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
            const marca = marcasMap[cnpjNum];
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

            return {
                i: idx,
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
                tipoIntegracao: e.tipoIntegracao,
                criacao: e.criacao,
                valorPlano: e.valorPlano,
                plano: marca ? marca.plano : '',
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
