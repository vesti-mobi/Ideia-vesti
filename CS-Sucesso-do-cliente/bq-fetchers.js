/**
 * Fetchers BigQuery + helpers para o CS Dashboard (migração Fabric → BQ).
 * Consumido por build-cloud-bq.js. Fonte: vesti-data-499015.vestilake_BI.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const { BigQuery } = require('@google-cloud/bigquery');

const DIR = __dirname;
const BQ_PROJECT = 'vesti-data-499015';
const BQ_DATASET = 'vestilake_BI';
const DS = `\`${BQ_PROJECT}.${BQ_DATASET}\``;
const _bq = new BigQuery({ projectId: BQ_PROJECT });
async function bqQuery(sql, label) {
    if (label) console.log(`  [bq] ${label}...`);
    const [rows] = await _bq.query({ query: sql, location: 'us-central1' });
    if (label) console.log(`  [bq] ${label}: ${rows.length} rows`);
    return rows;
}
const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
const digits = (s) => (s || '').replace(/\D/g, '');
const tsStr = (v) => v == null ? '' : String(v.value != null ? v.value : v);

// ===================== HubSpot Oráculo tickets =====================
const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN || '';
const ORACULO_PIPELINE_ID = '794686264';
const ORACULO_STAGES = {
    '1165541427':'Fila','1165361278':'Grupo de Implementação','1165350737':'Reunião 1',
    '1165350738':'Configurações Iniciais','1273974154':'Link de relatório',
    '1199622545':'Problema conta Meta ou YCloud','1180878228':'Acompanhamento e melhorias prompt',
    '1165350742':'Eventos Vesti','1216864772':'Agente Aquecimento de leads',
    '1204236378':'Integração','1183765142':'Agente Inativos','1269319857':'Campanhas',
    '1165361281':'Concluído','1238455699':'Parado','1249275660':'Churn'
};
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
function hubspotRequest(endpoint, method, body) {
    const bodyStr = body ? JSON.stringify(body) : null;
    return httpsRequest({
        hostname: 'api.hubapi.com', path: endpoint, method,
        headers: { 'Authorization': 'Bearer ' + HUBSPOT_TOKEN, 'Content-Type': 'application/json',
            ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}) },
    }, bodyStr).then(res => JSON.parse(res.body));
}
async function fetchOraculoTickets() {
    console.log('  Fetching HubSpot Oráculo tickets...');
    if (!HUBSPOT_TOKEN) { console.log('  WARN: HUBSPOT_TOKEN not set, skipping HubSpot'); return []; }
    try {
        const allTickets = []; let after = 0; let hasMore = true;
        while (hasMore) {
            const body = {
                filterGroups: [{ filters: [{ propertyName: 'hs_pipeline', operator: 'EQ', value: ORACULO_PIPELINE_ID }] }],
                properties: ['subject', 'hs_pipeline_stage', 'createdate', 'hs_lastmodifieddate'], limit: 100,
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
                allTickets.push({ id: t.id, subject: t.properties.subject, companyName, stageId,
                    stageName: ORACULO_STAGES[stageId] || stageId,
                    created: t.properties.createdate, modified: t.properties.hs_lastmodifieddate });
            }
            if (data.paging && data.paging.next && data.paging.next.after) after = data.paging.next.after;
            else hasMore = false;
        }
        console.log('  HubSpot Oráculo: ' + allTickets.length + ' tickets');
        return allTickets;
    } catch (e) { console.log('  WARN: HubSpot fetch failed: ' + e.message); return []; }
}

// ===================== HELPERS =====================
function normalize(s) {
    return (s || '').toLowerCase()
        .normalize('NFD').replace(/[̀-ͯ]/g, '')
        .replace(/\s*(jeans|modas|moda|confeccoes|confecções|oficial|clothing|collection|acessorios|acessórios|tricot|ltda|me|eireli|s\.a\.|sa)\s*/gi, ' ')
        .replace(/[^a-z0-9]/g, ' ').replace(/\s+/g, ' ').trim();
}
function normSimple(s) { return (s || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').trim(); }
function buildMatchStructures(allEmpresas) {
    const empLookup = {}; const empWords = {};
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
    for (const [en, emp] of Object.entries(empLookup)) { if (en.includes(tn) || tn.includes(en)) return emp; }
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
    for (const [, c] of candidates) { if (c.score > bestScore) { bestScore = c.score; bestMatch = c.emp; } }
    return bestScore >= 2 ? bestMatch : null;
}
function readCSV(filename, onRow) {
    return new Promise((resolve) => {
        const fp = path.join(DIR, filename);
        if (!fs.existsSync(fp)) { console.log('  SKIP: ' + filename + ' not found'); resolve(); return; }
        const txt = fs.readFileSync(fp, 'utf-8');
        const lines = txt.split(/\r?\n/);
        if (lines.length === 0) { resolve(); return; }
        const parseLine = (line) => {
            const out = []; let cur = ''; let inQ = false;
            for (let i = 0; i < line.length; i++) {
                const ch = line[i];
                if (inQ) {
                    if (ch === '"') { if (line[i + 1] === '"') { cur += '"'; i++; } else inQ = false; }
                    else cur += ch;
                } else {
                    if (ch === '"') inQ = true;
                    else if (ch === ',') { out.push(cur); cur = ''; }
                    else cur += ch;
                }
            }
            out.push(cur);
            return out;
        };
        const headers = parseLine(lines[0]);
        for (let i = 1; i < lines.length; i++) {
            if (!lines[i].trim()) continue;
            const cols = parseLine(lines[i]);
            const row = {};
            headers.forEach((h, idx) => { row[h] = cols[idx] !== undefined ? cols[idx] : ''; });
            onRow(row);
        }
        resolve();
    });
}

// ===================== BQ FETCH: empresas ativas =====================
async function fetchEmpresasBQ() {
    const q = `
        WITH active_domains AS (
            SELECT ID AS id, name, modulos, CAST(angel_id AS STRING) angel_id,
                   CAST(integration_id AS STRING) integration_id, integration_type
            FROM ${DS}.odbc_domains
            WHERE LOWER(modulos) LIKE '%vendas%'
              AND (partner_id IS NULL OR CAST(partner_id AS STRING) NOT IN (
                  'ff66c2f1-1f9f-456c-9308-028e48c89582','25fec57c-620c-4ecd-ae7d-cd4fee27b158'))
              AND LOWER(name) NOT LIKE '%teste%'
        ),
        tags AS (
            SELECT CAST(ct.company_id AS STRING) company_id, STRING_AGG(t.name, ', ') tags
            FROM ${DS}.odbc_company_tags ct
            JOIN ${DS}.odbc_tags t ON CAST(t.id AS STRING) = CAST(ct.tag_id AS STRING)
            GROUP BY 1
        ),
        ranked AS (
            SELECT CAST(c.id AS STRING) id, CAST(c.domain_id AS STRING) domain_id,
                   c.tax_document, c.social_name, c.company_name, c.scheme_url,
                   c.created_at, c.status,
                   ROW_NUMBER() OVER (PARTITION BY c.domain_id ORDER BY c.created_at ASC) rn
            FROM ${DS}.odbc_companies c
            WHERE CAST(c.domain_id AS STRING) IN (SELECT CAST(id AS STRING) FROM active_domains)
        )
        SELECT rc.id, rc.domain_id, rc.tax_document, rc.social_name, rc.company_name,
               rc.scheme_url, rc.created_at, rc.status, rc.rn,
               d.name domain_name, d.modulos, d.integration_type,
               ang.name anjo, intg.name integracao, tg.tags
        FROM ranked rc
        JOIN active_domains d ON CAST(d.id AS STRING) = rc.domain_id
        LEFT JOIN ${DS}.odbc_angels ang ON CAST(ang.id AS STRING) = d.angel_id
        LEFT JOIN ${DS}.odbc_integrations intg ON CAST(intg.id AS STRING) = d.integration_id
        LEFT JOIN tags tg ON tg.company_id = rc.id
    `;
    const rows = await bqQuery(q, 'Empresas ativas (odbc_domains vendas + companies)');
    const empresas = [];
    for (const r of rows) {
        const empresaId = r.id;
        if (!empresaId) continue;
        const isFilial = (r.rn || 1) > 1;
        const statusNum = num(r.status);
        const statusText = statusNum === 1 ? 'Ativa' : statusNum === 2 ? 'Desativada' : '';
        empresas.push({
            id: empresaId, dominioId: num(r.domain_id), rn: r.rn || 1,
            isFilial, isMatriz: !isFilial,
            cnpj: r.tax_document || '',
            razaoSocial: r.social_name || r.company_name || '',
            nomeFantasia: isFilial ? (r.company_name || r.domain_name || '') : (r.domain_name || r.company_name || ''),
            nomeDominio: r.domain_name || r.scheme_url || '',
            schemeUrl: r.scheme_url || '',
            canal: '', anjo: r.anjo || '', integracao: r.integracao || '',
            statusEmpresa: statusText, email: '', tipoAtacado: '',
            tags: r.tags || '', modulos: r.modulos || '',
            criacao: tsStr(r.created_at), fromLakehouse: true,
        });
    }
    const matrizes = empresas.filter(e => !e.isFilial).length;
    console.log('  Empresas: ' + empresas.length + ' (' + matrizes + ' matrizes + ' +
        (empresas.length - matrizes) + ' filiais, ' + empresas.filter(e => e.statusEmpresa === 'Ativa').length + ' Ativa)');
    return empresas;
}

// ===================== BQ FETCH: pedidos/GMV =====================
async function fetchPedidosBQ() {
    const q = `
        SELECT CAST(domainId AS STRING) domainId,
            FORMAT_TIMESTAMP('%Y-%m', CAST(settings_createdAt AS TIMESTAMP)) mes,
            COUNT(*) qtd,
            COUNTIF(payment_isPaid = 'True') pagos,
            COUNTIF(status_consolidatedOrderStatus = 'CANCELED') cancelados,
            COUNTIF(payment_isPaid != 'True'
                    AND (status_consolidatedOrderStatus IS NULL
                         OR status_consolidatedOrderStatus NOT IN ('CANCELED','PAID'))) pendentes,
            ROUND(SUM(CAST(summary_total AS FLOAT64)),2) valTotal,
            ROUND(SUM(IF(payment_isPaid='True', CAST(summary_total AS FLOAT64),0)),2) valPagos,
            ROUND(SUM(IF(status_consolidatedOrderStatus='CANCELED', CAST(summary_total AS FLOAT64),0)),2) valCancelados
        FROM ${DS}.MongoDB_Pedidos_Geral
        WHERE summary_total IS NOT NULL
          AND SAFE_CAST(domainId AS INT64) IS NOT NULL
          AND CAST(summary_total AS FLOAT64) > 0 AND CAST(summary_total AS FLOAT64) < 50000
          AND settings_createdAt IS NOT NULL
        GROUP BY domainId, mes
    `;
    const rows = await bqQuery(q, 'MongoDB_Pedidos_Geral (pedidos/GMV)');
    const porDominio = new Map();
    for (const r of rows) {
        const dom = num(r.domainId);
        if (!Number.isFinite(dom) || !dom) continue;
        if (!porDominio.has(dom)) porDominio.set(dom, { pedidos:0,pagos:0,cancelados:0,pendentes:0,valTotal:0,valPagos:0,valCancelados:0,mensal:[] });
        const d = porDominio.get(dom);
        const qtd=num(r.qtd),pagos=num(r.pagos),cancelados=num(r.cancelados),pendentes=num(r.pendentes);
        const valTotal=num(r.valTotal),valPagos=num(r.valPagos),valCancelados=num(r.valCancelados);
        d.pedidos+=qtd; d.pagos+=pagos; d.cancelados+=cancelados; d.pendentes+=pendentes;
        d.valTotal+=valTotal; d.valPagos+=valPagos; d.valCancelados+=valCancelados;
        d.mensal.push({ mes:r.mes, qtd,pagos,cancelados,pendentes,valTotal,valPagos,valCancelados });
    }
    const mensalGlobalMap = new Map();
    for (const d of porDominio.values()) for (const m of d.mensal) {
        const cur = mensalGlobalMap.get(m.mes) || { qtd:0,pagos:0,cancelados:0,pendentes:0,valTotal:0,valPagos:0,valCancelados:0 };
        cur.qtd+=m.qtd;cur.pagos+=m.pagos;cur.cancelados+=m.cancelados;cur.pendentes+=m.pendentes;
        cur.valTotal+=m.valTotal;cur.valPagos+=m.valPagos;cur.valCancelados+=m.valCancelados;
        mensalGlobalMap.set(m.mes, cur);
    }
    const mensalGlobal = [...mensalGlobalMap.entries()].map(([mes,v]) => ({mes,...v})).sort((a,b)=>a.mes.localeCompare(b.mes));
    let tq=0,tv=0; for (const d of porDominio.values()){tq+=d.pedidos;tv+=d.valTotal;}
    console.log('  Pedidos: ' + tq.toLocaleString('pt-BR') + ' / R$ ' + tv.toLocaleString('pt-BR',{minimumFractionDigits:2}) + ' / ' + porDominio.size + ' domínios');
    return { porDominio, mensalGlobal };
}

// ===================== BQ FETCH: VestiPago cartão/pix (IUGU split) =====================
async function fetchVestiPagoBQ() {
    const totalsRows = await bqQuery(`
        SELECT CAST(companyId AS STRING) companyId,
            COUNT(*) qtPedidos, COUNTIF(payment_isPaid='True') qtPagos,
            ROUND(SUM(CAST(summary_total AS FLOAT64)),2) valTotal,
            ROUND(SUM(IF(payment_isPaid='True', CAST(summary_total AS FLOAT64),0)),2) valPagos,
            COUNTIF(payment_method='CREDIT_CARD') qtCartao,
            ROUND(SUM(IF(payment_method='CREDIT_CARD', CAST(summary_total AS FLOAT64),0)),2) valCartao,
            COUNTIF(payment_method='PIX') qtPix,
            ROUND(SUM(IF(payment_method='PIX', CAST(summary_total AS FLOAT64),0)),2) valPix
        FROM ${DS}.MongoDB_Pedidos_Geral
        WHERE payment_transaction_provider='IUGU' AND companyId IS NOT NULL
          AND CAST(summary_total AS FLOAT64) > 0 AND CAST(summary_total AS FLOAT64) < 50000
        GROUP BY companyId`, 'VestiPago totals (cartão/pix)');
    const monthlyRows = await bqQuery(`
        SELECT CAST(companyId AS STRING) companyId,
            FORMAT_TIMESTAMP('%Y-%m', CAST(settings_createdAt AS TIMESTAMP)) mes,
            COUNTIF(payment_method='CREDIT_CARD') qtCartao,
            ROUND(SUM(IF(payment_method='CREDIT_CARD', CAST(summary_total AS FLOAT64),0)),2) valCartao,
            COUNTIF(payment_method='PIX') qtPix,
            ROUND(SUM(IF(payment_method='PIX', CAST(summary_total AS FLOAT64),0)),2) valPix
        FROM ${DS}.MongoDB_Pedidos_Geral
        WHERE payment_transaction_provider='IUGU' AND companyId IS NOT NULL AND settings_createdAt IS NOT NULL
          AND CAST(summary_total AS FLOAT64) > 0 AND CAST(summary_total AS FLOAT64) < 50000
        GROUP BY companyId, mes`, 'VestiPago monthly (cartão/pix)');
    return { totalsRows, monthlyRows };
}

// ===================== BQ FETCH: VestiPago companies (registro) =====================
async function fetchVestiPagoSet() {
    try {
        const rows = await bqQuery(`SELECT DISTINCT CAST(companyId AS STRING) companyId
            FROM ${DS}.MongoDB_Payment_Companies WHERE companyId IS NOT NULL`, 'VestiPago companies (registro)');
        if (rows.length === 0) throw new Error('vazia');
        return new Set(rows.map(r => r.companyId));
    } catch (e) {
        console.warn('  WARN: MongoDB_Payment_Companies indisponível/vazia (' + e.message + '); derivando de transações IUGU');
        const rows = await bqQuery(`SELECT DISTINCT CAST(companyId AS STRING) companyId
            FROM ${DS}.MongoDB_Pedidos_Geral WHERE payment_transaction_provider='IUGU' AND companyId IS NOT NULL`, 'VestiPago set (fallback)');
        return new Set(rows.map(r => r.companyId));
    }
}

// ===================== BQ FETCH: links/cliques (por domínio→matriz) =====================
async function fetchLinksBQ() {
    const linkRows = await bqQuery(`
        WITH ud AS (SELECT CAST(id AS STRING) uid, CAST(domain_id AS STRING) domain_id FROM ${DS}.odbc_users)
        SELECT ud.domain_id,
            FORMAT_TIMESTAMP('%Y-%m', CAST(p.product_sent_lists_created_at AS TIMESTAMP)) mes,
            COUNT(DISTINCT p.product_sent_lists_id) links
        FROM ${DS}.sucessodocliente_products p
        JOIN ud ON ud.uid = CAST(p.USERS_ID AS STRING)
        WHERE p.product_sent_lists_created_at IS NOT NULL
          AND SAFE_CAST(p.product_sent_lists_created_at AS TIMESTAMP) < TIMESTAMP '2100-01-01'
        GROUP BY ud.domain_id, mes`, 'Links por domínio/mês');
    const cliqueRows = await bqQuery(`
        WITH ud AS (SELECT CAST(id AS STRING) uid, CAST(domain_id AS STRING) domain_id FROM ${DS}.odbc_users)
        SELECT ud.domain_id,
            FORMAT_TIMESTAMP('%Y-%m', CAST(r.rankings_created_at AS TIMESTAMP)) mes,
            SUM(CAST(r.rankings_shared_links AS INT64)) cliques
        FROM ${DS}.sucessodocliente_rankings r
        JOIN ud ON ud.uid = CAST(r.USERS_ID AS STRING)
        WHERE r.rankings_created_at IS NOT NULL
          AND SAFE_CAST(r.rankings_created_at AS TIMESTAMP) < TIMESTAMP '2100-01-01'
        GROUP BY ud.domain_id, mes`, 'Cliques por domínio/mês');
    return { linkRows, cliqueRows };
}

// ===================== BQ FETCH: faturamento Iugu (fatura cheia total_cents) =====================
async function fetchFaturamentoBQ() {
    const rows = await bqQuery(`
        WITH lines AS (
            SELECT id, customer_id, customer_name, payer_cpf_cnpj cnpj, status,
                   SAFE_CAST(total_cents AS FLOAT64) total_cents,
                   COALESCE(due_date, SUBSTR(created_at_iso,1,10)) due,
                   items_description, SAFE_CAST(items_price_cents AS FLOAT64) ipc
            FROM ${DS}.iugu_invoices
            WHERE customer_name IS NOT NULL
        ),
        plan_line AS (
            SELECT id, ARRAY_AGG(items_description ORDER BY ipc DESC LIMIT 1)[OFFSET(0)] plan
            FROM lines
            WHERE ipc > 0
              AND LOWER(IFNULL(items_description,'')) NOT LIKE '%desconto%'
              AND LOWER(IFNULL(items_description,'')) NOT LIKE '%oraculo%'
              AND LOWER(IFNULL(items_description,'')) NOT LIKE '%oráculo%'
            GROUP BY id
        ),
        inv AS (
            SELECT id, ANY_VALUE(customer_id) customer_id, ANY_VALUE(customer_name) customer_name,
                   ANY_VALUE(cnpj) cnpj, ANY_VALUE(status) status,
                   ANY_VALUE(total_cents) total_cents, ANY_VALUE(due) due
            FROM lines GROUP BY id
        ),
        inv2 AS (SELECT inv.*, pl.plan FROM inv LEFT JOIN plan_line pl USING(id))
        SELECT customer_id,
            ANY_VALUE(customer_name) customer_name, ANY_VALUE(cnpj) cnpj,
            ROUND(SUM(IF(status IN ('paid','externally_paid'), total_cents,0))/100,2) paid,
            ROUND(SUM(IF(status='pending', total_cents,0))/100,2) pending,
            ROUND(SUM(IF(status='expired', total_cents,0))/100,2) expired,
            ROUND(SUM(IF(status='canceled', total_cents,0))/100,2) canceled,
            COUNTIF(status IN ('paid','externally_paid')) nPagas,
            COUNTIF(status='pending') nPendentes,
            COUNTIF(status='expired') nVencidas,
            COUNT(*) qtd,
            ARRAY_AGG(STRUCT(SUBSTR(due,1,7) AS mes, status, ROUND(total_cents/100,2) AS total, SUBSTR(due,1,10) AS due, plan)
                      ORDER BY due DESC LIMIT 24) faturas
        FROM inv2 GROUP BY customer_id`, 'Faturamento Iugu (fatura cheia)');
    return rows;
}

// ===================== BQ FETCH: Oráculo (config + stats) =====================
async function fetchOraculoBQ() {
    const cfgRows = await bqQuery(`
        SELECT CAST(company_id AS STRING) company_id, CAST(domain_id AS STRING) domain_id,
               name, n8n_url, phone_origin, created_at, updated_at, link_report,
               CAST(phone_by_vesti AS STRING) phone_by_vesti, CAST(catalogue_with_price AS STRING) catalogue_with_price,
               CAST(agent_retail AS STRING) agent_retail, CAST(works_with_closed_square AS STRING) works_with_closed_square,
               CAST(keep_assigned_seller AS STRING) keep_assigned_seller
        FROM ${DS}.\`o-configurations\`
        WHERE n8n_url IS NOT NULL AND n8n_url != '' AND company_id IS NOT NULL`, 'Oráculo configurations');
    const configMap = new Map();
    const truthy = (v) => v === '1' || v === 'true' || v === 'True' || v === '1.0' || v === true;
    cfgRows.forEach(r => {
        configMap.set(r.company_id, {
            name: r.name || '', domain_id: r.domain_id || '',
            n8n_url: r.n8n_url || '', phone: r.phone_origin || '',
            created_at: tsStr(r.created_at), updated_at: tsStr(r.updated_at),
            link_report: r.link_report || '',
            phone_by_vesti: truthy(r.phone_by_vesti), catalogue_with_price: truthy(r.catalogue_with_price),
            agent_retail: truthy(r.agent_retail), works_with_closed_square: truthy(r.works_with_closed_square),
            keep_assigned_seller: truthy(r.keep_assigned_seller),
        });
    });
    console.log('  Oráculo configs: ' + configMap.size);

    const pedMensal = await bqQuery(`
        SELECT CAST(companyId AS STRING) companyId, FORMAT_TIMESTAMP('%Y-%m', settings_createdAt) mes,
            COUNT(*) pedidos, ROUND(SUM(CAST(summary_total AS FLOAT64)),2) vendas
        FROM ${DS}.oraculo_Pedidos WHERE companyId IS NOT NULL AND settings_createdAt IS NOT NULL
        GROUP BY companyId, mes`, 'Oráculo pedidos mensal');
    const pedSemanal = await bqQuery(`
        SELECT CAST(companyId AS STRING) companyId, Semana, ANY_VALUE(Semana_Formatada) label,
            Tipo_Venda_Oraculo tipo, COUNT(*) qtd, ROUND(SUM(CAST(summary_total AS FLOAT64)),2) valor
        FROM ${DS}.oraculo_Pedidos WHERE companyId IS NOT NULL AND Semana IS NOT NULL
        GROUP BY companyId, Semana, tipo`, 'Oráculo pedidos semanal');
    const interSemanal = await bqQuery(`
        SELECT CAST(company_id AS STRING) companyId, Semana, ANY_VALUE(Semana_Formatada) label,
            COUNTIF(source='IA') ia, COUNTIF(source='HUMAN') human, COUNT(*) total
        FROM ${DS}.oraculo_Atendimentos WHERE company_id IS NOT NULL AND Semana IS NOT NULL
        GROUP BY company_id, Semana`, 'Oráculo interações semanal');
    const evSemanal = await bqQuery(`
        SELECT CAST(config_company_id AS STRING) companyId, Semana, Eventos, COUNT(*) qtd
        FROM ${DS}.oraculo_Eventos WHERE config_company_id IS NOT NULL AND Semana IS NOT NULL
        GROUP BY config_company_id, Semana, Eventos`, 'Oráculo eventos semanal');

    const statsMap = new Map();
    const ensure = (cid) => {
        if (!statsMap.has(cid)) statsMap.set(cid, {
            pedidosOraculo:0, vendasOraculo:0, interacoesOraculo:0, iaTotal:0,
            vendasMensal:{}, pedidosMensal:{}, _sem:{}, _interSem:{}, _evSem:{},
        });
        return statsMap.get(cid);
    };
    pedMensal.forEach(r => {
        const s = ensure(r.companyId);
        s.vendasMensal[r.mes] = (s.vendasMensal[r.mes]||0) + num(r.vendas);
        s.pedidosMensal[r.mes] = (s.pedidosMensal[r.mes]||0) + num(r.pedidos);
        s.vendasOraculo += num(r.vendas); s.pedidosOraculo += num(r.pedidos);
    });
    pedSemanal.forEach(r => {
        const s = ensure(r.companyId); const sem = r.Semana;
        if (!s._sem[sem]) s._sem[sem] = { sem, label:r.label||'', direta:0,influenciada:0,outros:0,vDireta:0,vInfluenciada:0,vOutros:0 };
        const qtd=num(r.qtd), valor=num(r.valor);
        if (r.tipo === 'Venda Direta') { s._sem[sem].direta+=qtd; s._sem[sem].vDireta+=valor; }
        else if (r.tipo === 'Venda Influenciada') { s._sem[sem].influenciada+=qtd; s._sem[sem].vInfluenciada+=valor; }
        else { s._sem[sem].outros+=qtd; s._sem[sem].vOutros+=valor; }
    });
    interSemanal.forEach(r => {
        const s = ensure(r.companyId); const sem = r.Semana;
        s._interSem[sem] = { sem, label:r.label||'', ia:num(r.ia), human:num(r.human), total:num(r.total) };
        s.interacoesOraculo += num(r.total); s.iaTotal += num(r.ia);
    });
    evSemanal.forEach(r => {
        const s = ensure(r.companyId); const sem = r.Semana;
        if (!s._evSem[sem]) s._evSem[sem] = { sem };
        s._evSem[sem][r.Eventos || 'Outro'] = (s._evSem[sem][r.Eventos||'Outro']||0) + num(r.qtd);
    });
    for (const s of statsMap.values()) {
        s.vendasSemanal = Object.values(s._sem).sort((a,b)=>a.sem-b.sem);
        s.interacoesSemanal = Object.values(s._interSem).sort((a,b)=>a.sem-b.sem);
        s.eventosSemanal = Object.values(s._evSem).sort((a,b)=>a.sem-b.sem);
        s.atendimentosOraculo = s.interacoesOraculo;
        s.pctIAOraculo = s.interacoesOraculo > 0 ? Math.round((s.iaTotal / s.interacoesOraculo) * 1000) / 10 : 0;
        s.vendasOraculo = Math.round(s.vendasOraculo * 100) / 100;
        delete s._sem; delete s._interSem; delete s._evSem; delete s.iaTotal;
    }
    console.log('  Oráculo stats: ' + statsMap.size + ' empresas');
    return { configMap, statsMap };
}

module.exports = {
    DIR, num, digits,
    fetchOraculoTickets, normalize, normSimple, buildMatchStructures, matchTicketToEmpresa, readCSV,
    fetchEmpresasBQ, fetchPedidosBQ, fetchVestiPagoBQ, fetchVestiPagoSet,
    fetchLinksBQ, fetchFaturamentoBQ, fetchOraculoBQ,
};
