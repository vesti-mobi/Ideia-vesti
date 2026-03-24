/**
 * Script leve que atualiza apenas os dados do HubSpot (Oráculo) no dados.js existente.
 * Roda no GitHub Actions sem precisar dos CSVs locais.
 *
 * Uso: HUBSPOT_TOKEN=xxx node update-hubspot.js
 */
const fs = require('fs');
const path = require('path');
const https = require('https');

const DIR = __dirname;
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

function hubspotRequest(endpoint, method, body) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'api.hubapi.com', path: endpoint, method: method || 'GET',
            headers: { 'Authorization': 'Bearer ' + HUBSPOT_TOKEN, 'Content-Type': 'application/json' },
        };
        const req = https.request(options, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch(e) { reject(e); } });
        });
        req.on('error', reject);
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

// Normalize name for fuzzy matching (same as build-data.js)
function normalize(s) {
    return (s || '').toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/\s*(jeans|modas|moda|confeccoes|confecções|oficial|clothing|collection|acessorios|acessórios|tricot|ltda|me|eireli|s\.a\.|sa)\s*/gi, ' ')
        .replace(/[^a-z0-9]/g, ' ').replace(/\s+/g, ' ').trim();
}

async function main() {
    if (!HUBSPOT_TOKEN) { console.error('HUBSPOT_TOKEN não definido'); process.exit(1); }

    // 1. Read existing dados.js
    const dadosPath = path.join(DIR, 'dados.js');
    if (!fs.existsSync(dadosPath)) { console.error('dados.js não encontrado'); process.exit(1); }
    const content = fs.readFileSync(dadosPath, 'utf-8');
    const DADOS = JSON.parse(content.replace('const DADOS = ', '').replace(/;$/, ''));
    console.log('Dados carregados:', DADOS.empresas.length, 'empresas');

    // 2. Fetch fresh Oráculo tickets
    console.log('Buscando tickets do Oráculo no HubSpot...');
    const data = await hubspotRequest('/crm/v3/objects/tickets/search', 'POST', {
        filterGroups: [{ filters: [{ propertyName: 'hs_pipeline', operator: 'EQ', value: ORACULO_PIPELINE_ID }] }],
        properties: ['subject', 'hs_pipeline_stage', 'createdate', 'hs_lastmodifieddate'],
        limit: 100,
    });
    const tickets = (data.results || []).map(t => {
        const stageId = t.properties.hs_pipeline_stage;
        let companyName = (t.properties.subject || '')
            .replace(/^[ÓO]R[ÁA]CULO\s*-\s*/i, '').replace(/\s*-\s*[ÓO]r[áa]culo.*/i, '')
            .replace(/\s*-\s*Agente.*/i, '').replace(/\s*\|.*/, '').replace(/\s*\(.*\)/, '').trim();
        if (companyName.startsWith('Oráculo ')) companyName = companyName.replace('Oráculo ', '').trim();
        if (companyName.startsWith('Óraculo ')) companyName = companyName.replace('Óraculo ', '').trim();
        return { companyName, stageName: ORACULO_STAGES[stageId] || stageId, created: t.properties.createdate, modified: t.properties.hs_lastmodifieddate };
    });
    console.log('Tickets encontrados:', tickets.length);

    // 3. Build empresa lookup for fuzzy matching
    const empLookup = {};
    const empWords = {};
    DADOS.empresas.forEach((e, idx) => {
        const n = normalize(e.nome);
        empLookup[n] = idx;
        n.split(' ').filter(w => w.length >= 3).forEach(w => {
            if (!empWords[w]) empWords[w] = [];
            empWords[w].push(idx);
        });
    });

    function matchTicket(ticket) {
        const tn = normalize(ticket.companyName);
        if (!tn || tn === 'oraculo' || tn === 'eventos') return -1;
        if (empLookup[tn] !== undefined) return empLookup[tn];
        for (const [en, idx] of Object.entries(empLookup)) {
            if (en.includes(tn) || tn.includes(en)) return idx;
        }
        const ticketWords = tn.split(' ').filter(w => w.length >= 3);
        if (ticketWords.length === 0) return -1;
        const candidates = new Map();
        ticketWords.forEach(tw => {
            for (const [word, idxs] of Object.entries(empWords)) {
                if (word === tw || word.startsWith(tw) || tw.startsWith(word)) {
                    idxs.forEach(idx => {
                        const prev = candidates.get(idx) || 0;
                        candidates.set(idx, prev + (word === tw ? 2 : 1));
                    });
                }
            }
        });
        let bestIdx = -1, bestScore = 0;
        for (const [idx, score] of candidates) {
            if (score > bestScore) { bestScore = score; bestIdx = idx; }
        }
        return bestScore >= 2 ? bestIdx : -1;
    }

    // 4. Clear old oráculo data and apply fresh
    let matched = 0;
    DADOS.empresas.forEach(e => { e.oraculoEtapa = ''; });

    const oraculoByIdx = {};
    for (const t of tickets) {
        const idx = matchTicket(t);
        if (idx >= 0) {
            matched++;
            if (!oraculoByIdx[idx] || t.modified > oraculoByIdx[idx].modified) {
                oraculoByIdx[idx] = t;
            }
        }
    }
    for (const [idx, t] of Object.entries(oraculoByIdx)) {
        DADOS.empresas[idx].oraculoEtapa = t.stageName;
    }
    console.log('Matched:', matched + '/' + tickets.length);

    // 5. Recalculate churn scores
    DADOS.empresas.forEach(e => {
        let score = 0;
        const motivos = [];
        if (e.m) {
            const meses = Object.keys(e.m).sort();
            if (meses.length >= 4) {
                const recent3 = meses.slice(-3);
                const prev3 = meses.slice(-6, -3);
                const sumR = recent3.reduce((s, m) => s + (e.m[m] ? e.m[m][0] : 0), 0);
                const sumP = prev3.reduce((s, m) => s + (e.m[m] ? e.m[m][0] : 0), 0);
                if (sumP > 0 && sumR < sumP * 0.5) { score += 30; motivos.push('Queda >50% pedidos'); }
                else if (sumP > 0 && sumR < sumP * 0.7) { score += 15; motivos.push('Queda >30% pedidos'); }
            }
            if (meses.length > 0) {
                const last = e.m[meses[meses.length - 1]];
                if (last && last[0] === 0) { score += 25; motivos.push('Zero pedidos mês atual'); }
            }
        }
        if ((e.pedidos || 0) > 10 && (e.pedidosCancelados || 0) > (e.pedidosPagos || 0) * 0.3) { score += 15; motivos.push('Alto cancelamento'); }
        if (!e.temIntegracao) { score += 10; motivos.push('Sem integração'); }
        if (e.oraculoEtapa === 'Churn') { score += 30; motivos.push('Oráculo: Churn'); }
        else if (e.oraculoEtapa === 'Parado') { score += 20; motivos.push('Oráculo: Parado'); }
        e.churnScore = Math.min(score, 100);
        e.churnRisco = score >= 60 ? 'Alto' : score >= 30 ? 'Médio' : 'Baixo';
        e.churnMotivos = motivos.join('; ');
    });

    // 6. Update oráculo summary
    const oraculoSummary = {};
    tickets.forEach(t => { oraculoSummary[t.stageName] = (oraculoSummary[t.stageName] || 0) + 1; });
    DADOS.oraculoSummary = oraculoSummary;
    DADOS.oraculoTickets = tickets.map(t => ({ nome: t.companyName, etapa: t.stageName, criado: t.created, atualizado: t.modified }));
    DADOS.churnStats = {
        alto: DADOS.empresas.filter(e => e.churnRisco === 'Alto').length,
        medio: DADOS.empresas.filter(e => e.churnRisco === 'Médio').length,
        total: DADOS.empresas.length,
    };
    DADOS.geradoEm = new Date().toISOString();

    // 7. Write
    const output = 'const DADOS = ' + JSON.stringify(DADOS) + ';';
    fs.writeFileSync(dadosPath, output, 'utf-8');
    console.log('\ndados.js atualizado (' + (output.length / 1024).toFixed(0) + ' KB)');
    console.log('Churn: ' + DADOS.churnStats.alto + ' alto, ' + DADOS.churnStats.medio + ' médio');
}

main().catch(err => { console.error(err); process.exit(1); });
