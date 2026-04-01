/**
 * Busca dados completos do Oráculo de cada empresa (painéis + configs)
 * incluindo vendas semanais, interações semanais e eventos.
 * Uso: node patch-oraculo.js
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

const ORACULO_PAINEIS_WS_ID = '63a65f3e-d96b-446e-a01d-f219132e1144';
const ORACULO_WS_ID = '2929476c-7b92-4366-9236-ccd13ffbd917';
const ORACULO_DS_ID = 'c6a480e9-2db4-45f7-ba67-b489407f59e6';

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

async function getFabricToken() {
    const querystring = require('querystring');
    const clientId = ENV.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e';
    const postBody = querystring.stringify({
        client_id: clientId, grant_type: 'refresh_token',
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
    if (data.refresh_token) {
        if (fs.existsSync(envPath)) {
            let env = fs.readFileSync(envPath, 'utf-8');
            env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + data.refresh_token);
            fs.writeFileSync(envPath, env, 'utf-8');
            console.log('  Refresh token atualizado');
        }
        fs.writeFileSync(path.join(DIR, '.new_refresh_token'), data.refresh_token, 'utf-8');
    }
    return data.access_token || null;
}

async function daxQuery(token, wsId, dsId, query) {
    const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + wsId + '/datasets/' + dsId + '/executeQueries',
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    if (res.statusCode === 429) return { error: 'rate_limit' };
    if (res.statusCode !== 200) return { error: 'http_' + res.statusCode };
    const data = JSON.parse(res.body);
    if (data.error) return { error: 'dax' };
    return { rows: data.results[0].tables[0].rows || [] };
}

async function fetchAllOraculoData(token, alreadyFetched) {
    console.log('Buscando datasets do workspace Oráculo painéis...');
    const dsRes = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + ORACULO_PAINEIS_WS_ID + '/datasets',
        method: 'GET',
        headers: { 'Authorization': 'Bearer ' + token },
    });
    if (dsRes.statusCode !== 200) { console.log('  ERRO: HTTP ' + dsRes.statusCode); return new Map(); }
    const datasets = JSON.parse(dsRes.body).value || [];
    console.log('  Encontrados ' + datasets.length + ' datasets');

    const daxKPIs = "EVALUATE ROW(\"pedidos\", COUNTROWS('f_Pedidos Oraculo'), \"interacoes\", COUNTROWS('f_Interacoes Oraculo Semanal'), \"atendimentos\", [KPI Atendimentos Oraculo], \"pctIA\", [KPI % Atendimento Oraculo], \"vendas\", [KPI Vendas Totais])";
    const daxVendasSemanal = "EVALUATE SUMMARIZECOLUMNS('f_Pedidos Oraculo'[Semana_Formatada], 'f_Pedidos Oraculo'[Semana], 'f_Pedidos Oraculo'[Tipo_Venda_Oraculo], \"qtd\", COUNTROWS('f_Pedidos Oraculo'), \"valor\", SUM('f_Pedidos Oraculo'[summary_total]))";
    const daxInterSemanal = "EVALUATE SUMMARIZECOLUMNS('f_Interacoes Oraculo Semanal'[Semana_Formatada], 'f_Interacoes Oraculo Semanal'[Semana], \"ia\", SUM('f_Interacoes Oraculo Semanal'[IA]), \"human\", SUM('f_Interacoes Oraculo Semanal'[Human]), \"total\", COUNTROWS('f_Interacoes Oraculo Semanal'))";
    const daxEventosSemanal = "EVALUATE SUMMARIZECOLUMNS('f_trigger_logs'[Semana], 'f_trigger_logs'[Eventos], \"qtd\", COUNTROWS('f_trigger_logs'))";
    const daxVendasMensal = "EVALUATE SUMMARIZECOLUMNS('f_Pedidos Oraculo'[settings_createdAt_TIMESTAMP], \"vendas\", [KPI Vendas Totais])";

    const map = new Map();
    let ok = 0, fail = 0;

    for (const ds of datasets) {
        if (ds.name === 'Report Usage Metrics Model') continue;
        const name = ds.name.replace(' - Oráculo', '').replace(' - Oraculo', '').trim();
        if (alreadyFetched && alreadyFetched.has(name.toLowerCase())) { console.log('  SKIP: ' + name + ' (já buscado)'); continue; }

        try {
            // KPIs
            const kpiRes = await daxQuery(token, ORACULO_PAINEIS_WS_ID, ds.id, daxKPIs);
            if (kpiRes.error === 'rate_limit') { console.log('  RATE LIMIT - parando'); break; }
            if (kpiRes.error) { fail++; console.log('  FAIL: ' + name + ' ' + kpiRes.error); continue; }
            const val = kpiRes.rows[0] || {};

            const stats = {
                name,
                pedidosOraculo: val['[pedidos]'] || 0,
                interacoesOraculo: val['[interacoes]'] || 0,
                atendimentosOraculo: val['[atendimentos]'] || 0,
                pctIAOraculo: val['[pctIA]'] != null ? Math.round(val['[pctIA]'] * 1000) / 10 : 0,
                vendasOraculo: val['[vendas]'] != null ? Math.round(val['[vendas]'] * 100) / 100 : 0,
                vendasMensal: {},
                vendasSemanal: [],
                interacoesSemanal: [],
                eventosSemanal: [],
            };

            // Vendas mensais (diário → mensal)
            const vmRes = await daxQuery(token, ORACULO_PAINEIS_WS_ID, ds.id, daxVendasMensal);
            if (vmRes.rows) {
                vmRes.rows.forEach(r => {
                    const dt = r["f_Pedidos Oraculo[settings_createdAt_TIMESTAMP]"] || '';
                    const v = r['[vendas]'] || 0;
                    if (dt && v) {
                        const mes = String(dt).substring(0, 7);
                        if (mes.length === 7) stats.vendasMensal[mes] = (stats.vendasMensal[mes] || 0) + Math.round(v * 100) / 100;
                    }
                });
            }

            // Vendas por semana e tipo
            const vsRes = await daxQuery(token, ORACULO_PAINEIS_WS_ID, ds.id, daxVendasSemanal);
            if (vsRes.rows) {
                const bySem = {};
                vsRes.rows.forEach(r => {
                    const sem = r["f_Pedidos Oraculo[Semana]"];
                    const label = r["f_Pedidos Oraculo[Semana_Formatada]"] || '';
                    const tipo = r["f_Pedidos Oraculo[Tipo_Venda_Oraculo]"] || 'Outros';
                    if (!bySem[sem]) bySem[sem] = { sem, label, direta: 0, influenciada: 0, outros: 0, vDireta: 0, vInfluenciada: 0, vOutros: 0 };
                    const qtd = r['[qtd]'] || 0;
                    const valor = Math.round((r['[valor]'] || 0) * 100) / 100;
                    if (tipo === 'Venda Direta') { bySem[sem].direta += qtd; bySem[sem].vDireta += valor; }
                    else if (tipo === 'Venda Influenciada') { bySem[sem].influenciada += qtd; bySem[sem].vInfluenciada += valor; }
                    else { bySem[sem].outros += qtd; bySem[sem].vOutros += valor; }
                });
                stats.vendasSemanal = Object.values(bySem).sort((a, b) => a.sem - b.sem);
            }

            // Interações semanais (IA vs Humano)
            const isRes = await daxQuery(token, ORACULO_PAINEIS_WS_ID, ds.id, daxInterSemanal);
            if (isRes.rows) {
                stats.interacoesSemanal = isRes.rows.map(r => ({
                    sem: r['f_Interacoes Oraculo Semanal[Semana]'],
                    label: r['f_Interacoes Oraculo Semanal[Semana_Formatada]'] || '',
                    ia: r['[ia]'] || 0,
                    human: r['[human]'] || 0,
                    total: r['[total]'] || 0,
                })).sort((a, b) => a.sem - b.sem);
            }

            // Eventos semanais
            const evRes = await daxQuery(token, ORACULO_PAINEIS_WS_ID, ds.id, daxEventosSemanal);
            if (evRes.rows) {
                const byEvSem = {};
                evRes.rows.forEach(r => {
                    const sem = r['f_trigger_logs[Semana]'];
                    const evento = r['f_trigger_logs[Eventos]'] || 'Outro';
                    if (!byEvSem[sem]) byEvSem[sem] = { sem };
                    byEvSem[sem][evento] = (byEvSem[sem][evento] || 0) + (r['[qtd]'] || 0);
                });
                stats.eventosSemanal = Object.values(byEvSem).sort((a, b) => a.sem - b.sem);
            }

            map.set(name.toLowerCase(), stats);
            const meses = Object.keys(stats.vendasMensal).length;
            console.log('  OK: ' + name + ' (ped=' + stats.pedidosOraculo + ', vendasSem=' + stats.vendasSemanal.length + ', interSem=' + stats.interacoesSemanal.length + ', evSem=' + stats.eventosSemanal.length + ')');
            ok++;
        } catch (e) {
            console.log('  FAIL: ' + name + ' ' + e.message);
            fail++;
        }
    }
    console.log('Stats: ' + ok + ' OK, ' + fail + ' failed');
    return map;
}

async function fetchOraculoConfigurations(token) {
    console.log('Buscando configurações do Oráculo...');
    const query = `EVALUATE SELECTCOLUMNS(
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
        "works_with_closed_square", Oraculo_configurations[works_with_closed_square]
    )`;
    const res = await daxQuery(token, ORACULO_WS_ID, ORACULO_DS_ID, query);
    if (res.error) { console.log('  ERRO configs: ' + res.error); return new Map(); }
    const map = new Map();
    (res.rows || []).forEach(r => {
        const companyId = r['[company_id]'] || '';
        if (companyId) {
            map.set(companyId, {
                name: r['[name]'] || '', domain_id: r['[domain_id]'] || '',
                n8n_url: r['[n8n_url]'] || '', phone: r['[phone_origin]'] || '',
                created_at: r['[created_at]'] || '', updated_at: r['[updated_at]'] || '',
                link_report: r['[link_report]'] || '',
                phone_by_vesti: r['[phone_by_vesti]'] === '1' || r['[phone_by_vesti]'] === 1,
                catalogue_with_price: r['[catalogue_with_price]'] === '1' || r['[catalogue_with_price]'] === 1,
                agent_retail: r['[agent_retail]'] === '1' || r['[agent_retail]'] === 1,
                works_with_closed_square: r['[works_with_closed_square]'] === '1' || r['[works_with_closed_square]'] === 1,
            });
        }
    });
    console.log('  Configs: ' + map.size + ' empresas');
    return map;
}

function normalize(s) { return s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim(); }

async function main() {
    if (!ENV.FABRIC_REFRESH_TOKEN || !ENV.FABRIC_TENANT_ID) {
        console.error('FABRIC_REFRESH_TOKEN e FABRIC_TENANT_ID obrigatórios');
        process.exit(1);
    }
    console.log('=== Patch Oráculo Completo ===\n');
    const token = await getFabricToken();
    if (!token) { console.error('Falha token'); process.exit(1); }
    console.log('  Token obtido\n');

    // Load existing dados.js to find already-fetched datasets
    const _dp = path.join(DIR, 'dados.js');
    const existingContent = fs.readFileSync(_dp, 'utf-8');
    const existingFn = new Function(existingContent + '; return DADOS;');
    const existingDados = existingFn();
    const alreadyFetched = new Set();
    existingDados.empresas.forEach(e => {
        if (e.oraculoFabric && e.oraculoFabric.vendasSemanal && e.oraculoFabric.vendasSemanal.length > 0) {
            alreadyFetched.add(normalize(e.nome));
        }
    });
    console.log('  Já buscados: ' + alreadyFetched.size + ' empresas\n');

    const [painelStats, configs] = await Promise.all([
        fetchAllOraculoData(token, alreadyFetched),
        fetchOraculoConfigurations(token),
    ]);

    if (painelStats.size === 0 && configs.size === 0) {
        console.log('\nNenhum dado. Nada a atualizar.');
        return;
    }

    console.log('\nCarregando dados.js...');
    const dadosPath = path.join(DIR, 'dados.js');
    const content = fs.readFileSync(dadosPath, 'utf-8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();
    console.log('  ' + DADOS.empresas.length + ' empresas');

    let matched = 0;
    for (const e of DADOS.empresas) {
        const nome = e.nome || '';
        const nomeNorm = normalize(nome);
        const nomeLC = nome.toLowerCase();

        let stats = painelStats.get(nomeLC) || null;
        if (!stats) {
            for (const [pName, pStats] of painelStats) {
                if (normalize(pName) === nomeNorm) { stats = pStats; break; }
            }
        }
        if (!stats) {
            for (const [pName, pStats] of painelStats) {
                const pNorm = normalize(pName);
                const minLen = Math.min(pNorm.length, nomeNorm.length);
                const maxLen = Math.max(pNorm.length, nomeNorm.length);
                if (minLen >= 5 && minLen / maxLen >= 0.4 && (nomeNorm.includes(pNorm) || pNorm.includes(nomeNorm))) {
                    stats = pStats; break;
                }
            }
        }

        const config = configs.get(e.id) || null;

        if (stats || config) {
            e.temOraculoFabric = true;
            const existing = e.oraculoFabric || {};
            e.oraculoFabric = {
                ...existing,
                ...(config || {}),
                pedidosOraculo: Math.max(stats ? stats.pedidosOraculo : 0, existing.pedidosOraculo || 0),
                interacoesOraculo: Math.max(stats ? stats.interacoesOraculo : 0, existing.interacoesOraculo || 0),
                atendimentosOraculo: Math.max(stats ? stats.atendimentosOraculo : 0, existing.atendimentosOraculo || 0),
                pctIAOraculo: stats && stats.pctIAOraculo ? stats.pctIAOraculo : (existing.pctIAOraculo || 0),
                vendasOraculo: Math.max(stats && stats.vendasOraculo ? stats.vendasOraculo : 0, existing.vendasOraculo || 0),
                vendasMensal: (stats && stats.vendasMensal && Object.keys(stats.vendasMensal).length > (existing.vendasMensal ? Object.keys(existing.vendasMensal).length : 0)) ? stats.vendasMensal : (existing.vendasMensal || undefined),
                pedidosMensal: existing.pedidosMensal || undefined,
                // Dados semanais dos gráficos
                vendasSemanal: stats && stats.vendasSemanal && stats.vendasSemanal.length > 0 ? stats.vendasSemanal : (existing.vendasSemanal || undefined),
                interacoesSemanal: stats && stats.interacoesSemanal && stats.interacoesSemanal.length > 0 ? stats.interacoesSemanal : (existing.interacoesSemanal || undefined),
                eventosSemanal: stats && stats.eventosSemanal && stats.eventosSemanal.length > 0 ? stats.eventosSemanal : (existing.eventosSemanal || undefined),
            };
            if (!e.oraculoEtapa && stats) e.oraculoEtapa = 'Ativo';
            matched++;
            console.log('  MATCH: ' + nome);
        }
    }

    console.log('\nTotal matched: ' + matched + '/' + DADOS.empresas.length);
    if (matched === 0) { console.log('Nenhuma matcheada.'); return; }

    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(dadosPath, output, 'utf-8');
    console.log('dados.js atualizado (' + (output.length / 1024 / 1024).toFixed(1) + ' MB)');
    console.log('\n=== CONCLUÍDO ===');
}

main().catch(e => { console.error('ERRO:', e.message); process.exit(1); });
