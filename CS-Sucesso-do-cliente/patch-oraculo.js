/**
 * Busca dados do Oráculo (painéis + configurações) do Power BI Fabric
 * e atualiza o dados.js existente sem re-rodar o build completo.
 *
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
// Fallback to process.env
['FABRIC_TENANT_ID','FABRIC_REFRESH_TOKEN','FABRIC_CLIENT_ID'].forEach(k => { if (!ENV[k] && process.env[k]) ENV[k] = process.env[k]; });

const FABRIC_TENANT_ID = ENV.FABRIC_TENANT_ID || '';
const FABRIC_REFRESH_TOKEN = ENV.FABRIC_REFRESH_TOKEN || '';
const FABRIC_CLIENT_ID = ENV.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e';
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
    const postBody = querystring.stringify({
        client_id: FABRIC_CLIENT_ID,
        grant_type: 'refresh_token',
        refresh_token: FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default',
    });
    const tokenRes = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: '/' + FABRIC_TENANT_ID + '/oauth2/v2.0/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postBody) },
    }, postBody);
    const tokenData = JSON.parse(tokenRes.body);
    if (tokenData.refresh_token) {
        const envUpdatePath = path.join(DIR, '.env');
        if (fs.existsSync(envUpdatePath)) {
            let env = fs.readFileSync(envUpdatePath, 'utf-8');
            env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + tokenData.refresh_token);
            fs.writeFileSync(envUpdatePath, env, 'utf-8');
            console.log('  Refresh token atualizado no .env');
        } else {
            // In CI, save for later use
            fs.writeFileSync(path.join(DIR, '.new_refresh_token'), tokenData.refresh_token, 'utf-8');
            console.log('  New refresh token saved to .new_refresh_token');
        }
    }
    return tokenData.access_token || null;
}

async function fetchOraculoPainelStats(token) {
    console.log('Buscando datasets do workspace Oráculo painéis...');
    const dsRes = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + ORACULO_PAINEIS_WS_ID + '/datasets',
        method: 'GET',
        headers: { 'Authorization': 'Bearer ' + token },
    });
    if (dsRes.statusCode !== 200) {
        console.log('  ERRO: HTTP ' + dsRes.statusCode, dsRes.body.substring(0, 200));
        return new Map();
    }
    const datasets = JSON.parse(dsRes.body).value || [];
    console.log('  Encontrados ' + datasets.length + ' datasets');

    const dax = "EVALUATE ROW(\"pedidos\", COUNTROWS('f_Pedidos Oraculo'), \"interacoes\", COUNTROWS('f_Interacoes Oraculo Semanal'), \"atendimentos\", [KPI Atendimentos Oraculo], \"pctIA\", [KPI % Atendimento Oraculo], \"vendas\", [KPI Vendas Totais])";
    // DAX para vendas diárias (agregar para mensal no código)
    const daxVendasDiarias = "EVALUATE SUMMARIZECOLUMNS('f_Pedidos Oraculo'[settings_createdAt_TIMESTAMP], \"vendas\", [KPI Vendas Totais])";
    const map = new Map();
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
                const stats = {
                    name,
                    pedidosOraculo: val['[pedidos]'] || 0,
                    interacoesOraculo: val['[interacoes]'] || 0,
                    atendimentosOraculo: val['[atendimentos]'] || 0,
                    pctIAOraculo: val['[pctIA]'] != null ? Math.round(val['[pctIA]'] * 1000) / 10 : 0,
                    vendasOraculo: val['[vendas]'] != null ? Math.round(val['[vendas]'] * 100) / 100 : 0,
                    vendasMensal: {},
                };
                // Buscar vendas diárias e agregar por mês
                try {
                    const bodyD = JSON.stringify({ queries: [{ query: daxVendasDiarias }], serializerSettings: { includeNulls: true } });
                    const resD = await httpsRequest({
                        hostname: 'api.powerbi.com',
                        path: '/v1.0/myorg/groups/' + ORACULO_PAINEIS_WS_ID + '/datasets/' + ds.id + '/executeQueries',
                        method: 'POST',
                        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyD) },
                    }, bodyD);
                    if (resD.statusCode === 200) {
                        const dailyRows = JSON.parse(resD.body).results?.[0]?.tables?.[0]?.rows || [];
                        dailyRows.forEach(r => {
                            const dt = r["f_Pedidos Oraculo[settings_createdAt_TIMESTAMP]"] || r["'f_Pedidos Oraculo'[settings_createdAt_TIMESTAMP]"] || r['[settings_createdAt_TIMESTAMP]'] || '';
                            const v = r['[vendas]'] || 0;
                            if (dt && v) {
                                const mes = String(dt).substring(0, 7); // "YYYY-MM"
                                if (mes.length === 7) stats.vendasMensal[mes] = (stats.vendasMensal[mes] || 0) + Math.round(v * 100) / 100;
                            }
                        });
                    }
                } catch (e2) { /* ignore daily vendas errors */ }
                map.set(name.toLowerCase(), stats);
                const meses = Object.keys(stats.vendasMensal).length;
                console.log('  OK: ' + name + ' (ped=' + (val['[pedidos]'] || 0) + ', atend=' + (val['[atendimentos]'] || 0) + (meses > 0 ? ', vendasMeses=' + meses : '') + ')');
                ok++;
            } else {
                console.log('  FAIL: ' + name + ' HTTP ' + res.statusCode);
                fail++;
            }
        } catch (e) {
            console.log('  FAIL: ' + name + ' ' + e.message);
            fail++;
        }
    }
    console.log('Oráculo painéis stats: ' + ok + ' OK, ' + fail + ' failed');
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
        "works_with_closed_square", Oraculo_configurations[works_with_closed_square],
        "keep_assigned_seller", Oraculo_configurations[keep_assigned_seller]
    )`;
    const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + ORACULO_WS_ID + '/datasets/' + ORACULO_DS_ID + '/executeQueries',
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    if (res.statusCode !== 200) {
        console.log('  ERRO configs: HTTP ' + res.statusCode, res.body.substring(0, 200));
        return new Map();
    }
    const rows = JSON.parse(res.body).results?.[0]?.tables?.[0]?.rows || [];
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
    console.log('Oráculo configurations: ' + map.size + ' empresas');
    return map;
}

function normalize(s) { return s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim(); }

async function main() {
    if (!FABRIC_REFRESH_TOKEN || !FABRIC_TENANT_ID) {
        console.error('FABRIC_REFRESH_TOKEN e FABRIC_TENANT_ID são obrigatórios no .env');
        process.exit(1);
    }

    console.log('=== Patch Oráculo dados.js ===\n');

    // 1. Get token
    console.log('Obtendo token Fabric...');
    const token = await getFabricToken();
    if (!token) { console.error('Falha ao obter token'); process.exit(1); }
    console.log('  Token obtido\n');

    // 2. Fetch Oráculo data
    const [painelStats, configs] = await Promise.all([
        fetchOraculoPainelStats(token),
        fetchOraculoConfigurations(token),
    ]);

    if (painelStats.size === 0 && configs.size === 0) {
        console.log('\nNenhum dado de Oráculo encontrado. Nada a atualizar.');
        return;
    }

    // 3. Load existing dados.js
    console.log('\nCarregando dados.js...');
    const dadosPath = path.join(DIR, 'dados.js');
    const content = fs.readFileSync(dadosPath, 'utf-8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();
    console.log('  ' + DADOS.empresas.length + ' empresas carregadas');

    // 4. Match and patch
    let matched = 0;
    for (const e of DADOS.empresas) {
        const nome = e.nome || '';
        const nomeNorm = normalize(nome);
        const nomeLC = nome.toLowerCase();

        // Match painel stats by name
        let stats = painelStats.get(nomeLC) || null;
        if (!stats) {
            // Try normalized match
            for (const [pName, pStats] of painelStats) {
                if (normalize(pName) === nomeNorm) { stats = pStats; break; }
            }
        }
        if (!stats) {
            // Partial match: require minimum 5 chars and the shorter must be >= 60% of the longer
            for (const [pName, pStats] of painelStats) {
                const pNorm = normalize(pName);
                const minLen = Math.min(pNorm.length, nomeNorm.length);
                const maxLen = Math.max(pNorm.length, nomeNorm.length);
                if (minLen >= 5 && minLen / maxLen >= 0.4 && (nomeNorm.includes(pNorm) || pNorm.includes(nomeNorm))) {
                    stats = pStats; break;
                }
            }
        }

        // Match config by company ID
        const config = configs.get(e.id) || null;

        if (stats || config) {
            e.temOraculoFabric = true;
            e.oraculoFabric = {
                ...(config || {}),
                pedidosOraculo: stats ? stats.pedidosOraculo : 0,
                interacoesOraculo: stats ? stats.interacoesOraculo : 0,
                atendimentosOraculo: stats ? stats.atendimentosOraculo : 0,
                pctIAOraculo: stats ? stats.pctIAOraculo : 0,
                vendasOraculo: stats ? stats.vendasOraculo : 0,
                vendasMensal: stats && stats.vendasMensal && Object.keys(stats.vendasMensal).length > 0 ? stats.vendasMensal : undefined,
            };
            // Set oraculoEtapa if not already set
            if (!e.oraculoEtapa && stats) {
                e.oraculoEtapa = 'Ativo';
            }
            matched++;
            console.log('  MATCH: ' + nome + (stats ? ' (stats)' : '') + (config ? ' (config)' : ''));
        }
    }

    console.log('\nTotal matched: ' + matched + '/' + DADOS.empresas.length);

    if (matched === 0) {
        console.log('Nenhuma empresa matcheada. Verifique os nomes dos datasets.');
        return;
    }

    // 5. Write updated dados.js
    console.log('Salvando dados.js...');
    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(dadosPath, output, 'utf-8');
    console.log('  dados.js atualizado (' + (output.length / 1024 / 1024).toFixed(1) + ' MB)');
    console.log('\n=== CONCLUÍDO ===');
}

main().catch(e => { console.error('ERRO:', e.message); process.exit(1); });
