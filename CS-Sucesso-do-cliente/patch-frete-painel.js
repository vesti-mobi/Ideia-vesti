/**
 * Patch cirurgico: pega frete do "Painel Frete" (workspace Vesti) via DAX e
 * faz MERGE no dados.js atual, preservando o frete que ja vem do Relatorio Confeccoes.
 *
 * Chave de match: idDominio (campo da empresa no dados.js).
 * Para empresas presentes em ambas as fontes, soma totais e mensais.
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

const WS = '786bfd95-0733-4fcb-aa84-ef2c97518959';
const DS = '6fd1cfe9-cedc-4028-8d11-b6eb1d653d46';
const DIR = __dirname;

function loadEnv() {
    const env = {};
    fs.readFileSync(path.join(DIR, '.env'), 'utf-8').split('\n').forEach(line => {
        const m = line.match(/^([^=]+)=(.*)$/);
        if (m) env[m[1].trim()] = m[2].trim();
    });
    return env;
}
const ENV = loadEnv();

function httpsRequest(options, body) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, res => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                const raw = Buffer.concat(chunks).toString();
                try { resolve({ status: res.statusCode, data: JSON.parse(raw) }); }
                catch { resolve({ status: res.statusCode, data: raw }); }
            });
        });
        req.on('error', reject);
        if (body) req.write(body);
        req.end();
    });
}

async function getToken() {
    const body = querystring.stringify({
        client_id: ENV.FABRIC_CLIENT_ID,
        grant_type: 'refresh_token',
        refresh_token: ENV.FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
    });
    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: `/${ENV.FABRIC_TENANT_ID}/oauth2/v2.0/token`,
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    if (res.data.refresh_token && res.data.refresh_token !== ENV.FABRIC_REFRESH_TOKEN) {
        const envPath = path.join(DIR, '.env');
        let env = fs.readFileSync(envPath, 'utf-8');
        env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + res.data.refresh_token);
        fs.writeFileSync(envPath, env, 'utf-8');
    }
    return res.data.access_token;
}

async function dax(token, query) {
    const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: `/v1.0/myorg/groups/${WS}/datasets/${DS}/executeQueries`,
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    if (res.status === 200 && res.data.results && res.data.results[0]) {
        return res.data.results[0].tables[0].rows || [];
    }
    throw new Error('DAX query failed: ' + res.status + ' ' + JSON.stringify(res.data).substring(0, 500));
}

async function main() {
    const token = await getToken();
    console.log('Token OK');

    const daxFrete2 = `EVALUATE FILTER(SUMMARIZECOLUMNS('OnLog - Fechamento'[Dominio], 'OnLog - Fechamento'[Data].[Year], 'OnLog - Fechamento'[Data].[MonthNo], "TotalFrete", SUM('OnLog - Fechamento'[ValorPostagem])), [TotalFrete] > 0)`;
    const rows = await dax(token, daxFrete2);
    console.log(`Painel Frete query: ${rows.length} linhas`);

    // Build map por idDominio
    const freteByDominio = {};
    rows.forEach(r => {
        const keys = Object.keys(r);
        const dominioKey = keys.find(k => k.includes('Dominio'));
        const yearKey = keys.find(k => k.includes('Year'));
        const monthKey = keys.find(k => k.includes('MonthNo'));
        const dominio = dominioKey ? r[dominioKey] : null;
        const year = yearKey ? r[yearKey] : null;
        const month = monthKey ? r[monthKey] : null;
        const frete = r['[TotalFrete]'] != null ? r['[TotalFrete]'] : (r['TotalFrete'] || 0);
        if (!dominio || !year || !month) return;
        const mes = year + '-' + String(month).padStart(2, '0');
        const id = String(dominio);
        if (!freteByDominio[id]) freteByDominio[id] = { mensal: [], total: 0 };
        freteByDominio[id].mensal.push({ mes, valor: Math.round(frete * 100) / 100 });
        freteByDominio[id].total += frete;
    });
    for (const c of Object.values(freteByDominio)) {
        c.mensal.sort((a, b) => b.mes.localeCompare(a.mes));
        c.total = Math.round(c.total * 100) / 100;
    }
    console.log(`Painel Frete: ${Object.keys(freteByDominio).length} idDominios distintos`);

    // Carrega dados.js
    const dadosPath = path.join(DIR, 'dados.js');
    const txt = fs.readFileSync(dadosPath, 'utf-8');
    const D = JSON.parse(txt.replace(/^const DADOS\s*=\s*/, '').replace(/;?\s*$/, ''));
    console.log(`dados.js: ${D.empresas.length} empresas`);

    // Merge: pra cada empresa, se tem entrada em freteByDominio, somar.
    let updated = 0, novas = 0;
    D.empresas.forEach(e => {
        const f = freteByDominio[String(e.idDominio)];
        if (!f) return;
        const tinhaAntes = !!e.freteAtivo;
        if (!tinhaAntes) {
            // empresa nao tinha frete -> adicionar
            e.freteAtivo = true;
            e.freteTotal = f.total;
            e.freteMensal = f.mensal.slice(0, 12);
            novas++;
        } else {
            // empresa ja tinha (do Relatorio Confeccoes) -> somar
            const mensalMap = {};
            (e.freteMensal || []).forEach(m => { mensalMap[m.mes] = (mensalMap[m.mes] || 0) + m.valor; });
            f.mensal.forEach(m => { mensalMap[m.mes] = (mensalMap[m.mes] || 0) + m.valor; });
            const mensal = Object.entries(mensalMap)
                .map(([mes, valor]) => ({ mes, valor: Math.round(valor * 100) / 100 }))
                .sort((a, b) => b.mes.localeCompare(a.mes));
            e.freteMensal = mensal.slice(0, 12);
            e.freteTotal = Math.round((e.freteTotal + f.total) * 100) / 100;
            updated++;
        }
    });

    console.log(`Empresas atualizadas (ja tinham frete): ${updated}`);
    console.log(`Empresas novas com frete: ${novas}`);

    // Verificar Arary explicitamente
    const arary = D.empresas.find(e => e.idDominio === 1355848);
    if (arary) {
        console.log(`\nArary apos patch: freteAtivo=${arary.freteAtivo} total=R$ ${arary.freteTotal}`);
        console.log('  mensal:', JSON.stringify(arary.freteMensal));
    }

    // Salva (preserva o formato const DADOS = ...; igual ao original)
    const out = 'const DADOS = ' + JSON.stringify(D) + ';';
    fs.writeFileSync(dadosPath, out, 'utf-8');
    console.log(`\ndados.js atualizado (${(out.length / 1024).toFixed(0)} KB)`);
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
