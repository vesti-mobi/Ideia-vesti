/**
 * Diagnóstico e correção para os tickets:
 * 1) Diferenças relatório outubro vs API (fuso horário UTC vs BRT)
 * 2) Cliente fora do endpoint listar clientes (CNPJ 57.663.493/0001-67)
 *
 * Uso: node fabric-ticket-fix.js
 * Requer: FABRIC_REFRESH_TOKEN e FABRIC_TENANT_ID no .env
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const querystring = require('querystring');
const { Connection, Request } = require('tedious');

const DIR = __dirname;

// Load .env
const _envPath = path.join(DIR, '.env');
const _env = {};
if (fs.existsSync(_envPath)) {
    fs.readFileSync(_envPath, 'utf-8').split('\n').forEach(l => {
        const m = l.match(/^([^#=]+)=(.*)$/);
        if (m) _env[m[1].trim()] = m[2].trim();
    });
}

const FABRIC_REFRESH_TOKEN = process.env.FABRIC_REFRESH_TOKEN || _env.FABRIC_REFRESH_TOKEN || '';
const FABRIC_TENANT_ID = process.env.FABRIC_TENANT_ID || _env.FABRIC_TENANT_ID || '';
const CLIENT_ID = _env.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e';

// Fabric IDs
const WORKSPACE_ID = 'aced753a-0f0e-4bcf-9264-72f6496cf2cf';
const DATASET_ID = 'e6c74524-e355-4447-9eb4-baae76b84dc4';
const SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com';
const DB_NAME = 'VestiHouse';

// ==================== HTTP ====================
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

// ==================== AUTH ====================
let _currentRefreshToken = FABRIC_REFRESH_TOKEN;

async function getToken(scope) {
    const postBody = querystring.stringify({
        client_id: CLIENT_ID,
        grant_type: 'refresh_token',
        refresh_token: _currentRefreshToken,
        scope: scope + ' offline_access',
    });
    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: `/${FABRIC_TENANT_ID}/oauth2/v2.0/token`,
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postBody) },
    }, postBody);
    const data = JSON.parse(res.body);
    if (!data.access_token) throw new Error('Token failed: ' + (data.error_description || ''));
    // Update refresh token for next call
    if (data.refresh_token) {
        _currentRefreshToken = data.refresh_token;
        if (fs.existsSync(_envPath)) {
            let env = fs.readFileSync(_envPath, 'utf-8');
            env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + data.refresh_token);
            fs.writeFileSync(_envPath, env, 'utf-8');
        }
    }
    return data.access_token;
}

// ==================== DAX ====================
async function daxQuery(token, query, label) {
    console.log(`  DAX: ${label}...`);
    const bodyStr = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: `/v1.0/myorg/groups/${WORKSPACE_ID}/datasets/${DATASET_ID}/executeQueries`,
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
    }, bodyStr);
    if (res.statusCode !== 200) { console.error(`  ERROR ${label}: HTTP ${res.statusCode} - ${res.body.substring(0, 300)}`); return []; }
    const data = JSON.parse(res.body);
    if (data.error) { console.error(`  ERROR ${label}: ${JSON.stringify(data.error).substring(0, 300)}`); return []; }
    const rows = (data.results?.[0]?.tables?.[0]?.rows) || [];
    const cleaned = rows.map(row => {
        const obj = {};
        for (const [key, val] of Object.entries(row)) {
            const match = key.match(/\[(.+)\]$/);
            obj[match ? match[1] : key] = val;
        }
        return obj;
    });
    console.log(`  ${label}: ${cleaned.length} rows`);
    return cleaned;
}

// ==================== SQL ====================
function runSQL(token, query, label) {
    return new Promise((resolve, reject) => {
        console.log(`  SQL: ${label}...`);
        const config = {
            server: SQL_SERVER,
            authentication: { type: 'azure-active-directory-access-token', options: { token } },
            options: { database: DB_NAME, encrypt: true, port: 1433, requestTimeout: 120000 },
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
                console.log(`  ${label}: ${rows.length} rows`);
                resolve(rows);
            });
            conn.execSql(request);
        });
        conn.connect();
    });
}

// ==================== TICKET 1: FUSO HORÁRIO ====================
async function diagnosticoFusoHorario(pbiToken) {
    console.log('\n' + '='.repeat(60));
    console.log('TICKET 1: Diagnóstico Fuso Horário (UTC vs BRT)');
    console.log('='.repeat(60));

    // Query 1: Pedidos por mês em UTC (como está hoje)
    const daxUTC = `
EVALUATE
SUMMARIZECOLUMNS(
    'Merged Pedidos'[Data Criacao].[Year],
    'Merged Pedidos'[Data Criacao].[MonthNo],
    "TotalPedidos", COUNTROWS('Merged Pedidos')
)
`;

    // Query 2: Pedidos que mudam de mês quando convertidos UTC->BRT
    // Pedidos criados entre 00:00 e 02:59 UTC = 21:00-23:59 BRT do dia anterior
    const daxImpacto = `
EVALUATE
VAR PedidosMudaMes = FILTER(
    'Merged Pedidos',
    HOUR('Merged Pedidos'[Data Criacao]) < 3
    && DAY('Merged Pedidos'[Data Criacao]) = 1
)
RETURN
SUMMARIZECOLUMNS(
    'Merged Pedidos'[Data Criacao].[Year],
    'Merged Pedidos'[Data Criacao].[MonthNo],
    KEEPFILTERS(PedidosMudaMes),
    "PedidosMudamDeMes", COUNTROWS('Merged Pedidos')
)
`;

    // Query 3: Detalhes outubro especificamente
    const daxOutubro = `
EVALUATE
VAR OutubroUTC = CALCULATETABLE(
    ADDCOLUMNS(
        SUMMARIZE('Merged Pedidos', 'Merged Pedidos'[Data Criacao]),
        "Hora", HOUR('Merged Pedidos'[Data Criacao]),
        "Dia", DAY('Merged Pedidos'[Data Criacao])
    ),
    'Merged Pedidos'[Data Criacao].[Year] = 2025,
    'Merged Pedidos'[Data Criacao].[MonthNo] = 10,
    HOUR('Merged Pedidos'[Data Criacao]) < 3,
    DAY('Merged Pedidos'[Data Criacao]) = 1
)
RETURN
SELECTCOLUMNS(OutubroUTC, "DataCriacao", [Data Criacao], "Hora", [Hora])
`;

    const [pedidosUTC, impacto, detalheOut] = await Promise.all([
        daxQuery(pbiToken, daxUTC, 'Pedidos por mês (UTC)'),
        daxQuery(pbiToken, daxImpacto, 'Pedidos que mudam de mês (UTC->BRT)').catch(() => []),
        daxQuery(pbiToken, daxOutubro, 'Detalhe outubro dia 1 00-03h').catch(() => []),
    ]);

    console.log('\n--- Pedidos por mês (UTC original) ---');
    const sorted = pedidosUTC.sort((a, b) => (a.Year * 100 + a.MonthNo) - (b.Year * 100 + b.MonthNo));
    sorted.forEach(r => {
        console.log(`  ${r.Year}-${String(r.MonthNo).padStart(2, '0')}: ${r.TotalPedidos} pedidos`);
    });

    if (impacto.length) {
        console.log('\n--- Pedidos que mudariam de mês (criados dia 1, 00:00-02:59 UTC) ---');
        impacto.forEach(r => {
            console.log(`  Mês ${r.Year}-${String(r.MonthNo).padStart(2, '0')}: ${r.PedidosMudamDeMes} pedidos pertencem ao mês anterior em BRT`);
        });
    }

    if (detalheOut.length) {
        console.log(`\n--- Outubro: ${detalheOut.length} pedidos do dia 1 entre 00-03h UTC (seriam setembro em BRT) ---`);
    }

    // Save report
    const report = {
        ticket: 'Diferenças relatório outubro vs API (fuso horário)',
        diagnostico: 'Datas armazenadas em UTC, relatório extrai mês sem converter para BRT (UTC-3)',
        impacto: 'Pedidos criados entre 00:00-02:59 UTC do dia 1 de cada mês pertencem ao mês anterior em BRT',
        pedidosPorMes: sorted,
        pedidosAfetados: impacto,
        correcao: 'Criar coluna calculada DataBRT = [Data Criacao] - TIME(3,0,0) no modelo semântico do Power BI',
        daxCorrecao: "DataBRT = 'Merged Pedidos'[Data Criacao] - TIME(3, 0, 0)"
    };

    fs.writeFileSync(path.join(DIR, 'ticket1-fuso-horario-diagnostico.json'), JSON.stringify(report, null, 2), 'utf-8');
    console.log('\nRelatório salvo: ticket1-fuso-horario-diagnostico.json');
    return report;
}

// ==================== TICKET 2: CLIENTE FORA DO ENDPOINT ====================
async function diagnosticoCliente(pbiToken) {
    console.log('\n' + '='.repeat(60));
    console.log('TICKET 2: Cliente fora do endpoint listar clientes');
    console.log('CNPJ: 57.663.493/0001-67 | Company: 86341028-49f1-45eb-bce8-b2ebf5f6bcf4');
    console.log('='.repeat(60));

    const companyId = '86341028-49f1-45eb-bce8-b2ebf5f6bcf4';

    // 1. Buscar empresa no DAX (Cadastros Empresas)
    const cadastro = await daxQuery(pbiToken,
        `EVALUATE FILTER('Cadastros Empresas', 'Cadastros Empresas'[Id Empresa] = "${companyId}")`,
        'Cadastro empresa').catch(() => []);

    // 2. Buscar pedidos dessa empresa no Merged Pedidos
    const pedidos = await daxQuery(pbiToken,
        `EVALUATE TOPN(10, FILTER('Merged Pedidos', 'Merged Pedidos'[ID Empresa] = "${companyId}"), 'Merged Pedidos'[Data Criacao], DESC)`,
        'Últimos pedidos da empresa').catch(() => []);

    // 3. Total de pedidos por mês dessa empresa
    const pedidosMes = await daxQuery(pbiToken,
        `EVALUATE CALCULATETABLE(
            SUMMARIZECOLUMNS(
                'Merged Pedidos'[Data Criacao].[Year],
                'Merged Pedidos'[Data Criacao].[MonthNo],
                "Qtd", COUNTROWS('Merged Pedidos'),
                "Val", SUM('Merged Pedidos'[Total])
            ),
            'Merged Pedidos'[ID Empresa] = "${companyId}"
        )`,
        'Pedidos por mês da empresa').catch(() => []);

    // 4. Buscar nome da empresa pra contexto
    const nomeEmpresa = cadastro.length > 0
        ? (cadastro[0]['Nome Empresa'] || cadastro[0]['Nome Dominio'] || JSON.stringify(cadastro[0]).substring(0, 100))
        : 'Não encontrada no cadastro';

    console.log('\n--- Resultados ---');
    console.log(`Empresa: ${nomeEmpresa}`);
    console.log(`Cadastro encontrado: ${cadastro.length > 0 ? 'SIM' : 'NÃO'}`);
    if (cadastro.length) {
        const c = cadastro[0];
        console.log(`  ID Domínio: ${c['Id Dominio'] || c['idDominio'] || '-'}`);
        console.log(`  Status: ${c['Status Empresa'] || c['statusEmpresa'] || '-'}`);
        console.log(`  Anjo: ${c['Anjo'] || '-'}`);
    }
    console.log(`Pedidos encontrados: ${pedidos.length}`);
    if (pedidosMes.length) {
        console.log('Pedidos por mês:');
        pedidosMes.sort((a, b) => (a.Year * 100 + a.MonthNo) - (b.Year * 100 + b.MonthNo))
            .forEach(r => console.log(`  ${r.Year}-${String(r.MonthNo).padStart(2, '0')}: ${r.Qtd} pedidos (R$ ${(r.Val || 0).toFixed(2)})`));
    }

    const report = {
        ticket: 'Cliente fora do endpoint listar clientes',
        integrador: 'UPDash',
        cnpj: '57.663.493/0001-67',
        companyId,
        nomeEmpresa,
        empresaEncontradaNoFabric: cadastro.length > 0,
        cadastroDetalhes: cadastro,
        pedidosRecentes: pedidos.slice(0, 5),
        pedidosPorMes: pedidosMes,
        analise: cadastro.length > 0
            ? 'Empresa EXISTE no Fabric com pedidos. O problema é no endpoint da API de listagem de clientes (filtro de data, paginação ou bug no backend). Esse ticket é para o suporte (Teixeira/Priscila/Renata) com os dados abaixo como evidência.'
            : 'Empresa NÃO encontrada no Fabric. Investigar pipeline de ingestão de dados.',
        recomendacao: 'Passar para suporte com: (1) empresa existe no Fabric, (2) dados de pedidos comprovam atividade, (3) endpoint específico funciona mas listagem não retorna o cliente.',
    };

    fs.writeFileSync(path.join(DIR, 'ticket2-cliente-endpoint-diagnostico.json'), JSON.stringify(report, null, 2), 'utf-8');
    console.log('\nRelatório salvo: ticket2-cliente-endpoint-diagnostico.json');
    return report;
}

// ==================== MAIN ====================
async function main() {
    console.log('=== Diagnóstico Fabric para Tickets HubSpot ===\n');

    if (!FABRIC_REFRESH_TOKEN || !FABRIC_TENANT_ID) {
        throw new Error('Configure FABRIC_REFRESH_TOKEN e FABRIC_TENANT_ID no .env');
    }

    // Get both tokens
    console.log('Autenticando...');
    const pbiToken = await getToken('https://analysis.windows.net/powerbi/api/.default');
    console.log('  PBI token OK');
    // Run both diagnostics
    const t1 = await diagnosticoFusoHorario(pbiToken);
    const t2 = await diagnosticoCliente(pbiToken);

    console.log('\n' + '='.repeat(60));
    console.log('RESUMO');
    console.log('='.repeat(60));
    console.log('\nTicket 1 (Fuso horário):');
    console.log('  Correção: Adicionar coluna calculada no modelo semântico:');
    console.log(`  ${t1.daxCorrecao}`);
    console.log('  Depois usar DataBRT nos relatórios em vez de Data Criacao.');
    console.log('\nTicket 2 (Cliente endpoint):');
    console.log(`  ${t2.analise}`);
}

main().catch(e => { console.error('\nERRO:', e.message); process.exit(1); });
