/**
 * Script para agregar dados dos CSVs do Power BI e gerar dados.js para o dashboard.
 * Executa com: node build-data.js
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const DIR = __dirname;

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

            return {
                i: idx,
                nome: e.nomeFantasia || e.nomeDominio,
                canal: e.canal,
                cartao: e.cartaoImpl ? 'Sim' : 'Não',
                pix: e.pixImpl ? 'Sim' : 'Não',
                cnpj: e.cnpj,
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

    // Output
    const output = {
        empresas: empresasList,
        mensal: monthlyData,
        meses: allMonths, // all available months for date range pickers
        totalEmpresas: empresasList.length,
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
