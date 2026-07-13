/**
 * CS Dashboard — build BigQuery (migração Fabric → BQ). Entry point.
 *
 * Substitui build-cloud.js + patch-links + patch-painel-cs + patch-oraculo +
 * patch-invoices por UM script que lê tudo do BigQuery `vestilake_BI` e monta o
 * dados.js completo. Frete REMOVIDO. Fontes locais preservadas (Cadastros/Config/
 * Marcas/Controle Luana/CSAT/NPS) + HubSpot (Oráculo tickets).
 *
 * O passo patch-empresas (DADOS.fabricCounts, lê ../PainelCSGerencial/companies_data.json)
 * continua como patch separado no workflow.
 *
 * Env: GOOGLE_APPLICATION_CREDENTIALS (SA key), HUBSPOT_TOKEN.
 * Usage: node build-cloud-bq.js
 */
const fs = require('fs');
const path = require('path');
const F = require('./bq-fetchers.js');
const { DIR, num, digits, normalize, normSimple } = F;

async function main() {
    console.log('=== CS Dashboard Build (BigQuery) ===\n');

    // ---------- 1. Fetch BigQuery (paralelo) ----------
    console.log('Consultando BigQuery...');
    const [lakehouseEmpresas, mongoPedidos, vp, vestiPagoSet, linksData, faturamentoRows, oraculo] = await Promise.all([
        F.fetchEmpresasBQ(), F.fetchPedidosBQ(), F.fetchVestiPagoBQ(), F.fetchVestiPagoSet(),
        F.fetchLinksBQ(), F.fetchFaturamentoBQ(), F.fetchOraculoBQ(),
    ]);
    const { totalsRows: vpTotalsRows, monthlyRows: vpMonthlyRows } = vp;
    const { linkRows, cliqueRows } = linksData;
    const { configMap: oraculoConfigMap, statsMap: oraculoStatsMap } = oraculo;

    // ---------- 2. HubSpot Oráculo tickets ----------
    console.log('\nHubSpot...');
    const oraculoTickets = await F.fetchOraculoTickets();

    // ---------- 3. CSVs locais: Cadastros / Config / Marcas ----------
    console.log('\nLendo CSVs locais...');
    const empresasMap = {};
    const empresasByDominio = {};
    await F.readCSV('Cadastros Empresas.csv', (row) => {
        const id = row['Id Empresa'];
        if (!id) return;
        empresasMap[id] = {
            id,
            cnpj: row['CNPJ'] || '',
            anjo: row['Anjo'] || '',
            integracao: row['Integração'] || row['Integracao'] || '',
            tags: row['Tags'] || '',
            temIntegracao: row['Tem Integração?'] || row['Tem Integracao?'] || '',
            idDominio: row['Id Dominio'] || '',
            nomeDominio: row['Nome do Dominio'] || '',
            nomeFantasia: row['Nome Fantasia'] || '',
            razaoSocial: row['Razao Social'] || '',
            canal: row['Canal de Vendas'] || '',
            modulo: row['Modulo'] || '',
            tipoAtacado: row['Tipo Atacado  Varejo'] || '',
            criacao: row['Criação do Dominio'] || row['Criacao do Dominio'] || '',
            tipoIntegracao: row['Domains.integration_type'] || '',
            dataPrimeiroPedido: row['Data do Primeiro Pedido VESTIPAGO'] || '',
            valorPlano: parseFloat(row['Valor Cobrado Plano']) || 0,
            transCartao: 0, transPix: 0, transTotal: 0, valCartao: 0, valPix: 0, valTotal: 0,
            pedidos: 0, pedidosPagos: 0, pedidosCancelados: 0, pedidosPendentes: 0,
            valPedidosPagos: 0, valPedidosCancelados: 0, valPedidosPendentes: 0,
            linksEnviados: 0, cliques: 0, cartaoImpl: false, pixImpl: false,
        };
        if (row['Id Dominio']) empresasByDominio[row['Id Dominio']] = empresasMap[id];
    });
    console.log('  Cadastros CSV: ' + Object.keys(empresasMap).length + ' empresas');

    await F.readCSV('Config Empresas.csv', (row) => {
        const companyId = row['docs.companyId'];
        if (companyId && empresasMap[companyId]) {
            const ck = row['docs.creditCard.isEnabled'];
            const pk = row['docs.pix.isEnabled'];
            empresasMap[companyId].cartaoImpl = (ck === true || ck === 'True' || ck === 'true');
            empresasMap[companyId].pixImpl = (pk === true || pk === 'True' || pk === 'true');
        }
    });

    // Marcas e Planos (Excel preferencial, fallback CSV)
    const marcasMap = {};
    const excelPath = path.join(DIR, 'Marcas e Planos.xlsx');
    let marcasSource = 'CSV';
    if (fs.existsSync(excelPath)) {
        try {
            const XLSX = require('xlsx');
            const wb = XLSX.readFile(excelPath);
            const extractSheetDate = (name) => {
                let m = name.match(/(\d{2})-?(\d{4})/); if (m) return m[2] + '-' + m[1];
                m = name.match(/(\d{2})-?(\d{2})$/); if (m) return '20' + m[2] + '-' + m[1];
                return '0000-00';
            };
            const vestiSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('vesti') && !s.toLowerCase().includes('starter'));
            const starterSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('starter'));
            vestiSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));
            starterSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));
            const sheetsToRead = [];
            if (starterSheets.length > 0) sheetsToRead.push(starterSheets[0]);
            if (vestiSheets.length > 0) sheetsToRead.push(vestiSheets[0]);
            if (sheetsToRead.length === 0) sheetsToRead.push(wb.SheetNames[0]);
            const allLinesByCnpj = {};
            for (const sheetName of sheetsToRead) {
                const ws = wb.Sheets[sheetName];
                const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
                for (const row of rows) {
                    const cnpj = String(row['CPFCNPJ'] || row['CPF e CNPJ'] || '').replace(/[.\-\/\s]/g, '');
                    if (!cnpj || cnpj.length < 11) continue;
                    if (!allLinesByCnpj[cnpj]) allLinesByCnpj[cnpj] = [];
                    allLinesByCnpj[cnpj].push({
                        marca: row['MARCA'] || '', plano: (row['PLANO'] || '').trim(),
                        setup: parseFloat(row['SETUP']) || 0, mensalidade: parseFloat(row['MENSALIDADE']) || 0,
                        integracao: parseFloat(row['INTEGRAÇÃO'] || row['INTEGRACAO']) || 0,
                        assistente: parseFloat(row['ASSISTENTE']) || 0, filial: parseFloat(row['FILIAL']) || 0,
                        descontos: parseFloat(row['DESCONTOS']) || 0,
                        totalCobrado: parseFloat(row['TOTAL COBRADO'] || row['TOTAL_COBRADO']) || 0,
                        observacoes: row['OBSERVAÇÕES'] || row['OBSERVACOES'] || '',
                        canal: row['CANAL'] || row['CANAL/Agência'] || '', subconta: row['Subconta'] || '',
                    });
                }
                console.log('  Excel sheet "' + sheetName + '": ' + rows.length + ' rows');
            }
            const isExtra = (p) => /oraculo|oráculo|integração|integracao|pacote/i.test(p);
            for (const [cnpj, lines] of Object.entries(allLinesByCnpj)) {
                const mainL = lines.find(l => !isExtra(l.plano)) || lines[0];
                const entry = { marca: mainL.marca, plano: mainL.plano, setup: mainL.setup, mensalidade: mainL.mensalidade, integracao: mainL.integracao, assistente: mainL.assistente, filial: mainL.filial, descontos: mainL.descontos, totalCobrado: mainL.totalCobrado, observacoes: mainL.observacoes, canal: mainL.canal, subconta: mainL.subconta };
                if (lines.length > 1) entry.planos = lines.map(l => ({ plano: l.plano, mensalidade: l.mensalidade, integracao: l.integracao, assistente: l.assistente, filial: l.filial, descontos: l.descontos, totalCobrado: l.totalCobrado, setup: l.setup }));
                marcasMap[cnpj] = entry;
            }
            marcasSource = 'Excel';
            console.log('  Marcas e Planos (Excel): ' + Object.keys(marcasMap).length + ' CNPJs');
        } catch (e) { console.log('  WARN: Excel read failed: ' + e.message); }
    }
    if (marcasSource === 'CSV') {
        await F.readCSV('Marcas e Planos.csv', (row) => {
            const cnpj = String(row['CPFCNPJ'] || '').replace(/[.\-\/\s]/g, '');
            if (cnpj) marcasMap[cnpj] = {
                marca: row['MARCA'] || '', plano: row['PLANO'] || '', setup: 0, mensalidade: 0,
                integracao: 0, assistente: 0, filial: 0, descontos: 0,
                totalCobrado: parseFloat(row['TOTAL_COBRADO']) || 0, observacoes: '', canal: '', subconta: '',
            };
        });
        console.log('  Marcas e Planos (CSV): ' + Object.keys(marcasMap).length + ' CNPJs');
    }

    // ---------- 4. Controle Luana + CSAT + NPS ----------
    const controleMap = {}; const controleByNome = {};
    await F.readCSV('controle_geral_luana_csv.csv', (row) => {
        const companyId = row['Company*ID'] || row['CompanyID'] || '';
        const marca = row['MARCAS'] || '';
        const entry = { usuario: row['Usuário'] || row['Usuario'] || '', senha: row['Senha'] || '',
            etapaHub: row['ETAPA HUB'] || '', mensalidade: row['MENSALIDADE'] || '' };
        if (companyId) controleMap[companyId] = entry;
        if (marca) controleByNome[marca.toLowerCase().trim()] = entry;
    });
    console.log('  Controle Luana: ' + Object.keys(controleMap).length + ' empresas');

    const csatByEmpresa = {};
    const csatPath = path.join(DIR, '_csat.json');
    if (fs.existsSync(csatPath)) {
        try {
            JSON.parse(fs.readFileSync(csatPath, 'utf-8')).forEach(c => {
                const key = normSimple(c.empresa);
                if (!csatByEmpresa[key]) csatByEmpresa[key] = [];
                csatByEmpresa[key].push({ mes: c.mes, nota: c.nota, obs: c.obs || '' });
            });
        } catch (e) { console.log('  WARN: _csat.json: ' + e.message); }
    }
    const npsMap = {};
    const npsPath = path.join(DIR, '_nps.json');
    if (fs.existsSync(npsPath)) {
        try { JSON.parse(fs.readFileSync(npsPath, 'utf-8')).forEach(n => { if (n.dominio) npsMap[String(n.dominio)] = n.nps; }); }
        catch (e) { console.log('  WARN: _nps.json: ' + e.message); }
    }

    // ---------- 5. Matriz por domínio ----------
    const matrizPorDominio = new Map();
    for (const lh of lakehouseEmpresas) if (lh.isMatriz && !matrizPorDominio.has(lh.dominioId)) matrizPorDominio.set(lh.dominioId, lh);
    // dominio -> matriz company id
    const matrizIdByDominio = new Map();
    for (const [dom, lh] of matrizPorDominio) matrizIdByDominio.set(String(dom), lh.id);

    function ensureStub(lh) {
        if (empresasMap[lh.id]) return empresasMap[lh.id];
        const stub = {
            id: lh.id, cnpj: lh.cnpj, anjo: lh.anjo, integracao: lh.integracao, tags: lh.tags,
            temIntegracao: lh.integracao ? 'Sim' : '', idDominio: lh.dominioId,
            nomeDominio: lh.nomeDominio, nomeFantasia: lh.nomeFantasia, razaoSocial: lh.razaoSocial,
            canal: lh.canal, modulo: lh.modulos, tipoAtacado: lh.tipoAtacado, criacao: lh.criacao,
            tipoIntegracao: '', dataPrimeiroPedido: '', valorPlano: 0, statusEmpresa: lh.statusEmpresa || '',
            transCartao: 0, transPix: 0, transTotal: 0, valCartao: 0, valPix: 0, valTotal: 0,
            pedidos: 0, pedidosPagos: 0, pedidosCancelados: 0, pedidosPendentes: 0,
            valPedidosPagos: 0, valPedidosCancelados: 0, valPedidosPendentes: 0,
            linksEnviados: 0, cliques: 0, cartaoImpl: false, pixImpl: false, _stubFromLakehouse: true,
        };
        empresasMap[lh.id] = stub;
        return stub;
    }

    // ---------- 6. Pedidos/GMV -> matriz ----------
    let mongoMatched = 0, mongoMissingDom = 0;
    for (const [dominioId, totals] of mongoPedidos.porDominio) {
        const matrizLh = matrizPorDominio.get(dominioId);
        if (!matrizLh) { mongoMissingDom++; continue; }
        const emp = ensureStub(matrizLh);
        emp.pedidos = totals.pedidos; emp.pedidosPagos = totals.pagos;
        emp.pedidosCancelados = totals.cancelados; emp.pedidosPendentes = totals.pendentes;
        emp.valTotal = totals.valTotal; emp.valPedidosPagos = totals.valPagos;
        emp.valPedidosCancelados = totals.valCancelados;
        emp.valPedidosPendentes = totals.valTotal - totals.valPagos - totals.valCancelados;
        emp.transTotal = totals.pedidos;
        emp._mongoMensal = totals.mensal.sort((a, b) => a.mes.localeCompare(b.mes));
        mongoMatched++;
    }
    console.log('  Mongo pedidos aplicados: ' + mongoMatched + ' matrizes (' + mongoMissingDom + ' domínios s/ matriz descartados)');

    // ---------- 7. VestiPago cartão/pix ----------
    const vpPorEmpresa = new Map();
    for (const row of vpTotalsRows) {
        if (!row.companyId) continue;
        vpPorEmpresa.set(row.companyId, {
            qtCartao: num(row.qtCartao), valCartao: num(row.valCartao),
            qtPix: num(row.qtPix), valPix: num(row.valPix),
            qtPedidos: num(row.qtPedidos), qtPagos: num(row.qtPagos),
            valTotal: num(row.valTotal), valPagos: num(row.valPagos),
        });
    }
    let vpMatched = 0;
    for (const [cid, v] of vpPorEmpresa) {
        const emp = empresasMap[cid]; if (!emp) continue;
        emp.transCartao = v.qtCartao; emp.transPix = v.qtPix;
        emp.valCartao = v.valCartao; emp.valPix = v.valPix; vpMatched++;
    }
    console.log('  VestiPago totals aplicados: ' + vpMatched + '/' + vpPorEmpresa.size);

    // VestiPago monthly por empresa+mês
    const vpPorEmpresaMensal = new Map();
    for (const row of vpMonthlyRows) {
        const cid = row.companyId; if (!cid || !row.mes) continue;
        if (!vpPorEmpresaMensal.has(cid)) vpPorEmpresaMensal.set(cid, new Map());
        vpPorEmpresaMensal.get(cid).set(row.mes, {
            qtCartao: num(row.qtCartao), valCartao: num(row.valCartao),
            qtPix: num(row.qtPix), valPix: num(row.valPix),
        });
    }

    // ---------- 8. Links/cliques -> matriz (canonical por domínio) ----------
    const linksByCompany = {}, linksMensais = {}, linksMensaisEmp = {};
    const cliquesByCompany = {}, cliquesMensais = {}, cliquesMensaisEmp = {};
    for (const r of linkRows) {
        const canId = matrizIdByDominio.get(String(r.domain_id));
        const links = num(r.links); const mes = r.mes;
        if (!canId || !mes) continue;
        linksByCompany[canId] = (linksByCompany[canId] || 0) + links;
        linksMensais[mes] = (linksMensais[mes] || 0) + links;
        if (!linksMensaisEmp[canId]) linksMensaisEmp[canId] = {};
        linksMensaisEmp[canId][mes] = (linksMensaisEmp[canId][mes] || 0) + links;
    }
    for (const r of cliqueRows) {
        const canId = matrizIdByDominio.get(String(r.domain_id));
        const cl = num(r.cliques); const mes = r.mes;
        if (!canId || !mes) continue;
        cliquesByCompany[canId] = (cliquesByCompany[canId] || 0) + cl;
        cliquesMensais[mes] = (cliquesMensais[mes] || 0) + cl;
        if (!cliquesMensaisEmp[canId]) cliquesMensaisEmp[canId] = {};
        cliquesMensaisEmp[canId][mes] = (cliquesMensaisEmp[canId][mes] || 0) + cl;
    }
    // aplica totais no empresasMap
    for (const [cid, v] of Object.entries(linksByCompany)) { if (empresasMap[cid]) empresasMap[cid].linksEnviados = v; }
    for (const [cid, v] of Object.entries(cliquesByCompany)) { if (empresasMap[cid]) empresasMap[cid].cliques = v; }
    console.log('  Links/cliques: ' + Object.keys(linksByCompany).length + ' empresas c/ links, ' + Object.keys(cliquesByCompany).length + ' c/ cliques');

    // ---------- 9. Pedidos company/month (mongo) + VP overlay ----------
    const pedidosCompanyMonth = {};
    for (const [dominioId, totals] of mongoPedidos.porDominio) {
        const matrizLh = matrizPorDominio.get(dominioId); if (!matrizLh) continue;
        const empId = matrizLh.id;
        if (!pedidosCompanyMonth[empId]) pedidosCompanyMonth[empId] = {};
        for (const m of totals.mensal) pedidosCompanyMonth[empId][m.mes] = {
            qtd: m.qtd, pagos: m.pagos, cancelados: m.cancelados, pendentes: m.pendentes,
            val: m.valTotal, valPagos: m.valPagos, tc: 0, tp: 0, vc: 0, vp: 0,
        };
    }
    for (const [empId, mesMap] of vpPorEmpresaMensal) {
        if (!pedidosCompanyMonth[empId]) pedidosCompanyMonth[empId] = {};
        for (const [mesKey, v] of mesMap) {
            const cur = pedidosCompanyMonth[empId][mesKey] || { qtd:0,pagos:0,cancelados:0,pendentes:0,val:0,valPagos:0,tc:0,tp:0,vc:0,vp:0 };
            cur.tc = v.qtCartao; cur.tp = v.qtPix; cur.vc = v.valCartao; cur.vp = v.valPix;
            pedidosCompanyMonth[empId][mesKey] = cur;
        }
    }

    // ---------- 10. HubSpot fuzzy matching ----------
    const allEmpresas = Object.values(empresasMap).filter(e => e.nomeFantasia || e.nomeDominio);
    const { empLookup, empWords } = F.buildMatchStructures(allEmpresas);
    const oraculoByEmpId = {};
    let oraculoMatched = 0;
    for (const t of oraculoTickets) {
        const emp = F.matchTicketToEmpresa(t, empLookup, empWords);
        if (emp) { oraculoMatched++; if (!oraculoByEmpId[emp.id] || t.modified > oraculoByEmpId[emp.id].modified) oraculoByEmpId[emp.id] = t; }
    }
    console.log('  Oráculo tickets matched: ' + oraculoMatched + '/' + oraculoTickets.length);

    // ---------- 11. Monthly global ----------
    const pedidosMensais = {};
    // VP global split por mês (soma sobre empresas)
    const vpGlobalMensal = {};
    for (const [, mesMap] of vpPorEmpresaMensal) for (const [mes, v] of mesMap) {
        if (!vpGlobalMensal[mes]) vpGlobalMensal[mes] = { tc:0, tp:0, vc:0, vp:0 };
        vpGlobalMensal[mes].tc += v.qtCartao; vpGlobalMensal[mes].tp += v.qtPix;
        vpGlobalMensal[mes].vc += v.valCartao; vpGlobalMensal[mes].vp += v.valPix;
    }
    for (const m of mongoPedidos.mensalGlobal) {
        const g = vpGlobalMensal[m.mes] || { tc:0, tp:0, vc:0, vp:0 };
        pedidosMensais[m.mes] = {
            cartao: g.tc, pix: g.tp, total: m.qtd,
            valCartao: Math.round(g.vc*100)/100, valPix: Math.round(g.vp*100)/100,
            valTotal: m.valTotal, pagos: m.pagos, cancelados: m.cancelados, pendentes: m.pendentes, valPagos: m.valPagos,
        };
    }
    const sortedMonths = Object.keys(pedidosMensais).sort();
    const allMonths = sortedMonths;
    const recentMonths = sortedMonths.slice(-18);
    const monthlyData = recentMonths.map(m => ({
        mes: m, ...pedidosMensais[m],
        links: linksMensais[m] || 0, cliques: cliquesMensais[m] || 0,
    }));

    // ---------- 12. Empresas ativas (fonte: lakehouse BQ) ----------
    const filialGroups = {}; const matrizIds = new Set();
    const empresasAtivas = [];
    for (const lh of lakehouseEmpresas) {
        let emp = empresasMap[lh.id];
        if (!emp) { emp = ensureStub(lh); }
        else {
            if (!emp.idDominio) emp.idDominio = lh.dominioId;
            if (!emp.cnpj) emp.cnpj = lh.cnpj;
            if (!emp.nomeFantasia) emp.nomeFantasia = lh.nomeFantasia;
            if (!emp.nomeDominio) emp.nomeDominio = lh.nomeDominio;
            if (!emp.razaoSocial) emp.razaoSocial = lh.razaoSocial;
            if (!emp.canal) emp.canal = lh.canal;
            if (!emp.anjo) emp.anjo = lh.anjo;
            if (!emp.integracao) emp.integracao = lh.integracao;
            if (!emp.tags) emp.tags = lh.tags;
        }
        if (lh.statusEmpresa) emp.statusEmpresa = lh.statusEmpresa;
        emp.isMatriz = lh.isMatriz; emp.isFilial = lh.isFilial; emp.lakehouseRn = lh.rn;
        empresasAtivas.push(emp);
    }
    empresasAtivas.forEach(e => {
        if (!e.idDominio) return;
        const key = String(e.idDominio);
        if (!filialGroups[key]) filialGroups[key] = [];
        filialGroups[key].push(e);
        if (e.isMatriz) matrizIds.add(e.id);
    });
    Object.values(filialGroups).forEach(group => {
        if (!group.some(e => matrizIds.has(e.id))) {
            const sorted = [...group].sort((a, b) => (a.lakehouseRn || 999) - (b.lakehouseRn || 999));
            if (sorted[0]) matrizIds.add(sorted[0].id);
        }
    });
    console.log('  Empresas ativas: ' + empresasAtivas.length);

    // ---------- 13. Build empresasList ----------
    const now = new Date();
    let empIndex = 0;
    let empresasList = empresasAtivas.map(e => {
        const cnpjNum = (e.cnpj || '').replace(/[.\-\/]/g, '');
        let marca = marcasMap[cnpjNum];
        if (!marca && cnpjNum.length >= 8) {
            for (const [mcnpj, mdata] of Object.entries(marcasMap)) if (mcnpj.substring(0, 8) === cnpjNum.substring(0, 8)) { marca = mdata; break; }
        }
        if (!marca) {
            const nomeEmp = normSimple(e.nomeFantasia || e.nomeDominio);
            for (const [, mdata] of Object.entries(marcasMap)) { const nMarca = normSimple(mdata.marca); if (nMarca && nomeEmp && nMarca === nomeEmp) { marca = mdata; break; } }
            if (!marca && nomeEmp.length >= 5) for (const [, mdata] of Object.entries(marcasMap)) { const nMarca = normSimple(mdata.marca); if (nMarca.length >= 5 && (nomeEmp.includes(nMarca) || nMarca.includes(nomeEmp))) { marca = mdata; break; } }
        }
        const idx = empIndex++;
        const nome = e.nomeFantasia || e.nomeDominio;
        const ctrl = controleMap[e.id] || controleByNome[(nome || '').toLowerCase().trim()];
        const oracTkt = oraculoByEmpId[e.id];

        let mensalidade = '';
        if (ctrl && ctrl.mensalidade) mensalidade = ctrl.mensalidade;
        else if (marca && marca.totalCobrado) mensalidade = 'R$ ' + marca.totalCobrado.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
        else if (e.valorPlano > 0) mensalidade = 'R$ ' + e.valorPlano.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
        const etapaHub = ctrl ? ctrl.etapaHub : '';
        const oraculoEtapa = oracTkt ? oracTkt.stageName : '';

        const empMonthly = pedidosCompanyMonth[e.id] || {};
        const empLinks = linksMensaisEmp[e.id] || {};
        const empCliques = cliquesMensaisEmp[e.id] || {};
        const empMonthKeys = [...new Set([...Object.keys(empMonthly), ...Object.keys(empLinks), ...Object.keys(empCliques)])].sort();
        const m = {};
        for (const mk of empMonthKeys) {
            const md = empMonthly[mk]; const lk = empLinks[mk] || 0; const ck = empCliques[mk] || 0;
            if (md || lk || ck) m[mk] = [
                md ? md.qtd : 0, md ? md.pagos : 0, md ? md.cancelados : 0, md ? md.pendentes : 0,
                md ? Math.round(md.val * 100) / 100 : 0, md ? Math.round(md.valPagos * 100) / 100 : 0, 0,
                md ? Math.round((md.val - md.valPagos) * 100) / 100 : 0,
                md ? md.tc : 0, md ? md.tp : 0, md ? Math.round(md.vc * 100) / 100 : 0, md ? Math.round(md.vp * 100) / 100 : 0,
                lk, ck,
            ];
        }

        // Churn
        let churnScore = 0; const churnMotivos = [];
        const recentMonthKeys = [];
        for (let i = 0; i < 6; i++) { const d = new Date(now.getFullYear(), now.getMonth() - i, 1); recentMonthKeys.push(d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0')); }
        const last3 = recentMonthKeys.slice(0, 3).reduce((s, k) => s + (empMonthly[k] ? empMonthly[k].qtd : 0), 0);
        const prev3 = recentMonthKeys.slice(3, 6).reduce((s, k) => s + (empMonthly[k] ? empMonthly[k].qtd : 0), 0);
        if (prev3 > 5 && last3 < prev3 * 0.5) { churnScore += 30; churnMotivos.push('Queda >50% nos pedidos'); }
        else if (prev3 > 5 && last3 < prev3 * 0.7) { churnScore += 15; churnMotivos.push('Queda >30% nos pedidos'); }
        const currentMonth = recentMonthKeys[0];
        if (e.pedidos > 0 && (!empMonthly[currentMonth] || empMonthly[currentMonth].qtd === 0)) { churnScore += 25; churnMotivos.push('Zero pedidos no mês atual'); }
        if (e.pedidos > 10 && e.pedidosCancelados > e.pedidosPagos * 0.3) { churnScore += 15; churnMotivos.push('Alto cancelamento'); }
        if (e.temIntegracao !== 'Sim') { churnScore += 10; churnMotivos.push('Sem integração'); }
        if (oraculoEtapa === 'Churn') { churnScore += 30; churnMotivos.push('Oráculo: Churn'); }
        else if (oraculoEtapa === 'Parado') { churnScore += 20; churnMotivos.push('Oráculo: Parado'); }
        churnScore = Math.min(churnScore, 100);
        const churnRisco = churnScore >= 60 ? 'Alto' : churnScore >= 30 ? 'Médio' : 'Baixo';

        const temVestiPago = vestiPagoSet.has(e.id);

        // Oráculo: gate por CONFIG (o-configurations com n8n_url = clientes Oráculo reais,
        // ~88 marcas). oraculo_Pedidos no BQ cobre TODAS as marcas (não só Oráculo), então
        // só anexamos stats quando a marca é cliente Oráculo (tem config).
        const oraculoConfig = oraculoConfigMap.get(e.id) || null;
        const oraculoStats = oraculoConfig ? (oraculoStatsMap.get(e.id) || null) : null;

        // Filiais
        const groupRoot = String(e.idDominio || e.id);
        const filiaisGroup = filialGroups[groupRoot] || [];
        const isMatriz = matrizIds.has(e.id);
        const matrizEmp = filiaisGroup.find(f => matrizIds.has(f.id));
        const matrizId = matrizEmp ? matrizEmp.id : e.id;
        const filiais = filiaisGroup.filter(f => f.id !== e.id).map(f => ({
            nome: f.nomeFantasia || f.nomeDominio, idDominio: f.idDominio, id: f.id,
            temVestiPago: vestiPagoSet.has(f.id), isMatriz: matrizIds.has(f.id),
        })).sort((a, b) => { if (a.isMatriz && !b.isMatriz) return -1; if (!a.isMatriz && b.isMatriz) return 1; return a.nome.localeCompare(b.nome); });

        return {
            i: idx, id: e.id, idDominio: e.idDominio, nome, canal: e.canal,
            cartao: e.cartaoImpl ? 'Sim' : 'Não', pix: e.pixImpl ? 'Sim' : 'Não', cnpj: e.cnpj,
            temVestiPago, vestiPagoTransacionando: temVestiPago && (e.transCartao + e.transPix) > 0,
            transCartao: e.transCartao, transPix: e.transPix, transTotal: e.transTotal,
            valCartao: Math.round(e.valCartao * 100) / 100, valPix: Math.round(e.valPix * 100) / 100,
            valTotal: Math.round(e.valTotal * 100) / 100, gmv: Math.round(e.valTotal * 100) / 100,
            pedidos: e.pedidos, pedidosPagos: e.pedidosPagos, pedidosCancelados: e.pedidosCancelados, pedidosPendentes: e.pedidosPendentes,
            valPedidosPagos: Math.round(e.valPedidosPagos * 100) / 100, valPedidosCancelados: Math.round(e.valPedidosCancelados * 100) / 100,
            valPedidosPendentes: Math.round(e.valPedidosPendentes * 100) / 100,
            linksEnviados: (isMatriz || filiaisGroup.length <= 1) ? (linksByCompany[e.id] || 0) : 0,
            cliques: (isMatriz || filiaisGroup.length <= 1) ? (cliquesByCompany[e.id] || 0) : 0,
            anjo: e.anjo, modulo: e.modulo, tags: e.tags, temIntegracao: e.temIntegracao,
            integracao: e.integracao || '', tipoIntegracao: e.tipoIntegracao, criacao: e.criacao, valorPlano: e.valorPlano,
            plano: marca ? marca.plano : '', planoMensalidade: marca ? marca.mensalidade : 0,
            planoIntegracao: marca ? marca.integracao : 0, planoAssistente: marca ? marca.assistente : 0,
            planoFilial: marca ? marca.filial : 0, planoDescontos: marca ? marca.descontos : 0,
            planoTotalCobrado: marca ? marca.totalCobrado : 0, planoSetup: marca ? marca.setup : 0,
            planoObservacoes: marca ? marca.observacoes : '', planoSubconta: marca ? marca.subconta : '',
            planos: marca && marca.planos ? marca.planos : undefined,
            marcaAtiva: e.statusEmpresa === 'Ativa' ? 'Sim' : e.statusEmpresa === 'Desativada' ? 'Não' : '',
            mensalidade, etapaHub, oraculoEtapa,
            temOraculoFabric: !!(oraculoStats || oraculoConfig),
            oraculoFabric: (oraculoStats || oraculoConfig) ? {
                ...(oraculoConfig || {}),
                pedidosOraculo: oraculoStats ? oraculoStats.pedidosOraculo : 0,
                interacoesOraculo: oraculoStats ? oraculoStats.interacoesOraculo : 0,
                atendimentosOraculo: oraculoStats ? oraculoStats.atendimentosOraculo : 0,
                pctIAOraculo: oraculoStats ? oraculoStats.pctIAOraculo : 0,
                vendasOraculo: oraculoStats ? oraculoStats.vendasOraculo : 0,
                vendasMensal: oraculoStats ? oraculoStats.vendasMensal : undefined,
                pedidosMensal: oraculoStats ? oraculoStats.pedidosMensal : undefined,
                vendasSemanal: oraculoStats ? oraculoStats.vendasSemanal : undefined,
                interacoesSemanal: oraculoStats ? oraculoStats.interacoesSemanal : undefined,
                eventosSemanal: oraculoStats ? oraculoStats.eventosSemanal : undefined,
            } : undefined,
            usuario: ctrl ? ctrl.usuario : '', senha: ctrl ? ctrl.senha : '',
            churnScore, churnRisco, churnMotivos: churnMotivos.length > 0 ? churnMotivos.join('; ') : '',
            statusEmpresa: e.statusEmpresa || '', controleEstoque: '',
            naoPagos: e.pedidosPendentes,
            csat: (() => { const nk = normSimple(nome); if (csatByEmpresa[nk]) return csatByEmpresa[nk]; for (const [ck, cv] of Object.entries(csatByEmpresa)) if (ck.length >= 4 && nk.startsWith(ck)) return cv; })(),
            nps: npsMap[String(e.idDominio)] != null ? npsMap[String(e.idDominio)] : undefined,
            isMatriz: filiaisGroup.length > 1 ? isMatriz : undefined,
            matrizId: filiaisGroup.length > 1 && !isMatriz ? matrizId : undefined,
            filiais: filiais.length > 0 ? filiais : undefined,
            m,
        };
    });

    // ---------- 14. Faturamento (Iugu, fatura cheia) — match CNPJ depois nome ----------
    // Uma marca costuma ter varios customer_id na Iugu (setup, assinatura, reajuste).
    // Tanto o match por CNPJ quanto o por nome precisam SOMAR todos, senao a marca fica subcontada.
    const emptyBucket = () => ({ paid:0,pending:0,expired:0,canceled:0,nPagas:0,nPendentes:0,nVencidas:0,qtd:0,faturas:[] });
    const addToBucket = (b, rec) => {
        b.paid+=rec.paid; b.pending+=rec.pending; b.expired+=rec.expired; b.canceled+=rec.canceled;
        b.nPagas+=rec.nPagas; b.nPendentes+=rec.nPendentes; b.nVencidas+=rec.nVencidas; b.qtd+=rec.qtd;
        b.faturas.push(...rec.faturas);
        return b;
    };
    const fatByCnpj = {}, fatRecs = [];
    for (const r of faturamentoRows) {
        const rec = {
            paid: num(r.paid), pending: num(r.pending), expired: num(r.expired), canceled: num(r.canceled),
            nPagas: num(r.nPagas), nPendentes: num(r.nPendentes), nVencidas: num(r.nVencidas),
            qtd: num(r.qtd), faturas: (r.faturas || []).map(f => ({ mes: f.mes, status: f.status, total: num(f.total), due: f.due, plan: f.plan || '' })),
            customer_name: r.customer_name || '',
        };
        const c = digits(r.cnpj);
        if (c.length >= 11) addToBucket(fatByCnpj[c] || (fatByCnpj[c] = emptyBucket()), rec);
        const n = normSimple(rec.customer_name);
        if (n && n.length >= 4) fatRecs.push({ nome: n, rec });
    }
    let invoiceMatched = 0;
    for (const emp of empresasList) {
        let data = null;
        const ec = digits(emp.cnpj);
        if (ec.length >= 11 && fatByCnpj[ec]) data = fatByCnpj[ec];
        if (!data) {
            const nomeNorm = normSimple(emp.nome);
            const cands = fatRecs.filter(({ nome }) => {
                const sh = Math.min(nomeNorm.length, nome.length);
                return nomeNorm === nome || (sh >= 5 && (nomeNorm.startsWith(nome) || nome.startsWith(nomeNorm)));
            });
            if (cands.length) data = cands.reduce((b, c) => addToBucket(b, c.rec), emptyBucket());
        }
        if (data) {
            const faturasSorted = [...data.faturas].sort((a, b) => (b.due || '').localeCompare(a.due || ''));
            const ultima = faturasSorted.find(f => f.status !== 'canceled') || faturasSorted[0];
            const planoIugu = (ultima && ultima.plan) || (faturasSorted[0] && faturasSorted[0].plan) || '';
            emp.faturamento = {
                planoIugu,
                totalPago: Math.round(data.paid * 100) / 100, totalPendente: Math.round(data.pending * 100) / 100,
                totalVencido: Math.round(data.expired * 100) / 100, totalCancelado: Math.round(data.canceled * 100) / 100,
                qtdFaturas: data.qtd,
                faturas: faturasSorted.slice(0, 12).map(f => ({ mes: f.mes, status: f.status, total: f.total })),
            };
            if (ultima && ultima.total > 0) {
                emp.planoMensalidade = Math.round(ultima.total * 100) / 100;
                emp.mensalidade = 'R$ ' + ultima.total.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            }
            if (ultima) {
                emp.faturaStatus = ultima.status;
                emp.faturasPagas = data.nPagas; emp.faturasPendentes = data.nPendentes; emp.faturasVencidas = data.nVencidas;
            }
            invoiceMatched++;
        }
    }
    console.log('  Faturamento matched: ' + invoiceMatched + '/' + empresasList.length);

    // ---------- 15. Dedup duplicatas inativas por CNPJ ----------
    const cnpjAtivos = new Set();
    for (const emp of empresasList) { const c = digits(emp.cnpj); if (c && (emp.pedidos || 0) > 0) cnpjAtivos.add(c); }
    const antes = empresasList.length;
    empresasList = empresasList.filter(emp => { const c = digits(emp.cnpj); if (!c) return true; if ((emp.pedidos || 0) > 0) return true; return !cnpjAtivos.has(c); });
    if (antes !== empresasList.length) console.log('  Dedup CNPJ: ' + antes + ' -> ' + empresasList.length);

    // ---------- 16. Output ----------
    const oraculoSummary = {};
    for (const t of oraculoTickets) oraculoSummary[t.stageName] = (oraculoSummary[t.stageName] || 0) + 1;
    const churnAlto = empresasList.filter(e => e.churnRisco === 'Alto').length;
    const churnMedio = empresasList.filter(e => e.churnRisco === 'Médio').length;

    const output = {
        empresas: empresasList, mensal: monthlyData, meses: allMonths,
        totalEmpresas: empresasList.length, oraculoSummary,
        oraculoTickets: oraculoTickets.map(t => ({ nome: t.companyName, etapa: t.stageName, criado: t.created, atualizado: t.modified })),
        churnStats: { alto: churnAlto, medio: churnMedio, total: empresasList.length },
        linksMensaisEmp, cliquesMensaisEmp,
        geradoEm: new Date().toISOString(),
    };

    // Sanity checks
    const totalGMV = empresasList.reduce((s, e) => s + e.gmv, 0);
    const totalPedidos = empresasList.reduce((s, e) => s + e.pedidos, 0);
    const semStatus = empresasList.filter(e => !e.statusEmpresa).length;
    const pctSemStatus = empresasList.length > 0 ? semStatus / empresasList.length : 0;
    function abort(motivo) { console.error('\n*** ABORTING BUILD: ' + motivo + ' ***\n*** dados.js NÃO sobrescrito. ***'); process.exit(1); }
    if (totalPedidos === 0 && monthlyData.length === 0 && empresasList.length > 0) abort('0 pedidos, 0 meses (BQ sem dados)');
    if (pctSemStatus > 0.60) abort('>60% sem statusEmpresa (' + (pctSemStatus * 100).toFixed(1) + '%)');
    try {
        const prevPath = path.join(DIR, 'dados.js');
        if (fs.existsSync(prevPath)) {
            const prev = (new Function(fs.readFileSync(prevPath, 'utf-8') + '; return DADOS;'))();
            const prevEmp = (prev.empresas || []).length;
            const prevGMV = (prev.empresas || []).reduce((s, e) => s + (e.gmv || 0), 0);
            const prevPed = (prev.empresas || []).reduce((s, e) => s + (e.pedidos || 0), 0);
            // Piso histórico: BQ (MongoDB_Pedidos_Geral) só tem ~2025-07+; no cutover a
            // versão anterior (Fabric) tinha mais meses, então GMV/Pedidos caem uma vez.
            // Thresholds relaxados p/ 0.40; após o cutover o prev já é BQ (ratio ~1).
            if (prevEmp > 0 && empresasList.length < prevEmp * 0.80) abort('queda empresas: ' + empresasList.length + ' vs ' + prevEmp);
            if (prevGMV > 0 && totalGMV < prevGMV * 0.40) abort('queda GMV: ' + totalGMV.toFixed(0) + ' vs ' + prevGMV.toFixed(0));
            if (prevPed > 0 && totalPedidos < prevPed * 0.40) abort('queda Pedidos: ' + totalPedidos + ' vs ' + prevPed);
        }
    } catch (err) { console.warn('AVISO: comparação c/ dados.js anterior falhou: ' + err.message); }

    fs.writeFileSync(path.join(DIR, 'dados.js'), 'const DADOS = ' + JSON.stringify(output), 'utf-8');

    console.log('\n=== RESULT ===');
    console.log('Empresas: ' + empresasList.length);
    console.log('Meses (global): ' + monthlyData.length);
    console.log('Total GMV: R$ ' + totalGMV.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
    console.log('Total Pedidos: ' + totalPedidos.toLocaleString('pt-BR'));
    console.log('Total Links: ' + empresasList.reduce((s, e) => s + e.linksEnviados, 0).toLocaleString('pt-BR'));
    console.log('Faturamento: ' + invoiceMatched + ' | Oráculo: ' + empresasList.filter(e => e.temOraculoFabric).length);
    console.log('Churn: ' + churnAlto + ' alto, ' + churnMedio + ' médio');
    console.log('\nDone.');
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
