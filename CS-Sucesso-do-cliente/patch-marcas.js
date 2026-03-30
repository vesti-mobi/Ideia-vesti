/**
 * Patch dados.js com dados completos de Marcas e Planos do Excel.
 * Guarda TODAS as linhas por empresa (plano + Oráculo + outros) no campo `planos`.
 * Os campos plano* ficam com os valores da linha principal (não-Oráculo).
 * Match por: CNPJ exato -> raiz CNPJ -> nome exato -> nome parcial.
 */
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const DIR = __dirname;

function normalize(s) { return s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim(); }

function extractSheetDate(name) {
    let m = name.match(/(\d{2})-?(\d{4})/); if (m) return m[2] + '-' + m[1];
    m = name.match(/(\d{2})-?(\d{2})$/); if (m) return '20' + m[2] + '-' + m[1]; return '0000-00';
}

function parseLine(row) {
    return {
        marca: row['MARCA'] || '',
        plano: (row['PLANO'] || '').trim(),
        setup: parseFloat(row['SETUP']) || 0,
        mensalidade: parseFloat(row['MENSALIDADE']) || 0,
        integracao: parseFloat(row['INTEGRAÇÃO'] || row['INTEGRACAO']) || 0,
        assistente: parseFloat(row['ASSISTENTE']) || 0,
        filial: parseFloat(row['FILIAL']) || 0,
        descontos: parseFloat(row['DESCONTOS']) || 0,
        totalCobrado: parseFloat(row['TOTAL COBRADO'] || row['TOTAL_COBRADO']) || 0,
        observacoes: row['OBSERVAÇÕES'] || row['OBSERVACOES'] || '',
        subconta: row['Subconta'] || '',
    };
}

// 1. Read Excel
const wb = XLSX.readFile(path.join(DIR, 'Marcas e Planos.xlsx'));
const vestiSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('vesti') && !s.toLowerCase().includes('starter'));
const starterSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('starter'));
vestiSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));
starterSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));
const sheetsToRead = [];
if (starterSheets.length > 0) sheetsToRead.push(starterSheets[0]);
if (vestiSheets.length > 0) sheetsToRead.push(vestiSheets[0]);

// Collect ALL lines per CNPJ
const allLinesByCnpj = {};
for (const sheetName of sheetsToRead) {
    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
    for (const row of rows) {
        const cnpj = String(row['CPFCNPJ'] || row['CPF e CNPJ'] || '').replace(/[.\-\/\s]/g, '');
        if (!cnpj || cnpj.length < 11) continue;
        if (!allLinesByCnpj[cnpj]) allLinesByCnpj[cnpj] = [];
        allLinesByCnpj[cnpj].push(parseLine(row));
    }
    console.log('Sheet "' + sheetName + '"');
}

// 2. Build per-CNPJ data: main plan fields + planos array
const isExtra = (p) => /oraculo|oráculo|integração|integracao|pacote/i.test(p);

const dataByCnpj = {};
for (const [cnpj, lines] of Object.entries(allLinesByCnpj)) {
    const main = lines.find(l => !isExtra(l.plano)) || lines[0];
    dataByCnpj[cnpj] = {
        // Main plan fields (for table/KPIs/export)
        marca: main.marca,
        plano: main.plano,
        setup: main.setup,
        mensalidade: main.mensalidade,
        integracao: main.integracao,
        assistente: main.assistente,
        filial: main.filial,
        descontos: main.descontos,
        totalCobrado: main.totalCobrado,
        observacoes: main.observacoes,
        subconta: main.subconta,
        // All lines for financeiro detail
        planos: lines.map(l => ({
            plano: l.plano,
            mensalidade: l.mensalidade,
            integracao: l.integracao,
            assistente: l.assistente,
            filial: l.filial,
            descontos: l.descontos,
            totalCobrado: l.totalCobrado,
            setup: l.setup,
        })),
    };
}
console.log('CNPJs:', Object.keys(dataByCnpj).length);

// By-name map (keep highest totalCobrado)
const dataByName = {};
for (const d of Object.values(dataByCnpj)) {
    const name = normalize(d.marca);
    if (name && name.length >= 3) {
        if (!dataByName[name] || d.totalCobrado > dataByName[name].totalCobrado) dataByName[name] = d;
    }
}

// 3. Load dados.js
console.log('\nCarregando dados.js...');
const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf8');
const fn = new Function(content + '; return DADOS;');
const DADOS = fn();

// 4. Match and patch
let byCnpj = 0, byRoot = 0, byName = 0, byPartial = 0;
for (const e of DADOS.empresas) {
    const cnpj = (e.cnpj || '').replace(/[.\-\/]/g, '');
    const nomeNorm = normalize(e.nome || '');

    let data = dataByCnpj[cnpj];
    if (data) { byCnpj++; }

    if (!data && cnpj.length >= 8) {
        const root = cnpj.substring(0, 8);
        let best = null;
        for (const [mcnpj, mdata] of Object.entries(dataByCnpj)) {
            if (mcnpj.substring(0, 8) === root) {
                if (!best || mdata.totalCobrado > best.totalCobrado) best = mdata;
            }
        }
        if (best) { data = best; byRoot++; }
    }

    if (!data) { data = dataByName[nomeNorm]; if (data) byName++; }

    if (!data && nomeNorm.length >= 5) {
        for (const [mName, mData] of Object.entries(dataByName)) {
            if (mName.length >= 5 && (nomeNorm.includes(mName) || mName.includes(nomeNorm))) {
                data = mData; byPartial++; break;
            }
        }
    }

    if (data) {
        e.plano = data.plano || e.plano || '';
        e.planoMensalidade = data.mensalidade;
        e.planoIntegracao = data.integracao;
        e.planoAssistente = data.assistente;
        e.planoFilial = data.filial;
        e.planoDescontos = data.descontos;
        e.planoTotalCobrado = data.totalCobrado;
        e.planoSetup = data.setup;
        e.planoObservacoes = data.observacoes;
        e.planoSubconta = data.subconta;
        // Array com todas as linhas de plano (para aba financeiro)
        if (data.planos.length > 1) {
            e.planos = data.planos;
        } else {
            delete e.planos; // single line, not needed
        }
    }
}

const total = byCnpj + byRoot + byName + byPartial;
console.log('Matched:', total, '(CNPJ:', byCnpj, '| Root:', byRoot, '| Name:', byName, '| Partial:', byPartial, ')');
console.log('Com múltiplos planos:', DADOS.empresas.filter(e => e.planos).length);

console.log('\nExemplos:');
['aero summer 1', 'pury', 'tricomix', 'diamantes matriz', 'bauarte', 'kelly rodrigues store'].forEach(n => {
    const m = DADOS.empresas.filter(e => e.nome.toLowerCase().includes(n));
    m.slice(0, 1).forEach(e => {
        console.log('  ' + e.nome + ' | plano:' + e.plano + ' mensal:' + e.planoMensalidade + ' total:' + e.planoTotalCobrado);
        if (e.planos) e.planos.forEach(p => console.log('    -> ' + p.plano + ' mensal:' + p.mensalidade + ' assist:' + p.assistente + ' filial:' + p.filial + ' desc:' + p.descontos + ' total:' + p.totalCobrado));
    });
});

// 5. Save
const output = 'const DADOS = ' + JSON.stringify(DADOS);
fs.writeFileSync(path.join(DIR, 'dados.js'), output, 'utf-8');
console.log('\ndados.js salvo (' + (output.length / 1024 / 1024).toFixed(1) + ' MB)');
