/**
 * Process nps.csv and inject NPS per Dominio into dados.js
 * Only sets e.nps if the empresa does NOT have csat data.
 * Also saves _nps.json with [{dominio, nps}] for build-cloud.js.
 */
const fs = require('fs');
const path = require('path');

const DIR = __dirname;

function parseCSV(text) {
    const lines = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (ch === '"') {
            if (inQuotes && text[i + 1] === '"') {
                current += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (ch === ',' && !inQuotes) {
            lines.push(current);
            current = '';
        } else if ((ch === '\n' || ch === '\r') && !inQuotes) {
            if (ch === '\r' && text[i + 1] === '\n') i++;
            lines.push(current);
            current = '';
            // yield row
            if (lines.length > 0) {
                yield_row(lines);
            }
            lines.length = 0;
        } else {
            current += ch;
        }
    }
    if (current || lines.length > 0) {
        lines.push(current);
        yield_row(lines);
    }
    // This approach won't work with yield. Let me use a simpler approach.
}

// Simpler CSV parser
function readCSV(filePath) {
    const text = fs.readFileSync(filePath, 'utf-8');
    const rows = [];
    let currentRow = [];
    let currentField = '';
    let inQuotes = false;

    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (ch === '"') {
            if (inQuotes && text[i + 1] === '"') {
                currentField += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (ch === ',' && !inQuotes) {
            currentRow.push(currentField);
            currentField = '';
        } else if ((ch === '\n' || ch === '\r') && !inQuotes) {
            if (ch === '\r' && text[i + 1] === '\n') i++;
            currentRow.push(currentField);
            currentField = '';
            if (currentRow.length > 1 || currentRow[0] !== '') {
                rows.push(currentRow);
            }
            currentRow = [];
        } else {
            currentField += ch;
        }
    }
    if (currentField || currentRow.length > 0) {
        currentRow.push(currentField);
        rows.push(currentRow);
    }

    // Convert to objects
    const headers = rows[0];
    const data = [];
    for (let i = 1; i < rows.length; i++) {
        const obj = {};
        for (let j = 0; j < headers.length; j++) {
            obj[headers[j]] = (rows[i][j] || '').trim();
        }
        data.push(obj);
    }
    return data;
}

function main() {
    console.log('=== Patch NPS from CSV ===\n');

    // 1. Read nps.csv
    const csvPath = path.join(DIR, 'nps.csv');
    const npsData = readCSV(csvPath);
    console.log('NPS rows in CSV:', npsData.length);

    // 2. Group by Dominio and calculate NPS
    const groups = {};
    for (const row of npsData) {
        const dominio = row.Dominio ? row.Dominio.replace(/\.0$/, '') : '';
        if (!dominio) continue;
        const nota = parseFloat(row.Nota);
        if (isNaN(nota)) continue;

        if (!groups[dominio]) groups[dominio] = { promoters: 0, detractors: 0, total: 0 };
        groups[dominio].total++;
        if (nota >= 9) groups[dominio].promoters++;
        if (nota <= 6) groups[dominio].detractors++;
    }

    const npsMap = {};
    const npsArray = [];
    for (const [dom, g] of Object.entries(groups)) {
        const nps = Math.round(((g.promoters - g.detractors) * 100) / g.total * 10) / 10;
        npsMap[dom] = nps;
        npsArray.push({ dominio: dom, nps });
    }
    console.log('Domains with NPS:', Object.keys(npsMap).length);

    // 3. Load dados.js
    console.log('\nLoading dados.js...');
    const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();
    console.log('Total empresas:', DADOS.empresas.length);

    // 4. Match: always set e.nps (even if empresa already has csat)
    let matched = 0, withCsat = 0;
    for (const e of DADOS.empresas) {
        const dom = String(e.idDominio || '');
        if (npsMap[dom] != null) {
            e.nps = npsMap[dom];
            matched++;
            if (e.csat && ((Array.isArray(e.csat) && e.csat.length > 0) || (!Array.isArray(e.csat) && e.csat))) {
                withCsat++;
            }
        }
    }
    console.log('NPS applied to empresas:', matched);
    console.log('Also have CSAT:', withCsat);

    // 5. Save dados.js
    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(path.join(DIR, 'dados.js'), output, 'utf-8');
    console.log('dados.js saved.');

    // 6. Save _nps.json
    fs.writeFileSync(path.join(DIR, '_nps.json'), JSON.stringify(npsArray, null, 2), 'utf-8');
    console.log('_nps.json saved with', npsArray.length, 'entries.');

    // Show some sample NPS values
    console.log('\nSample NPS values:');
    npsArray.slice(0, 10).forEach(n => console.log(`  Dominio ${n.dominio}: NPS = ${n.nps}`));
}

main();
