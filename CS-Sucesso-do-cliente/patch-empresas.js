/**
 * Sincroniza contagens de empresas (matriz / filial / total) com as do
 * PainelCSGerencial. Fonte: ../PainelCSGerencial/companies_data.json
 * (gerado por fetch_fabric.py — Lakehouse VestiHouse, Fabric).
 *
 * Grava DADOS.fabricCounts = {matriz, filial, total} em dados.js. O
 * index.html usa esse objeto para renderizar os KPIs do topo, garantindo
 * número idêntico ao PainelCSGerencial.
 */
const fs = require('fs');
const path = require('path');

const DIR = __dirname;
const COMPANIES = path.resolve(DIR, '..', 'PainelCSGerencial', 'companies_data.json');
const DADOS_PATH = path.join(DIR, 'dados.js');

function main() {
    if (!fs.existsSync(COMPANIES)) {
        console.error('Arquivo nao encontrado: ' + COMPANIES);
        process.exit(1);
    }
    const list = JSON.parse(fs.readFileSync(COMPANIES, 'utf-8'));
    let matriz = 0, filial = 0;
    list.forEach(c => {
        if (c.isMatriz || c.is_filial === false) matriz++;
        else if (c.is_filial || c.isMatriz === false) filial++;
    });
    const total = matriz + filial;
    console.log('[fabric] matriz=' + matriz + ' filial=' + filial + ' total=' + total);

    const content = fs.readFileSync(DADOS_PATH, 'utf-8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();
    DADOS.fabricCounts = { matriz, filial, total };

    fs.writeFileSync(DADOS_PATH, 'const DADOS = ' + JSON.stringify(DADOS), 'utf-8');
    console.log('[ok] dados.js atualizado com DADOS.fabricCounts');
}
main();
