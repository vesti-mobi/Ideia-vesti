/**
 * Regera integracoes.json (mapa de plataforma de integração por companyId/domínio/CNPJ)
 * a partir de "Cadastros Empresas.csv". O CSV está no .gitignore e não existe no runner
 * do GitHub Actions, então o build lê este snapshot commitado.
 *
 * Uso (LOCAL, com o CSV na pasta):  node _refresh_integracoes.js
 */
const fs = require('fs');
const F = require('./bq-fetchers.js');
const { digits } = F;

(async () => {
  const byCompany = {}, byDominio = {}, byCnpj = {};
  await F.readCSV('Cadastros Empresas.csv', (row) => {
    const v = (row['Integração'] || row['Integracao'] || '').trim();
    if (!v) return;
    const id = row['Id Empresa'], dom = row['Id Dominio'], c = digits(row['CNPJ']);
    if (id) byCompany[id] = v;
    if (dom) byDominio[String(dom)] = v;
    if (c.length >= 11 && !byCnpj[c]) byCnpj[c] = v;
  });
  const out = { _fonte: 'Cadastros Empresas.csv (col Integração)', byCompany, byDominio, byCnpj };
  fs.writeFileSync('integracoes.json', JSON.stringify(out, null, 1));
  console.log('integracoes.json: companies=' + Object.keys(byCompany).length +
    ' dominios=' + Object.keys(byDominio).length + ' cnpjs=' + Object.keys(byCnpj).length);
})().catch((e) => { console.error(e); process.exit(1); });
