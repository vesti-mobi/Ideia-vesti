/**
 * carregar_tipo_empresa.js — leva a classificação Atacado × Varejo para o BigQuery.
 *
 * POR QUE ISSO EXISTE
 * A regra "varejo novo" da aba de Bonificação precisa saber se uma filial é de
 * varejo ou de atacado. Essa marcação existe e é oficial, mas mora no Fabric
 * (`dbo.Confeccao2025_Query1`, coluna "Tipo _Atacado | Varejo_") e NÃO estava no
 * BigQuery — nenhum campo do espelho `odbc_*` diz isso: `lojista` vem false em
 * todas as filiais, `market` vem nulo, `parent_id` nunca é preenchido, as 18
 * tags são de segmento de moda (jeans, fitness, praia) e o canal "Varejo Vesti"
 * está zerado desde jul/2025.
 *
 * FONTES, EM ORDEM DE PREFERÊNCIA
 *   1. Fabric, direto da tabela (--fabric). É o dado vivo.
 *   2. CSV local `CS-Sucesso-do-cliente/Cadastros Empresas.csv`, derivado dela e
 *      congelado em 30/03/2026 — o fallback de hoje, porque em 26/08/2026 o
 *      warehouse do Fabric não respondia nem a um SELECT 1 ("Couldn't complete
 *      the operation due to a system update", em 6 tentativas com reconexão).
 *
 * A tabela de destino é recriada por inteiro (WRITE_TRUNCATE): é um cadastro,
 * não um histórico. Quando o Fabric voltar, rodar com --fabric atualiza tudo e a
 * aba de Bonificação melhora sozinha, sem mexer em mais nada.
 *
 * Rodar:  node carregar_tipo_empresa.js [--fabric]
 */
const fs = require('fs');
const path = require('path');

const RAIZ = path.resolve(__dirname, '..');
const SA_KEY = process.env.GOOGLE_APPLICATION_CREDENTIALS
  || 'C:/Users/Laura/Downloads/vesti-data-499015-7ea468dae45e.json';
process.env.GOOGLE_APPLICATION_CREDENTIALS = SA_KEY;
const { BigQuery } = require(path.join(RAIZ, 'node_modules/@google-cloud/bigquery'));
const bq = new BigQuery({ projectId: 'vesti-data-499015' });

const PROJETO = 'vesti-data-499015';
const DATASET = 'vestilake_BI';
const TABELA = 'confeccao_tipo_empresa';
const CSV = path.join(RAIZ, 'CS-Sucesso-do-cliente', 'Cadastros Empresas.csv');

const ESQUEMA = [
  { name: 'id_empresa', type: 'STRING' },
  { name: 'id_dominio', type: 'STRING' },
  { name: 'nome_dominio', type: 'STRING' },
  { name: 'nome_fantasia', type: 'STRING' },
  { name: 'razao_social', type: 'STRING' },
  { name: 'tipo', type: 'STRING' },          // 'Atacado' | 'Varejo' | null
  { name: 'canal', type: 'STRING' },
  { name: 'fonte', type: 'STRING' },         // de onde veio esta carga
  { name: 'atualizado_em', type: 'DATE' },   // data do DADO, não da carga
];

/* CSV com aspas: o arquivo tem razão social com vírgula dentro ("X COMERCIO,
   INDUSTRIA LTDA"), então split(',') corromperia as colunas seguintes. */
function lerCSV(texto) {
  const linhas = [];
  let campo = '', linha = [], dentroDeAspas = false;
  for (let i = 0; i < texto.length; i++) {
    const c = texto[i];
    if (dentroDeAspas) {
      if (c === '"' && texto[i + 1] === '"') { campo += '"'; i++; }
      else if (c === '"') dentroDeAspas = false;
      else campo += c;
    } else if (c === '"') dentroDeAspas = true;
    else if (c === ',') { linha.push(campo); campo = ''; }
    else if (c === '\n') { linha.push(campo); linhas.push(linha); linha = []; campo = ''; }
    else if (c !== '\r') campo += c;
  }
  if (campo || linha.length) { linha.push(campo); linhas.push(linha); }
  return linhas;
}

function doCSV() {
  const texto = fs.readFileSync(CSV, 'utf8').replace(/^\uFEFF/, '');
  const linhas = lerCSV(texto).filter(l => l.length > 1);
  const cab = linhas[0].map(s => s.trim());
  const col = nome => cab.indexOf(nome);
  const iId = col('Id Empresa'), iTipo = col('Tipo Atacado  Varejo');
  if (iId < 0 || iTipo < 0) throw new Error('o CSV nao tem "Id Empresa" / "Tipo Atacado  Varejo"');
  const iDom = col('Id Dominio'), iNomeDom = col('Nome do Dominio'),
        iFant = col('Nome Fantasia'), iRazao = col('Razao Social'), iCanal = col('Canal de Vendas');
  /* A data do dado é a da última modificação do arquivo: é ela que diz até
     quando a classificação cobre, e é o que a aba precisa avisar. */
  const dataDoArquivo = fs.statSync(CSV).mtime.toISOString().slice(0, 10);
  const pega = (c, i) => (i >= 0 && c[i] ? String(c[i]).trim() : '') || null;

  const vistos = new Set(), saida = [];
  for (let i = 1; i < linhas.length; i++) {
    const c = linhas[i];
    const id = pega(c, iId);
    if (!id || vistos.has(id)) continue;
    vistos.add(id);
    saida.push({
      id_empresa: id,
      id_dominio: pega(c, iDom),
      nome_dominio: pega(c, iNomeDom),
      nome_fantasia: pega(c, iFant),
      razao_social: pega(c, iRazao),
      tipo: pega(c, iTipo),
      canal: pega(c, iCanal),
      fonte: 'CS-Sucesso/Cadastros Empresas.csv (derivado de dbo.Confeccao2025_Query1)',
      atualizado_em: dataDoArquivo,
    });
  }
  return saida;
}

/* Leitura direta do Fabric, para quando o warehouse voltar. Reautentica a cada
   tentativa: o erro "system update" pede conexão nova, não só um retry. */
async function doFabric() {
  const vm = require('vm');
  const arq = path.join(RAIZ, 'CS-Sucesso-do-cliente', 'build-cloud.js');
  let src = fs.readFileSync(arq, 'utf8');
  src = src.slice(0, src.indexOf('async function main()'))
      + '\nmodule.exports={getSqlAccessToken,runSqlQuery};\n';
  const m = { exports: {} };
  const cwd = process.cwd();
  process.chdir(path.dirname(arq));   // o build-cloud.js lê o .env pelo cwd
  vm.runInNewContext(src, { module: m, exports: m.exports, require, process, console,
    __dirname: path.dirname(arq), Buffer, setTimeout, clearTimeout, URLSearchParams });
  process.chdir(cwd);

  const SQL = 'SELECT [Id Empresa] AS id_empresa, [Id Dominio] AS id_dominio,'
            + ' [Nome do Dominio] AS nome_dominio, [Nome Fantasia] AS nome_fantasia,'
            + ' [Razao Social] AS razao_social, [Tipo _Atacado | Varejo_] AS tipo,'
            + ' [Canal de Vendas] AS canal'
            + ' FROM dbo.Confeccao2025_Query1';
  let ultimo;
  for (let i = 1; i <= 5; i++) {
    try {
      const token = await m.exports.getSqlAccessToken();
      const rows = await m.exports.runSqlQuery(token, SQL, 'Confeccao2025_Query1', 180000);
      const hoje = new Date().toISOString().slice(0, 10);
      return rows.map(r => Object.assign({}, r, {
        fonte: 'Fabric dbo.Confeccao2025_Query1', atualizado_em: hoje,
      }));
    } catch (e) {
      ultimo = e;
      console.log('  Fabric, tentativa ' + i + ': ' + e.message.slice(0, 80));
      await new Promise(r => setTimeout(r, 6000 * i));
    }
  }
  throw ultimo;
}

(async () => {
  const usarFabric = process.argv.includes('--fabric');
  console.log('Classificacao Atacado x Varejo -> ' + DATASET + '.' + TABELA);

  const linhas = usarFabric ? await doFabric() : doCSV();
  if (!usarFabric) console.log('  fonte: CSV local (use --fabric quando o warehouse voltar)');
  if (!linhas.length) throw new Error('nenhuma linha para carregar');

  const varejo = linhas.filter(l => l.tipo === 'Varejo').length;
  const atacado = linhas.filter(l => l.tipo === 'Atacado').length;
  console.log('  ' + linhas.length + ' empresas / ' + varejo + ' varejo / ' + atacado + ' atacado');
  console.log('  dado de ' + (linhas[0] || {}).atualizado_em);

  const arqTmp = path.join(__dirname, '_tipo_empresa.ndjson');
  fs.writeFileSync(arqTmp, linhas.map(l => JSON.stringify(l)).join('\n') + '\n', 'utf8');
  try {
    const [job] = await bq.dataset(DATASET).table(TABELA).load(arqTmp, {
      sourceFormat: 'NEWLINE_DELIMITED_JSON',
      schema: { fields: ESQUEMA },
      writeDisposition: 'WRITE_TRUNCATE',
    });
    const erros = (job.status || {}).errors;
    if (erros && erros.length) throw new Error(JSON.stringify(erros.slice(0, 2)));
    console.log('  carregado.');
  } finally {
    if (fs.existsSync(arqTmp)) fs.unlinkSync(arqTmp);
  }

  const [conf] = await bq.query({ query:
    'SELECT IFNULL(tipo,"(sem tipo)") tipo, COUNT(*) n FROM `'
    + PROJETO + '.' + DATASET + '.' + TABELA + '` GROUP BY 1 ORDER BY n DESC' });
  console.table(conf);
})().catch(e => { console.error('\nFALHOU: ' + e.message); process.exit(1); });
