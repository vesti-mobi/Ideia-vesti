/**
 * sincronizar_cs.js — corrige o CS responsável (angel_id) de
 * vestilake_BI.odbc_domains ANTES da carga do painel, comparando com o
 * Postgres de produção da Vesti via Metabase.
 *
 * Por quê (achado em 04/09/2026, com a Laura): a troca de CS responsável no
 * admin da Vesti (campo `angel_id` de `odbc_domains`) às vezes não atualiza
 * o `updated_at` da linha. A réplica Vesti -> BigQuery usa esse campo pra
 * saber o que precisa sincronizar — então a troca fica invisível pra ela
 * para sempre, até algo mais tocar aquela linha. Corrigido na mão nesse dia
 * (24 marcas divergentes na carteira ativa, entre elas a MissManu). Este
 * script automatiza a MESMA correção todo dia, direto na fonte — o que
 * beneficia qualquer outro painel da Vesti que leia `odbc_domains`, não só
 * este.
 *
 * Fonte extra: Metabase, banco "Vesti" (Postgres de produção, id descoberto
 * por nome — não fixo, porque IDs de banco no Metabase não são estáveis
 * entre ambientes), tabelas `public.domains` e `public.angels`. O escopo é
 * o MESMO da carteira que o fetch_dados.js usa (`modulos` contém "vendas"
 * OU está em DOMINIOS_EXTRA, sem "teste"/"andressa vesti" no nome) — varrer
 * o odbc_domains inteiro (2+ milhões de linhas, maioria loja de teste ou só
 * compradora) não serve pra nada aqui e seria lento à toa.
 *
 * Escreve em `odbc_domains` só as linhas onde `angel_id` diverge — nunca a
 * tabela inteira. Se faltar METABASE_URL/METABASE_API_KEY, ou qualquer
 * etapa falhar, o script avisa e sai sem travar a carga: o painel segue com
 * o espelho do jeito que já estava, igual ao comportamento do Tino/HubSpot
 * em fetch_dados.js quando a fonte deles cai.
 *
 * Rodar:  node sincronizar_cs.js   (antes de node fetch_dados.js)
 */

const path = require('path');

const RAIZ = path.resolve(__dirname, '..');
const SA_KEY = process.env.GOOGLE_APPLICATION_CREDENTIALS
  || 'C:/Users/Laura/Downloads/vesti-data-499015-7ea468dae45e.json';
process.env.GOOGLE_APPLICATION_CREDENTIALS = SA_KEY;

const { BigQuery } = require(path.join(RAIZ, 'node_modules/@google-cloud/bigquery'));
const bq = new BigQuery({ projectId: 'vesti-data-499015' });
const DS = '`vesti-data-499015.vestilake_BI`';

const METABASE_URL = (process.env.METABASE_URL || '').replace(/\/+$/, '');
const METABASE_API_KEY = process.env.METABASE_API_KEY;

/* Mesma lista do fetch_dados.js (domínios sem "vendas" nos módulos, mas que
   são marca de verdade — ver o comentário lá). Duplicada aqui de propósito:
   este script roda ANTES e não importa fetch_dados.js, pra não arriscar
   disparar a carga inteira só por causa desta correção. */
const DOMINIOS_EXTRA = ['1593235', '1833676'];

const aspas = v => "'" + String(v).replace(/\\/g, '\\\\').replace(/'/g, "\\'") + "'";

async function mbGet(caminho) {
  const r = await fetch(METABASE_URL + caminho, { headers: { 'x-api-key': METABASE_API_KEY } });
  const j = await r.json();
  if (!r.ok) throw new Error('Metabase GET ' + caminho + ': ' + r.status + ' ' + JSON.stringify(j).slice(0, 200));
  return j;
}
async function mbPost(caminho, corpo) {
  const r = await fetch(METABASE_URL + caminho, {
    method: 'POST',
    headers: { 'x-api-key': METABASE_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify(corpo),
  });
  const j = await r.json();
  if (!r.ok || j.error) throw new Error('Metabase POST ' + caminho + ': ' + (j.error || r.status) + ' ' + JSON.stringify(j).slice(0, 300));
  return j;
}

async function buscarDatabaseId() {
  const j = await mbGet('/api/database');
  const lista = j.data || j;
  const db = lista.find(d => /^vesti$/i.test(d.name) && d.engine === 'postgres');
  if (!db) throw new Error('banco "Vesti" (postgres) não encontrado no Metabase — nome ou engine mudou?');
  return db.id;
}

async function main() {
  if (!METABASE_URL || !METABASE_API_KEY) {
    console.log('[sincronizar_cs] sem METABASE_URL/METABASE_API_KEY — pulando, angel_id fica como está no espelho');
    return;
  }
  console.log('\n[sincronizar_cs] CS responsável: comparando produção (Metabase) x espelho (BigQuery)');

  const dbId = await buscarDatabaseId();

  /* d.modulos é jsonb na produção (texto no espelho BigQuery) — precisa de
     ::text explícito, senão o Postgres tenta ler '' como JSON e quebra. */
  const query = `SELECT d.id, d.angel_id, d.updated_at
                  FROM public.domains d
                  WHERE (LOWER(COALESCE(d.modulos::text,'')) LIKE '%vendas%'
                         OR d.id IN (${DOMINIOS_EXTRA.join(',')}))
                    AND LOWER(COALESCE(d.name,'')) NOT LIKE '%teste%'
                    AND LOWER(COALESCE(d.name,'')) NOT LIKE '%andressa vesti%'`;
  const resp = await mbPost('/api/dataset', { database: dbId, type: 'native', native: { query } });
  const cols = resp.data.cols.map(c => c.name);
  const producao = resp.data.rows.map(r => Object.fromEntries(cols.map((c, i) => [c, r[i]])));
  console.log('  domínios na produção (carteira ativa)'.padEnd(44) + String(producao.length).padStart(8));
  if (!producao.length) { console.log('  Metabase devolveu 0 linhas — não mexo em nada'); return; }

  const ids = producao.map(p => "'" + String(p.id) + "'").join(',');
  const [bqRows] = await bq.query(
    `SELECT CAST(ID AS STRING) id, angel_id, updated_At
     FROM ${DS}.odbc_domains
     WHERE CAST(ID AS STRING) IN (${ids})`
  );
  const bqMap = new Map(bqRows.map(r => [r.id, r]));

  const divergentes = producao.filter(p => {
    const b = bqMap.get(String(p.id));
    if (!b) return false;   // domínio novo, ainda não replicado — a carga normal cuida
    return (p.angel_id || '') !== (b.angel_id || '');
  });
  console.log('  divergentes (angel_id diferente da produção)'.padEnd(44) + String(divergentes.length).padStart(8));
  if (!divergentes.length) { console.log('  nada para corrigir'); return; }

  const linhas = divergentes.map(p => {
    const angel = p.angel_id ? aspas(p.angel_id) : 'CAST(NULL AS STRING)';
    return `STRUCT(${aspas(p.id)} AS id, ${angel} AS angel_id, TIMESTAMP(${aspas(p.updated_at)}) AS updated_At)`;
  });
  await bq.query(`
    MERGE ${DS}.odbc_domains T
    USING (SELECT * FROM UNNEST([${linhas.join(',\n      ')}])) S
    ON CAST(T.ID AS STRING) = S.id
    WHEN MATCHED THEN UPDATE SET T.angel_id = S.angel_id, T.updated_At = S.updated_At
  `);
  console.log('  corrigidos no BigQuery'.padEnd(44) + String(divergentes.length).padStart(8));
  divergentes.forEach(d => console.log('    - domínio ' + d.id));
}

if (require.main === module) {
  main().catch(e => {
    console.log('[sincronizar_cs] falhou, seguindo sem corrigir angel_id: ' + e.message.slice(0, 300));
  });
}

module.exports = { main };
