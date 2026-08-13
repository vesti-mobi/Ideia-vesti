/**
 * fetch_dados.js — popula o Painel de Clientes (CS) com dados reais.
 *
 * Fontes
 *   BigQuery  vesti-data-499015.vestilake_BI   (somente SELECT)
 *   HubSpot   pipeline "Expand (Upgrades)" + tasks
 *
 * Saída: dados.js  ->  window.PAINEL_DATA, no formato documentado no index.html.
 *
 * Grão temporal: SEMANA ISO do ano corrente (o painel inteiro é "semana do ano").
 * Só entram semanas 1..semanaAtual do ANO definido abaixo.
 *
 * Rodar:  node fetch_dados.js
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

const RAIZ = path.resolve(__dirname, '..');
const SA_KEY = process.env.GOOGLE_APPLICATION_CREDENTIALS
  || 'C:/Users/Laura/Downloads/vesti-data-499015-7ea468dae45e.json';
process.env.GOOGLE_APPLICATION_CREDENTIALS = SA_KEY;

const { BigQuery } = require(path.join(RAIZ, 'node_modules/@google-cloud/bigquery'));
const bq = new BigQuery({ projectId: 'vesti-data-499015' });
const DS = '`vesti-data-499015.vestilake_BI`';

// ---------------------------------------------------------------- parâmetros
const HOJE = new Date();
const ANO = HOJE.getUTCFullYear();
const SEMANA_ATUAL = isoWeek(HOJE);

/* Teto de valor por pedido. Mesmo filtro que o CS-Sucesso e o PainelElisa usam
   para descartar pedido-teste/outlier — mantido para os números baterem entre
   os painéis. */
const TETO_PEDIDO = 50000;

/* Quanto da antecipação cobrada do lojista fica com a Vesti.
   Medido em vestipago_transaction_detail (antecipationVestiFee/antecipationValue).
   Recalculado a cada execução; a constante abaixo é só o fallback. */
let FATOR_ANTECIPACAO_VESTI = 0.188;

/* Churn: marca sem fatura paga há mais de N dias e sem fatura futura em aberto. */
const DIAS_CHURN = 45;

/* Anjos que não são mais CS da carteira. As marcas deles passam a "Sem CS" —
   assim a coluna e o filtro contam a mesma história. */
const ANJOS_FORA = ['Shirley Silva', 'Priscila Argolo'];

/* A aba Tarefas mostra só o time de CS. Qualquer outro dono de tarefa no
   HubSpot (vendas, parceiros, sem responsável) fica de fora. */
const CS_TAREFAS = ['Luana Coutinho', 'Thamiris Ribeiro', 'Cristiane Canatelli',
                    'Elisa Marques', 'Gabriella Busto', 'Alexia Oliveira', 'Tatiane Ayres'];

// ---------------------------------------------------------------- utilidades
function isoWeek(d) {
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  t.setUTCDate(t.getUTCDate() - ((t.getUTCDay() + 6) % 7) + 3);
  const base = new Date(Date.UTC(t.getUTCFullYear(), 0, 4));
  return 1 + Math.round(((t - base) / 864e5 - 3 + ((base.getUTCDay() + 6) % 7)) / 7);
}
function dataDaSemana(s, ano) {           // quinta-feira da semana ISO
  const base = new Date(Date.UTC(ano, 0, 4));
  const seg = new Date(base);
  seg.setUTCDate(base.getUTCDate() - ((base.getUTCDay() + 6) % 7) + (s - 1) * 7);
  seg.setUTCDate(seg.getUTCDate() + 3);
  return seg.toISOString().slice(0, 10);
}
const num = v => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
const r2 = v => Math.round(num(v) * 100) / 100;
const soDigitos = v => String(v || '').replace(/\D/g, '');
const iso = v => {
  if (!v) return null;
  const s = typeof v === 'object' && v.value ? v.value : String(v);
  return s.slice(0, 10);
};

async function q(label, sql) {
  const t0 = Date.now();
  process.stdout.write('  ' + label.padEnd(42));
  const [rows] = await bq.query({ query: sql });
  console.log(String(rows.length).padStart(8) + ' linhas   ' + ((Date.now() - t0) / 1000).toFixed(1) + 's');
  return rows;
}

// filtro de semana do ano corrente, reaproveitado em várias queries
const FILTRO_SEMANA = (col) => `
  EXTRACT(ISOYEAR FROM DATE(CAST(${col} AS TIMESTAMP))) = ${ANO}
  AND EXTRACT(ISOWEEK FROM DATE(CAST(${col} AS TIMESTAMP))) <= ${SEMANA_ATUAL}`;

// ============================================================== 1. BIGQUERY
async function puxarBQ() {
  console.log('\n[BigQuery] projeto vesti-data-499015 / vestilake_BI');

  const cadastro = await q('cadastro de marcas (domínio + CS)', `
    WITH dom AS (
      SELECT CAST(ID AS STRING) id, name, angel_id, integration_type, integration_id, created_at,
             ROW_NUMBER() OVER (PARTITION BY ID ORDER BY updated_At DESC) rn
      FROM ${DS}.odbc_domains
      WHERE LOWER(IFNULL(modulos,'')) LIKE '%vendas%'
        AND LOWER(IFNULL(name,'')) NOT LIKE '%teste%'
        AND LOWER(IFNULL(name,'')) NOT LIKE '%andressa vesti%'
    ),
    comp AS (
      SELECT CAST(domain_id AS STRING) domain_id, tax_document, social_name, company_name, status,
             ROW_NUMBER() OVER (PARTITION BY domain_id ORDER BY created_at ASC) rn
      FROM ${DS}.odbc_companies
    ),
    /* integration_id chega como "7.0" (float em texto) e odbc_integrations.id é
       inteiro — juntar direto não casa nada. Normaliza pelos dois lados. */
    integ AS (
      SELECT CAST(SAFE_CAST(id AS FLOAT64) AS INT64) id, ANY_VALUE(name) nome
      FROM ${DS}.odbc_integrations GROUP BY 1
    )
    SELECT d.id, d.name nome, d.integration_type, i.nome integracao_nome,
           a.name cs, c.tax_document cnpj, c.social_name, c.status status_empresa,
           SUBSTR(CAST(d.created_at AS STRING),1,10) criacao
    FROM dom d
    LEFT JOIN comp c ON c.domain_id = d.id AND c.rn = 1
    LEFT JOIN ${DS}.odbc_angels a ON CAST(a.id AS STRING) = CAST(d.angel_id AS STRING)
    LEFT JOIN integ i ON i.id = CAST(SAFE_CAST(d.integration_id AS FLOAT64) AS INT64)
    WHERE d.rn = 1`);

  const pedidos = await q('pedidos por marca × semana', `
    SELECT CAST(domainId AS STRING) dom,
      EXTRACT(ISOWEEK FROM DATE(CAST(settings_createdAt AS TIMESTAMP))) sem,
      COUNT(*) pedidos,
      COUNTIF(payment_isPaid='True') pagos,
      ROUND(SUM(IF(payment_isPaid='True', CAST(summary_total AS FLOAT64), 0)),2) valor,
      ROUND(SUM(IF(payment_isPaid='True', SAFE_CAST(payment_transaction_vestiPagoValue AS FLOAT64), 0)),2) vp,
      ROUND(SUM(IF(payment_isPaid='True', SAFE_CAST(payment_transaction_antifraudValue AS FLOAT64), 0)),2) af,
      ROUND(SUM(IF(payment_isPaid='True', SAFE_CAST(payment_transaction_antecipationValue AS FLOAT64), 0)),2) antec
    FROM ${DS}.MongoDB_Pedidos_Geral
    WHERE settings_createdAt IS NOT NULL AND SAFE_CAST(domainId AS INT64) IS NOT NULL
      AND SAFE_CAST(summary_total AS FLOAT64) > 0
      AND SAFE_CAST(summary_total AS FLOAT64) < ${TETO_PEDIDO}
      AND ${FILTRO_SEMANA('settings_createdAt')}
    GROUP BY 1,2`);

  const ultimoPedido = await q('último pedido por marca', `
    SELECT CAST(domainId AS STRING) dom,
           SUBSTR(CAST(MAX(CAST(settings_createdAt AS TIMESTAMP)) AS STRING),1,10) ultimo
    FROM ${DS}.MongoDB_Pedidos_Geral
    WHERE settings_createdAt IS NOT NULL AND SAFE_CAST(domainId AS INT64) IS NOT NULL
    GROUP BY 1`);

  /* Duas medidas diferentes na mesma aba, e elas NÃO são o mesmo recorte:
       - links gerados/pagos = pedidos originados em link de COBRANÇA.
         'Link' (573k pedidos) é o link de compartilhamento do vendedor, não
         de cobrança — incluir ele inflava o número em ~20x.
       - valor transacionado / fee / antecipação = pedidos pagos por um
         provider do Vesti Pago (IUGU, STARKBANK, PAGARME), qualquer origem.
         Pedido com provider nulo é venda registrada na plataforma mas paga
         por fora: não gera receita e precisa ficar de fora. */
  /* Só 'Link de cobrança'. Conferido no dado: os 'Link de cobrança' sem provider
     são exatamente os que ninguém pagou (0 pagos), então o link é do Vesti Pago
     de ponta a ponta. Já 'Link sem preço' tem 4.226 pedidos PAGOS por fora
     (R$ 8,8M) — misturar os dois faria a aba contar link que não é do produto. */
  const LINK_COBRANCA = `settings_source = 'Link de cobrança'`;
  const VIA_VESTIPAGO = `payment_isPaid='True' AND payment_transaction_provider IS NOT NULL`;
  const vestipago = await q('Vesti Pago por marca × semana', `
    SELECT CAST(domainId AS STRING) dom,
      EXTRACT(ISOWEEK FROM DATE(CAST(settings_createdAt AS TIMESTAMP))) sem,
      COUNTIF(${LINK_COBRANCA}) gerados,
      COUNTIF(${LINK_COBRANCA} AND payment_isPaid='True') pagos,
      COUNTIF(${VIA_VESTIPAGO}) transacoes,
      ROUND(SUM(IF(${VIA_VESTIPAGO}, CAST(summary_total AS FLOAT64), 0)),2) valor,
      ROUND(SUM(IF(${VIA_VESTIPAGO} AND payment_method='CREDIT_CARD', CAST(summary_total AS FLOAT64), 0)),2) valor_cartao,
      ROUND(SUM(IF(${VIA_VESTIPAGO} AND payment_method='PIX', CAST(summary_total AS FLOAT64), 0)),2) valor_pix,
      ROUND(SUM(IF(${VIA_VESTIPAGO}, SAFE_CAST(payment_transaction_vestiPagoValue AS FLOAT64), 0)),2) vp,
      ROUND(SUM(IF(${VIA_VESTIPAGO}, SAFE_CAST(payment_transaction_antifraudValue AS FLOAT64), 0)),2) af,
      ROUND(SUM(IF(${VIA_VESTIPAGO}, SAFE_CAST(payment_transaction_antecipationValue AS FLOAT64), 0)),2) antec
    FROM ${DS}.MongoDB_Pedidos_Geral
    WHERE settings_createdAt IS NOT NULL AND SAFE_CAST(domainId AS INT64) IS NOT NULL
      AND SAFE_CAST(summary_total AS FLOAT64) > 0
      AND SAFE_CAST(summary_total AS FLOAT64) < ${TETO_PEDIDO}
      AND ${FILTRO_SEMANA('settings_createdAt')}
      AND (${LINK_COBRANCA} OR ${VIA_VESTIPAGO})
    GROUP BY 1,2`);

  const fatorRows = await q('fator Vesti sobre antecipação', `
    SELECT ROUND(SAFE_DIVIDE(SUM(SAFE_CAST(antecipationVestiFee AS FLOAT64)),
                             SUM(SAFE_CAST(antecipationValue AS FLOAT64))), 4) fator
    FROM ${DS}.vestipago_transaction_detail
    WHERE SAFE_CAST(antecipationValue AS FLOAT64) > 0`);
  if (fatorRows[0] && fatorRows[0].fator > 0) FATOR_ANTECIPACAO_VESTI = fatorRows[0].fator;
  console.log('     -> fator antecipação Vesti = ' + (FATOR_ANTECIPACAO_VESTI * 100).toFixed(2) + '%');

  const oraculoGmv = await q('Oráculo: GMV por marca × semana', `
    SELECT CAST(domainId AS STRING) dom,
      EXTRACT(ISOWEEK FROM DATE(CAST(settings_createdAt AS TIMESTAMP))) sem,
      ROUND(SUM(SAFE_CAST(summary_total AS FLOAT64)),2) gmv_iniciado,
      ROUND(SUM(IF(Tipo_Venda_Oraculo='Venda Direta', SAFE_CAST(summary_total AS FLOAT64), 0)),2) gmv_finalizado
    FROM ${DS}.oraculo_Pedidos
    WHERE Tipo_Venda_Oraculo IS NOT NULL AND settings_createdAt IS NOT NULL
      AND SAFE_CAST(domainId AS INT64) IS NOT NULL
      AND ${FILTRO_SEMANA('settings_createdAt')}
    GROUP BY 1,2`);

  const oraculoAtend = await q('Oráculo: atendimentos por marca × semana', `
    SELECT CAST(domain_id AS STRING) dom,
      EXTRACT(ISOWEEK FROM DATE(CAST(DataReferencia AS TIMESTAMP))) sem,
      COUNTIF(source='IA') ia, COUNT(*) total
    FROM ${DS}.oraculo_Atendimentos
    WHERE DataReferencia IS NOT NULL AND SAFE_CAST(domain_id AS INT64) IS NOT NULL
      AND ${FILTRO_SEMANA('DataReferencia')}
    GROUP BY 1,2`);

  const tino = await q('Tino: cliques por marca × semana', `
    WITH ud AS (
      SELECT CAST(id AS STRING) uid, ANY_VALUE(CAST(domain_id AS STRING)) dom
      FROM ${DS}.odbc_users WHERE domain_id IS NOT NULL GROUP BY 1
    )
    SELECT ud.dom,
      EXTRACT(ISOWEEK FROM DATE(CAST(r.rankings_created_at AS TIMESTAMP))) sem,
      SUM(SAFE_CAST(r.rankings_shared_links AS INT64)) cliques
    FROM ${DS}.sucessodocliente_rankings r
    JOIN ud ON ud.uid = CAST(r.USERS_ID AS STRING)
    WHERE r.rankings_created_at IS NOT NULL
      AND SAFE_CAST(r.rankings_created_at AS TIMESTAMP) < TIMESTAMP '2100-01-01'
      AND ${FILTRO_SEMANA('r.rankings_created_at')}
    GROUP BY 1,2`);

  /* Último uso do Tino: a data mais recente em que a marca compartilhou link.
     Vai além de 2026 de propósito — é o que responde "quando essa marca usou
     pela última vez" e "quem nunca usou". */
  const tinoUltimo = await q('Tino: último uso por marca', `
    WITH ud AS (
      SELECT CAST(id AS STRING) uid, ANY_VALUE(CAST(domain_id AS STRING)) dom
      FROM ${DS}.odbc_users WHERE domain_id IS NOT NULL GROUP BY 1
    )
    SELECT ud.dom,
           SUBSTR(CAST(MAX(CAST(r.rankings_created_at AS TIMESTAMP)) AS STRING),1,10) ultimo,
           SUM(SAFE_CAST(r.rankings_shared_links AS INT64)) cliques_total
    FROM ${DS}.sucessodocliente_rankings r
    JOIN ud ON ud.uid = CAST(r.USERS_ID AS STRING)
    WHERE r.rankings_created_at IS NOT NULL
      AND SAFE_CAST(r.rankings_created_at AS TIMESTAMP) < TIMESTAMP '2100-01-01'
    GROUP BY 1`);

  /* Mensalidade tem que ser mensalidade. A fatura do Iugu junta plano, Oráculo,
     Filial, Assistente e taxa de ativação no mesmo total — só 75% é plano. Por
     isso a soma é feita LINHA A LINHA do item, separando o plano do resto.
     O "resto" continua sendo receita e entra na Receita total, em coluna própria. */
  const mensalidade = await q('Iugu: plano × outros, por CNPJ × semana', `
    WITH itens AS (
      SELECT id, items_id,
             ANY_VALUE(payer_cpf_cnpj) cnpj, ANY_VALUE(status) status,
             ANY_VALUE(items_description) item,
             ANY_VALUE(SAFE_CAST(items_price_cents AS FLOAT64)) cents,
             ANY_VALUE(COALESCE(due_date, SUBSTR(created_at_iso,1,10))) due
      FROM ${DS}.iugu_invoices
      WHERE payer_cpf_cnpj IS NOT NULL AND payer_cpf_cnpj != ''
      GROUP BY id, items_id
    )
    SELECT REGEXP_REPLACE(cnpj,'[^0-9]','') cnpj,
           EXTRACT(ISOWEEK FROM DATE(due)) sem,
           ROUND(SUM(IF(
             LOWER(IFNULL(item,'')) LIKE '%oraculo%' OR LOWER(IFNULL(item,'')) LIKE '%oráculo%'
          OR LOWER(IFNULL(item,'')) LIKE '%assistente%' OR LOWER(IFNULL(item,'')) LIKE '%agente%'
          OR LOWER(IFNULL(item,'')) LIKE '%filial%'
          OR LOWER(IFNULL(item,'')) LIKE '%ativa%'  OR LOWER(IFNULL(item,'')) LIKE '%setup%',
             0, cents))/100,2) plano,
           ROUND(SUM(IF(
             LOWER(IFNULL(item,'')) LIKE '%oraculo%' OR LOWER(IFNULL(item,'')) LIKE '%oráculo%'
          OR LOWER(IFNULL(item,'')) LIKE '%assistente%' OR LOWER(IFNULL(item,'')) LIKE '%agente%'
          OR LOWER(IFNULL(item,'')) LIKE '%filial%'
          OR LOWER(IFNULL(item,'')) LIKE '%ativa%'  OR LOWER(IFNULL(item,'')) LIKE '%setup%',
             cents, 0))/100,2) outros
    FROM itens
    WHERE status IN ('paid','externally_paid') AND due IS NOT NULL
      AND EXTRACT(ISOYEAR FROM DATE(due)) = ${ANO}
      AND EXTRACT(ISOWEEK FROM DATE(due)) <= ${SEMANA_ATUAL}
    GROUP BY 1,2`);

  /* O plano não é uma coluna: é a linha de item mais cara da fatura, tirando
     desconto/Oráculo. Mesma regra do CS-Sucesso, para os dois painéis
     mostrarem o mesmo nome de plano. */
  const faturas = await q('Iugu: vencimento, plano e última paga', `
    WITH linhas AS (
      SELECT id, payer_cpf_cnpj cnpj, status,
             SAFE_CAST(total_cents AS FLOAT64) cents,
             COALESCE(due_date, SUBSTR(created_at_iso,1,10)) due,
             items_description item, SAFE_CAST(items_price_cents AS FLOAT64) ipc
      FROM ${DS}.iugu_invoices
      WHERE payer_cpf_cnpj IS NOT NULL AND payer_cpf_cnpj != ''
    ),
    plano_da_fatura AS (
      SELECT id, ARRAY_AGG(item ORDER BY ipc DESC LIMIT 1)[OFFSET(0)] plano
      FROM linhas
      WHERE ipc > 0
        AND LOWER(IFNULL(item,'')) NOT LIKE '%desconto%'
        AND LOWER(IFNULL(item,'')) NOT LIKE '%oraculo%'
        AND LOWER(IFNULL(item,'')) NOT LIKE '%oráculo%'
      GROUP BY id
    ),
    inv AS (
      SELECT id, ANY_VALUE(cnpj) cnpj, ANY_VALUE(status) status,
             ANY_VALUE(cents) cents, ANY_VALUE(due) due
      FROM linhas GROUP BY id
    ),
    inv2 AS (SELECT inv.*, p.plano FROM inv LEFT JOIN plano_da_fatura p USING(id))
    SELECT REGEXP_REPLACE(cnpj,'[^0-9]','') cnpj,
           MIN(IF(status='pending' AND due >= CAST(CURRENT_DATE() AS STRING), due, NULL)) proximo_venc,
           MAX(IF(status IN ('paid','externally_paid'), due, NULL)) ultima_paga,
           ARRAY_AGG(IF(status IN ('paid','externally_paid') AND plano IS NOT NULL,
                        STRUCT(due, cents, plano), NULL)
                     IGNORE NULLS ORDER BY due DESC LIMIT 1)[SAFE_OFFSET(0)] ultima
    FROM inv2
    GROUP BY 1`);

  return { cadastro, pedidos, ultimoPedido, vestipago, oraculoGmv, oraculoAtend,
           tino, tinoUltimo, mensalidade, faturas };
}

// =============================================================== 2. HUBSPOT
function envDe(p) {
  try {
    return Object.fromEntries(fs.readFileSync(p, 'utf8').split(/\r?\n/)
      .filter(l => l.includes('=') && !l.trim().startsWith('#'))
      .map(l => [l.slice(0, l.indexOf('=')).trim(), l.slice(l.indexOf('=') + 1).trim()]));
  } catch { return {}; }
}
const HS_TOKEN = process.env.HUBSPOT_TOKEN
  || envDe(path.join(RAIZ, 'CS-Sucesso-do-cliente/.env')).HUBSPOT_TOKEN;

function hs(metodo, caminho, corpo) {
  return new Promise((res, rej) => {
    const b = corpo ? JSON.stringify(corpo) : null;
    const req = https.request({
      hostname: 'api.hubapi.com', path: caminho, method: metodo,
      headers: Object.assign({ Authorization: 'Bearer ' + HS_TOKEN },
        b ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(b) } : {})
    }, r => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => {
        if (r.statusCode === 429) return setTimeout(() => hs(metodo, caminho, corpo).then(res, rej), 1200);
        if (r.statusCode >= 400) return rej(new Error(r.statusCode + ' ' + d.slice(0, 200)));
        try { res(JSON.parse(d)); } catch (e) { rej(e); }
      });
    });
    req.on('error', rej);
    if (b) req.write(b);
    req.end();
  });
}

async function buscarTudo(objeto, props, filtros, sort) {
  const out = []; let after;
  do {
    const corpo = { properties: props, limit: 100, filterGroups: filtros, sorts: sort };
    if (after) corpo.after = after;
    const j = await hs('POST', `/crm/v3/objects/${objeto}/search`, corpo);
    out.push(...(j.results || []));
    after = j.paging && j.paging.next && j.paging.next.after;
    if (out.length >= 10000) break;   // trava de segurança
  } while (after);
  return out;
}

/* Classificação do negócio a partir do nome. O HubSpot guarda cross-sell e
   upsell juntos no pipeline "Expand (Upgrades)"; o produto aparece no nome
   ("Oraculo | Marca", "Filial Integrada - Marca", "Marca - Upgrade"...).
   A ordem importa: o primeiro padrão que casar vence. */
const REGRAS_PRODUTO = [
  { re: /\bfili?a(l|is)\b/,                        produto: 'Filial',              cat: 'cross' },
  { re: /\bupgrade\b|\bplano pro\b|\bpro\b(?!duto)/, produto: 'Upgrade de plano',  cat: 'up' },
  { re: /\bmultiloja\b|\bmatriz\b/,                produto: 'Multiloja',           cat: 'cross' },
  { re: /oraculo/,                                 produto: 'Oráculo',             cat: 'cross' },
  // pega "Agente de Atendimento", "Ag Atendimento", "Assistente do Vendedor".
  // O \w* no fim consome a palavra inteira, senão sobra "ento" no nome da marca.
  { re: /ag(ente)?\.?\s*(de\s*)?atendim\w*|assistente( do)? vendedor|\bagente\b/, produto: 'Assistente do Vendedor', cat: 'cross' },
  { re: /integracao|totvs|millennium|bling|tiny|presence/, produto: 'Integração',  cat: 'cross' },
  { re: /vesti\s?pago|\bvp\b/,                     produto: 'Vesti Pago',          cat: 'cross' },
  { re: /\btino\b/,                                produto: 'Tino',                cat: 'cross' },
  { re: /antecipa/,                                produto: 'Antecipação',         cat: 'cross' },
  { re: /catalogo|vitrine/,                        produto: 'Catálogo',            cat: 'cross' },
];
/* Sem acento e em minúsculas antes de testar: "Óraculo", "Oraculo" e "Oráculo"
   têm que cair no mesmo balde. As regex acima já são escritas normalizadas.
   A troca é 1 para 1 de propósito — assim o índice do match na versão
   normalizada aponta para a mesma posição no nome original, e dá para
   recortar o produto de lá sem estragar os acentos da marca. */
const ACENTOS = { á:'a', à:'a', â:'a', ã:'a', ä:'a', é:'e', è:'e', ê:'e', ë:'e',
                  í:'i', ì:'i', î:'i', ï:'i', ó:'o', ò:'o', ô:'o', õ:'o', ö:'o',
                  ú:'u', ù:'u', û:'u', ü:'u', ç:'c', ñ:'n' };
/* trim + colapso de espaços: vários nomes vêm com espaço sobrando no fim
   ("Landê Oficial "), o que quebrava as exceções ancoradas em ^...$ */
const semAcento = s => String(s || '').normalize('NFC').toLowerCase()
  .replace(/[áàâãäéèêëíìîïóòôõöúùûüçñ]/g, c => ACENTOS[c])
  .replace(/\s+/g, ' ').trim();

/* Negócios cujo nome não diz o produto. Decididos com a Laura em 13/08/2026. */
const EXCECOES = [
  { re: /kelly rodrigues store fortaleza/, produto: 'Filial',           cat: 'cross' },
  { re: /^jay & co$/,                      produto: 'Upgrade de plano', cat: 'up' },
  { re: /^lande oficial$/,                 produto: 'Upgrade de plano', cat: 'up' },
];

function classificar(nome) {
  const n = semAcento(nome);
  for (const e of EXCECOES) if (e.re.test(n)) return { produto: e.produto, cat: e.cat, ini: -1, fim: -1 };
  for (const r of REGRAS_PRODUTO) {
    const m = n.match(r.re);
    if (m) return { produto: r.produto, cat: r.cat, ini: m.index, fim: m.index + m[0].length };
  }
  return { produto: 'Outros', cat: 'cross', ini: -1, fim: -1 };
}
/* Marca = nome do negócio menos o pedaço que identificou o produto. Só é usado
   quando o negócio não tem empresa associada no HubSpot. */
function marcaDoNome(nome, cls) {
  // mesmo tratamento de espaços do semAcento, senão ini/fim apontam torto
  const orig = String(nome || '').normalize('NFC').replace(/\s+/g, ' ').trim();
  const sem = cls.ini >= 0 ? orig.slice(0, cls.ini) + ' ' + orig.slice(cls.fim) : orig;
  return sem.replace(/\[[^\]]*\]/g, ' ')          // tira marcadores tipo "[Perdido]"
            .replace(/[|\-–—:]+/g, ' ')
            .replace(/\s+/g, ' ').trim();
}

async function puxarHubSpot() {
  console.log('\n[HubSpot]');
  if (!HS_TOKEN) { console.log('  sem HUBSPOT_TOKEN — negócios e tarefas ficam vazios'); return { negocios: [], tarefas: [] }; }

  const owners = {};
  let after;
  do {
    const j = await hs('GET', '/crm/v3/owners?limit=100' + (after ? '&after=' + after : ''));
    (j.results || []).forEach(o => owners[o.id] = ((o.firstName || '') + ' ' + (o.lastName || '')).trim() || o.email);
    after = j.paging && j.paging.next && j.paging.next.after;
  } while (after);
  console.log('  owners'.padEnd(44) + String(Object.keys(owners).length).padStart(8));

  const pipes = await hs('GET', '/crm/v3/pipelines/deals');
  const expand = pipes.results.find(p => /^expand/i.test(p.label));
  const estagio = {};
  expand.stages.forEach(s => {
    estagio[s.id] = /ganho/i.test(s.label) ? 'Ganho' : /perdido/i.test(s.label) ? 'Perdido' : 'Em aberto';
  });

  const deals = await buscarTudo('deals',
    ['dealname', 'amount', 'dealstage', 'closedate', 'createdate', 'hubspot_owner_id'],
    [{ filters: [{ propertyName: 'pipeline', operator: 'EQ', value: expand.id }] }],
    [{ propertyName: 'createdate', direction: 'DESCENDING' }]);
  console.log('  negócios (pipeline Expand)'.padEnd(44) + String(deals.length).padStart(8));

  // empresa associada -> nome do cliente (mais confiável que raspar o nome do deal)
  const empresaDoDeal = {};
  for (let i = 0; i < deals.length; i += 100) {
    const lote = deals.slice(i, i + 100).map(d => ({ id: d.id }));
    try {
      const j = await hs('POST', '/crm/v4/associations/deals/companies/batch/read', { inputs: lote });
      (j.results || []).forEach(r => {
        if (r.to && r.to.length) empresaDoDeal[r.from.id] = r.to[0].toObjectId;
      });
    } catch (e) { /* associação é opcional */ }
  }
  const idsEmpresa = [...new Set(Object.values(empresaDoDeal))];
  const nomeEmpresa = {};
  for (let i = 0; i < idsEmpresa.length; i += 100) {
    try {
      const j = await hs('POST', '/crm/v3/objects/companies/batch/read',
        { properties: ['name'], inputs: idsEmpresa.slice(i, i + 100).map(id => ({ id: String(id) })) });
      (j.results || []).forEach(c => nomeEmpresa[c.id] = c.properties.name);
    } catch (e) { /* idem */ }
  }
  console.log('  empresas associadas'.padEnd(44) + String(Object.keys(nomeEmpresa).length).padStart(8));

  const negocios = deals.map(d => {
    const p = d.properties;
    const cls = classificar(p.dealname);
    const emp = nomeEmpresa[empresaDoDeal[d.id]];
    return {
      cliente: emp || marcaDoNome(p.dealname, cls) || p.dealname || '(sem nome)',
      produto: cls.produto,
      categoria: cls.cat,
      negocio: p.dealname || '',
      valor: r2(p.amount),
      status: estagio[p.dealstage] || 'Em aberto',
      data: iso(p.closedate) || iso(p.createdate),
      cs: owners[p.hubspot_owner_id] || '',
    };
  }).filter(n => n.data && Number(n.data.slice(0, 4)) === ANO);

  const desde = Date.UTC(ANO, 0, 1);
  const tasksRaw = await buscarTudo('tasks',
    ['hs_task_subject', 'hs_timestamp', 'hs_task_type', 'hs_task_status', 'hubspot_owner_id'],
    [{ filters: [{ propertyName: 'hs_timestamp', operator: 'GTE', value: String(desde) }] }],
    [{ propertyName: 'hs_timestamp', direction: 'DESCENDING' }]);
  const TIPOS = { TODO: 'A fazer', CALL: 'Ligação', EMAIL: 'E-mail', MEETING: 'Reunião', LINKED_IN_MESSAGE: 'LinkedIn' };
  const tarefas = tasksRaw.map(t => ({
    tarefa: t.properties.hs_task_subject || '(sem assunto)',
    data: iso(t.properties.hs_timestamp),
    tipo: TIPOS[t.properties.hs_task_type] || t.properties.hs_task_type || 'Outro',
    cs: owners[t.properties.hubspot_owner_id] || '(sem responsável)',
  })).filter(t => t.data && Number(t.data.slice(0, 4)) === ANO && CS_TAREFAS.includes(t.cs));
  console.log('  tarefas (só o time de CS)'.padEnd(44) + String(tarefas.length).padStart(8));

  return { negocios, tarefas };
}

// ================================================================ 3. MONTAGEM
function montar(bqd, hsd) {
  console.log('\n[montagem]');
  const semanaOk = s => Number.isFinite(s) && s >= 1 && s <= SEMANA_ATUAL;

  // ---- índice de marcas
  const porDom = new Map();
  bqd.cadastro.forEach(c => {
    if (porDom.has(c.id)) return;
    porDom.set(c.id, {
      dom: c.id,
      nome: (c.nome || c.social_name || 'Domínio ' + c.id).trim(),
      cs: (c.cs && c.cs !== 'N/A' && !ANJOS_FORA.includes(c.cs)) ? c.cs : 'Sem CS',
      integracao: c.integracao_nome || c.integration_type || 'Sem integração',
      cnpj: soDigitos(c.cnpj),
      statusEmpresa: c.status_empresa,
      criacao: c.criacao,
    });
  });
  console.log('  marcas no cadastro'.padEnd(44) + String(porDom.size).padStart(8));

  const somaEm = (mapa, chave, campos) => {
    let o = mapa.get(chave);
    if (!o) { o = {}; mapa.set(chave, o); }
    for (const k in campos) o[k] = r2((o[k] || 0) + num(campos[k]));
    return o;
  };

  // ---- séries semanais da carteira
  const serieCli = new Map();               // dom|sem -> {}
  bqd.pedidos.forEach(r => {
    if (!semanaOk(r.sem) || !porDom.has(r.dom)) return;
    somaEm(serieCli, r.dom + '|' + r.sem, {
      pedidos: r.pedidos, pedidosPagos: r.pagos, valorPedidos: r.valor,
      receitaInterchange: num(r.vp) + num(r.af),
      receitaAntecipacao: num(r.antec) * FATOR_ANTECIPACAO_VESTI,
    });
  });

  // mensalidade entra por CNPJ -> domínio
  const domPorCnpj = new Map();
  porDom.forEach(m => { if (m.cnpj) if (!domPorCnpj.has(m.cnpj)) domPorCnpj.set(m.cnpj, m.dom); });
  let mensAplicada = 0;
  bqd.mensalidade.forEach(r => {
    const dom = domPorCnpj.get(r.cnpj);
    if (!dom || !semanaOk(r.sem)) return;
    somaEm(serieCli, dom + '|' + r.sem, { receitaMensalidade: r.plano, receitaOutrosIugu: r.outros });
    mensAplicada++;
  });
  console.log('  linhas de mensalidade casadas'.padEnd(44) + String(mensAplicada).padStart(8));

  const clientesSeries = [];
  serieCli.forEach((v, k) => {
    const [dom, sem] = k.split('|');
    const m = porDom.get(dom);
    clientesSeries.push({
      semana: Number(sem), cliente: m.nome,
      pedidos: v.pedidos || 0, valorPedidos: v.valorPedidos || 0,
      receitaInterchange: v.receitaInterchange || 0,
      receitaMensalidade: v.receitaMensalidade || 0,
      receitaOutrosIugu: v.receitaOutrosIugu || 0,
      receitaAntecipacao: r2(v.receitaAntecipacao || 0),
    });
  });

  // ---- último pedido / vencimento / churn
  const ultPedido = new Map();
  bqd.ultimoPedido.forEach(r => ultPedido.set(r.dom, r.ultimo));

  const fatPorCnpj = new Map();
  bqd.faturas.forEach(r => fatPorCnpj.set(r.cnpj, r));

  const hojeIso = HOJE.toISOString().slice(0, 10);
  const limiteChurn = new Date(HOJE.getTime() - DIAS_CHURN * 864e5).toISOString().slice(0, 10);

  // ---- risco de churn (regra explícita, sem caixa-preta)
  function avaliarRisco(m, ultimoPed, fat) {
    const motivos = [];
    const diasPed = ultimoPed ? Math.round((HOJE - new Date(ultimoPed)) / 864e5) : null;
    if (diasPed == null) motivos.push('nunca vendeu');
    else if (diasPed > 45) motivos.push('sem pedido há ' + diasPed + ' dias');
    else if (diasPed > 15) motivos.push('sem pedido há ' + diasPed + ' dias');
    if (fat && fat.ultima_paga && fat.ultima_paga < limiteChurn) motivos.push('sem fatura paga desde ' + fat.ultima_paga);
    if (fat && !fat.proximo_venc) motivos.push('sem fatura futura em aberto');
    const grave = (diasPed == null || diasPed > 45) || (fat && fat.ultima_paga && fat.ultima_paga < limiteChurn);
    const medio = (diasPed != null && diasPed > 15) || (fat && !fat.proximo_venc);
    return { risco: grave ? 'Alto' : medio ? 'Médio' : 'Baixo', motivos };
  }

  const clientes = [];
  porDom.forEach(m => {
    const fat = m.cnpj ? fatPorCnpj.get(m.cnpj) : null;
    const ultimoPed = ultPedido.get(m.dom) || null;
    // churn = sem fatura paga há mais de DIAS_CHURN dias e sem fatura futura em aberto
    let dataChurn = null;
    if (fat && fat.ultima_paga && fat.ultima_paga < limiteChurn && !fat.proximo_venc) {
      const d = new Date(new Date(fat.ultima_paga).getTime() + DIAS_CHURN * 864e5).toISOString().slice(0, 10);
      dataChurn = d <= hojeIso ? d : null;
    }
    const av = dataChurn ? { risco: null, motivos: [] } : avaliarRisco(m, ultimoPed, fat);
    clientes.push({
      nome: m.nome, cs: m.cs,
      status: dataChurn ? 'inativo' : (ultimoPed && ultimoPed >= limiteChurn ? 'ativo' : 'inativo'),
      integracao: m.integracao,
      plano: (fat && fat.ultima && fat.ultima.plano) ? String(fat.ultima.plano) : 'Sem plano',
      ultimoAcesso: null,                    // sem fonte no BigQuery — ver README
      ultimoPedido: ultimoPed,
      vencimento: fat ? fat.proximo_venc : null,
      riscoChurn: av.risco,
      motivosRisco: av.motivos,
      dataChurn,
      _dom: m.dom, _cnpj: m.cnpj,
    });
  });

  // ---- produtos: tabelas + séries
  const nomeDe = dom => (porDom.get(dom) || {}).nome;
  const csDe = dom => (porDom.get(dom) || {}).cs;

  const oraSerie = new Map();
  bqd.oraculoGmv.forEach(r => {
    if (!semanaOk(r.sem) || !porDom.has(r.dom)) return;
    somaEm(oraSerie, r.dom + '|' + r.sem, { gmvIniciado: r.gmv_iniciado, gmvFinalizado: r.gmv_finalizado });
  });
  bqd.oraculoAtend.forEach(r => {
    if (!semanaOk(r.sem) || !porDom.has(r.dom)) return;
    somaEm(oraSerie, r.dom + '|' + r.sem, { atendimentos: r.total, atendimentosIA: r.ia });
  });
  const oraculoSeries = [], oraAcc = new Map();
  oraSerie.forEach((v, k) => {
    const [dom, sem] = k.split('|');
    oraculoSeries.push({
      semana: Number(sem), cliente: nomeDe(dom),
      atendimentos: v.atendimentos || 0, atendimentosIA: v.atendimentosIA || 0,
      gmvIniciado: v.gmvIniciado || 0, gmvFinalizado: v.gmvFinalizado || 0,
    });
    somaEm(oraAcc, dom, { at: v.atendimentos || 0, ia: v.atendimentosIA || 0, gi: v.gmvIniciado || 0, gf: v.gmvFinalizado || 0 });
  });
  const oraculoTab = [...oraAcc.entries()].map(([dom, v]) => ({
    cliente: nomeDe(dom), cs: csDe(dom),
    pctIA: v.at ? Math.round(v.ia / v.at * 100) : null,
    atendimentos: v.at, gmvIniciado: r2(v.gi), gmvFinalizado: r2(v.gf),
  })).filter(x => x.atendimentos || x.gmvIniciado);

  const tinoSeries = [], tinoAcc = new Map();
  const gmvSemana = new Map();     // dom|sem -> valor pago, para a linha de receita do Tino
  bqd.pedidos.forEach(r => { if (semanaOk(r.sem)) gmvSemana.set(r.dom + '|' + r.sem, num(r.valor)); });
  bqd.tino.forEach(r => {
    if (!semanaOk(r.sem) || !porDom.has(r.dom)) return;
    tinoSeries.push({
      semana: r.sem, cliente: nomeDe(r.dom),
      cliques: num(r.cliques), receita: gmvSemana.get(r.dom + '|' + r.sem) || 0,
    });
    somaEm(tinoAcc, r.dom, { cliques: num(r.cliques) });
  });
  /* A tabela do Tino lista TODA marca da carteira, inclusive quem nunca usou —
     é justamente essa lista que a CS precisa atacar. Último acesso e cliques
     no total são de toda a história, não só do ano. */
  const tinoUlt = new Map();
  bqd.tinoUltimo.forEach(r => tinoUlt.set(r.dom, r));
  const tinoTab = [];
  porDom.forEach(m => {
    const u = tinoUlt.get(m.dom);
    tinoTab.push({
      cliente: m.nome, cs: m.cs,
      cliques: (tinoAcc.get(m.dom) || {}).cliques || 0,
      ultimoAcessoTino: u ? u.ultimo : null,
      cliquesTotal: u ? num(u.cliques_total) : 0,
      nuncaUsou: !u,
    });
  });

  const vpSeries = [], vpAcc = new Map();
  bqd.vestipago.forEach(r => {
    if (!semanaOk(r.sem) || !porDom.has(r.dom)) return;
    const fee = num(r.vp) + num(r.af);
    const antec = num(r.antec) * FATOR_ANTECIPACAO_VESTI;
    vpSeries.push({
      semana: r.sem, cliente: nomeDe(r.dom),
      linksGerados: num(r.gerados), linksPagos: num(r.pagos), valor: num(r.valor),
      valorCartao: num(r.valor_cartao), valorPix: num(r.valor_pix),
      receitaFee: r2(fee), receitaAntecipacao: r2(antec),
    });
    somaEm(vpAcc, r.dom, {
      valorTransacionado: num(r.valor), valorCartao: num(r.valor_cartao), valorPix: num(r.valor_pix),
      receitaFee: fee, receitaAntecipacao: antec,
      linksGerados: num(r.gerados), linksPagos: num(r.pagos), transacoes: num(r.transacoes),
    });
  });
  const vpTab = [...vpAcc.entries()].map(([dom, v]) => ({
    cliente: nomeDe(dom), cs: csDe(dom),
    valorTransacionado: v.valorTransacionado,
    valorCartao: v.valorCartao, valorPix: v.valorPix,
    receitaFee: r2(v.receitaFee), receitaAntecipacao: r2(v.receitaAntecipacao),
    linksGerados: v.linksGerados, linksPagos: v.linksPagos,
  })).filter(x => x.linksGerados > 0 || x.valorTransacionado > 0);

  // ---- churn semanal
  const churnPorSemana = new Map();
  clientes.forEach(c => {
    if (!c.dataChurn) return;
    const s = isoWeek(new Date(c.dataChurn));
    if (!semanaOk(s) || Number(c.dataChurn.slice(0, 4)) !== ANO) return;
    churnPorSemana.set(c.nome + '|' + s, 1);
  });
  const churnSeries = [...churnPorSemana.keys()].map(k => {
    const i = k.lastIndexOf('|');
    return { semana: Number(k.slice(i + 1)), cliente: k.slice(0, i), churns: 1 };
  });

  // ---- só quem teve alguma atividade no ano entra no painel
  const ativos = new Set([
    ...clientesSeries.map(x => x.cliente),
    ...oraculoSeries.map(x => x.cliente),
    ...tinoSeries.map(x => x.cliente),
    ...vpSeries.map(x => x.cliente),
    ...churnSeries.map(x => x.cliente),
  ]);
  const clientesFinal = clientes.filter(c => ativos.has(c.nome));
  clientesFinal.forEach(c => { delete c._dom; delete c._cnpj; });
  console.log('  marcas com atividade em ' + ANO + ''.padEnd(20) + String(clientesFinal.length).padStart(13));

  const csLista = [...new Set(clientesFinal.map(c => c.cs))].sort();

  return {
    meta: {
      ano: ANO, semanaAtual: SEMANA_ATUAL, cs: csLista,
      geradoEm: new Date().toISOString(),
      fatorAntecipacaoVesti: FATOR_ANTECIPACAO_VESTI,
      diasChurn: DIAS_CHURN,
      tetoPedido: TETO_PEDIDO,
      avisos: {
        ultimoAcesso: 'Sem fonte: não existe coluna de login/sessão do lojista no vestilake_BI.',
        mensalidade: 'Receita mensalidade = SÓ as linhas de plano da fatura Iugu. Oráculo, Filial, '
               + 'Assistente e taxa de ativação saem em "Outros (Iugu)" — juntos eram 25% do que '
               + 'antes ia todo para a coluna de mensalidade. Os dois entram na Receita total.',
        receita: 'Interchange = payment_transaction_vestiPagoValue + antifraudValue (pedidos pagos). '
               + 'Antecipação = payment_transaction_antecipationValue x ' + FATOR_ANTECIPACAO_VESTI
               + ' (parcela da Vesti, medida em vestipago_transaction_detail).',
        feePix: 'ATENÇÃO: o fee da Vesti só existe para CARTÃO. Em PIX o campo vem nulo tanto em '
              + 'MongoDB_Pedidos_Geral quanto em vestipago_transaction_detail — e PIX é ~53% das '
              + 'transações Vesti Pago. Logo "Receita (fee)" e "Interchange" cobrem só o cartão; '
              + 'o valor transacionado cobre os dois.',
        vestiPago: 'Valor transacionado = pedidos pagos com provider Vesti Pago (IUGU/STARKBANK/PAGARME), '
                 + 'qualquer origem, separado por cartão e PIX. Links gerados/pagos = só "Link de cobrança": '
                 + 'os que aparecem sem provider são exatamente os não pagos, então o link é do Vesti Pago '
                 + 'de ponta a ponta. "Link sem preço" ficou de fora (tem 4.226 pedidos pagos por fora, '
                 + 'R$ 8,8M) e "Link" puro é o link de compartilhamento do vendedor.',
        negociosCat: 'Upsell = só upgrade de plano. Filial e Multiloja contam como cross-sell. '
                   + 'Exceções decididas na revisão: Kelly Rodrigues Store Fortaleza = Filial; '
                   + 'Jay & Co e Landê Oficial = Upgrade.',
        cs: 'Marcas de anjos que saíram da carteira (' + ANJOS_FORA.join(', ') + ') aparecem como "Sem CS". '
          + 'A aba Tarefas mostra só: ' + CS_TAREFAS.join(', ') + '.',
        churn: 'Derivado do Iugu: sem fatura paga há mais de ' + DIAS_CHURN + ' dias e sem fatura futura em aberto.',
        negocios: 'Pipeline "Expand (Upgrades)" do HubSpot. Cross-sell x upsell classificado pelo nome do negócio '
                + '(sem escopo de line items na API). Fechado = somente estágio "Ganho (Expand)".',
      },
    },
    clientes: clientesFinal,
    clientesSeries: clientesSeries.filter(x => ativos.has(x.cliente)),
    negocios: hsd.negocios,
    oraculo: { tabela: oraculoTab, series: oraculoSeries },
    // a tabela do Tino traz toda a carteira do painel, com ou sem uso
    tino: { tabela: tinoTab.filter(x => ativos.has(x.cliente)), series: tinoSeries },
    vestiPago: { tabela: vpTab, series: vpSeries },
    churn: { series: churnSeries },
    tarefas: hsd.tarefas,
  };
}

// ==================================================================== MAIN
(async () => {
  console.log('Painel de Clientes — carga de dados');
  console.log('ano ' + ANO + ', semanas 1..' + SEMANA_ATUAL);

  const bqd = await puxarBQ();
  const hsd = await puxarHubSpot().catch(e => {
    console.log('  HubSpot falhou: ' + e.message.slice(0, 160));
    return { negocios: [], tarefas: [] };
  });
  const data = montar(bqd, hsd);

  const saida = path.join(__dirname, 'dados.js');
  fs.writeFileSync(saida, 'window.PAINEL_DATA = ' + JSON.stringify(data) + ';\n', 'utf8');
  const mb = (fs.statSync(saida).size / 1048576).toFixed(2);

  console.log('\n[pronto] dados.js  ' + mb + ' MB');
  console.log('  clientes        ' + data.clientes.length);
  console.log('  séries carteira ' + data.clientesSeries.length);
  console.log('  negócios        ' + data.negocios.length);
  console.log('  tarefas         ' + data.tarefas.length);
  console.log('  oráculo         ' + data.oraculo.tabela.length + ' marcas / ' + data.oraculo.series.length + ' semanas-marca');
  console.log('  tino            ' + data.tino.tabela.length + ' marcas / ' + data.tino.series.length);
  console.log('  vesti pago      ' + data.vestiPago.tabela.length + ' marcas / ' + data.vestiPago.series.length);
  console.log('  churn           ' + data.churn.series.length);
})().catch(e => { console.error('\nFALHOU:', e.message); process.exit(1); });
