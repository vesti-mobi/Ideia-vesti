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

/* Marcas que entram no painel MESMO sem o módulo de vendas no cadastro.
   A carteira é montada por "modulos contém vendas", que é o filtro que impede o
   painel de encher de conta de lojista comprador. Estas duas caem fora por esse
   filtro, mas são marcas de verdade: têm CS, canal, pedidos e usam o Tino.
   Decidido com a Laura em 18/08/2026, depois de a aba do Tino mostrá-las como
   "sem marca no cadastro". Se aparecer outra assim, é só acrescentar aqui. */
const DOMINIOS_EXTRA = [
  '1593235',   // Lete Moda Praia (Summer House) — CS Jennyfer, canal Starter
  '1833676',   // Santho Pano (Donna Sami)       — CS Luana, canal Vesti
];

/* Churn: marca sem fatura paga há mais de N dias e sem fatura futura em aberto. */
const DIAS_CHURN = 45;

/* Anjos que não são mais CS da carteira. As marcas deles passam a "Sem CS" —
   assim a coluna e o filtro contam a mesma história. */
const ANJOS_FORA = ['Shirley Silva', 'Priscila Argolo'];

/* A aba Reuniões mostra só o time de CS. Reunião de qualquer outro dono no
   HubSpot (vendas, parceiros, sem responsável) fica de fora. */
const CS_TIME = ['Luana Coutinho', 'Thamiris Ribeiro', 'Cristiane Canatelli',
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
const fmtBR = v => 'R$ ' + Math.round(num(v)).toLocaleString('pt-BR');
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
             CAST(partner_id AS STRING) partner_id,
             ROW_NUMBER() OVER (PARTITION BY ID ORDER BY updated_At DESC) rn
      FROM ${DS}.odbc_domains
      WHERE (LOWER(IFNULL(modulos,'')) LIKE '%vendas%'
             OR CAST(ID AS STRING) IN (${DOMINIOS_EXTRA.map(x => "'" + x + "'").join(',')}))
        AND LOWER(IFNULL(name,'')) NOT LIKE '%teste%'
        AND LOWER(IFNULL(name,'')) NOT LIKE '%andressa vesti%'
    ),
    comp AS (
      SELECT CAST(domain_id AS STRING) domain_id, tax_document, social_name, company_name, status,
             ROW_NUMBER() OVER (PARTITION BY domain_id ORDER BY created_at ASC) rn
      FROM ${DS}.odbc_companies
    ),
    /* Domínios que têm Oráculo: quem já registrou atendimento na base do produto.
       É o sinal mais direto de "usa" — a fatura do Iugu junta tudo num total só. */
    orac AS (
      SELECT DISTINCT CAST(domain_id AS STRING) dom
      FROM ${DS}.oraculo_Atendimentos
      WHERE SAFE_CAST(domain_id AS INT64) IS NOT NULL
    ),
    /* integration_id chega como "7.0" (float em texto) e odbc_integrations.id é
       inteiro — juntar direto não casa nada. Normaliza pelos dois lados. */
    integ AS (
      SELECT CAST(SAFE_CAST(id AS FLOAT64) AS INT64) id, ANY_VALUE(name) nome
      FROM ${DS}.odbc_integrations GROUP BY 1
    ),
    /* Canal = parceiro dono da conta (Vesti, Attasoft, Uemtel, Trial, Starter…).
       odbc_partners vem com cada linha duplicada no espelho, daí o GROUP BY. */
    part AS (
      SELECT CAST(id AS STRING) id, ANY_VALUE(name) nome
      FROM ${DS}.odbc_partners GROUP BY 1
    )
    SELECT d.id, d.name nome, d.integration_type, i.nome integracao_nome,
           a.name cs, c.tax_document cnpj, c.social_name, c.company_name, c.status status_empresa,
           SUBSTR(CAST(d.created_at AS STRING),1,10) criacao,
           p.nome canal,
           o.dom IS NOT NULL tem_oraculo
    FROM dom d
    LEFT JOIN comp c ON c.domain_id = d.id AND c.rn = 1
    LEFT JOIN ${DS}.odbc_angels a ON CAST(a.id AS STRING) = CAST(d.angel_id AS STRING)
    LEFT JOIN integ i ON i.id = CAST(SAFE_CAST(d.integration_id AS FLOAT64) AS INT64)
    LEFT JOIN part p ON p.id = d.partner_id
    LEFT JOIN orac o ON o.dom = d.id
    WHERE d.rn = 1`);

  /* Marcas que existem na Vesti mas ficam FORA do painel porque o cadastro não
     tem o módulo de vendas (conta só de compras). Elas não entram em nenhuma aba
     — servem só para dar nome e CS a uma marca do Tino que não casou com a
     carteira, como Lete Moda Praia e Santho Pano. Por isso o casamento contra
     esta lista é só por nome EXATO: com 16 mil linhas, palpite por semelhança
     erraria mais do que acertaria. */
  const cadastroFora = await q('marcas fora da carteira (só compras, com CS)', `
    WITH dom AS (
      SELECT CAST(ID AS STRING) id, ANY_VALUE(name) name,
             ANY_VALUE(CAST(angel_id AS STRING)) angel_id,
             ANY_VALUE(CAST(partner_id AS STRING)) partner_id
      FROM ${DS}.odbc_domains
      WHERE LOWER(IFNULL(modulos,'')) NOT LIKE '%vendas%'
        AND CAST(ID AS STRING) NOT IN (${DOMINIOS_EXTRA.map(x => "'" + x + "'").join(',')})
        AND angel_id IS NOT NULL AND CAST(angel_id AS STRING) NOT IN ('', 'N/A')
        AND LOWER(IFNULL(name,'')) NOT LIKE '%teste%'
      GROUP BY ID
    ),
    comp AS (
      SELECT CAST(domain_id AS STRING) domain_id, ANY_VALUE(social_name) social_name,
             ANY_VALUE(company_name) company_name
      FROM ${DS}.odbc_companies GROUP BY 1
    ),
    part AS (SELECT CAST(id AS STRING) id, ANY_VALUE(name) nome FROM ${DS}.odbc_partners GROUP BY 1)
    SELECT d.id, d.name nome, c.social_name, c.company_name, a.name cs, p.nome canal
    FROM dom d
    LEFT JOIN comp c ON c.domain_id = d.id
    LEFT JOIN ${DS}.odbc_angels a ON CAST(a.id AS STRING) = d.angel_id
    LEFT JOIN part p ON p.id = d.partner_id`);

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

  /* INTERCHANGE LÍQUIDO — o que sobra para a Vesti depois do banco.
     O fee do cartão cobrado do lojista (vestiPagoValue) não é receita inteira da
     Vesti: ele se divide em `mdrCardBrandValue`, que vai para o adquirente
     (IUGU / STARKBANK / PAGARME), e `mdrVestiValue`, que fica aqui. Conferido na
     base de 2026: 955.864 = 842.685 (banco) + 113.179 (Vesti), bate na casa do
     centavo nos três provedores.
     Por isso o interchange passa a ser medido em vestipago_transaction_detail e
     não mais em MongoDB_Pedidos_Geral: só esta tabela tem a quebra do MDR.
     Antifraude continua inteiro — é cobrança da Vesti, não taxa de banco. */
  const interchange = await q('interchange líquido por marca × semana', `
    SELECT CAST(domainId AS STRING) dom,
      EXTRACT(ISOWEEK FROM DATE(CAST(paidAt AS TIMESTAMP))) sem,
      ROUND(SUM(SAFE_CAST(vestiPagoValue AS FLOAT64)),2) fee_bruto,
      ROUND(SUM(SAFE_CAST(mdrCardBrandValue AS FLOAT64)),2) taxa_banco,
      ROUND(SUM(SAFE_CAST(mdrVestiValue AS FLOAT64)),2) fee_vesti,
      ROUND(SUM(SAFE_CAST(antifraudValue AS FLOAT64)),2) antifraude
    FROM ${DS}.vestipago_transaction_detail
    WHERE paidAt IS NOT NULL AND SAFE_CAST(domainId AS INT64) IS NOT NULL
      AND ${FILTRO_SEMANA('paidAt')}
    GROUP BY 1,2`);

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

  /* O Tino saiu do BigQuery (ver puxarTino, seção 1B). A tabela
     sucessodocliente_rankings só tinha os links compartilhados ("cliques"), e ela
     nem sabe quem tem o produto: quem manda nisso é a base do próprio Tino. */

  /* Mensalidade tem que ser mensalidade. A fatura do Iugu junta plano, Oráculo,
     Filial, Assistente e taxa de ativação no mesmo total — só 75% é plano. Por
     isso a soma é feita LINHA A LINHA do item, separando o plano do resto.
     O "resto" continua sendo receita e entra na Receita total, em coluna própria. */
  const mensalidade = await q('Iugu: plano × outros, por CNPJ × semana', `
    WITH itens AS (
      SELECT id, items_id,
             ANY_VALUE(payer_cpf_cnpj) cnpj, ANY_VALUE(payer_name) payer, ANY_VALUE(status) status,
             ANY_VALUE(items_description) item,
             ANY_VALUE(SAFE_CAST(items_price_cents AS FLOAT64)) cents,
             ANY_VALUE(COALESCE(due_date, SUBSTR(created_at_iso,1,10))) due
      FROM ${DS}.iugu_invoices
      WHERE payer_cpf_cnpj IS NOT NULL AND payer_cpf_cnpj != ''
      GROUP BY id, items_id
    )
    SELECT REGEXP_REPLACE(cnpj,'[^0-9]','') cnpj,
           /* payer_name entra para o resgate por nome: quando o CNPJ da fatura
              não existe no cadastro (caso Sawary), é por ele que a fatura acha
              a marca. O Iugu grava "Marca = RAZÃO SOCIAL LTDA". */
           ANY_VALUE(payer) payer,
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
    GROUP BY 1,3`);

  /* O plano não é uma coluna: é a linha de item mais cara da fatura, tirando
     desconto/Oráculo. Mesma regra do CS-Sucesso, para os dois painéis
     mostrarem o mesmo nome de plano. */
  const faturas = await q('Iugu: vencimento, plano e última paga', `
    WITH linhas AS (
      SELECT id, payer_cpf_cnpj cnpj, payer_name payer, status,
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
      SELECT id, ANY_VALUE(cnpj) cnpj, ANY_VALUE(payer) payer, ANY_VALUE(status) status,
             ANY_VALUE(cents) cents, ANY_VALUE(due) due
      FROM linhas GROUP BY id
    ),
    inv2 AS (SELECT inv.*, p.plano FROM inv LEFT JOIN plano_da_fatura p USING(id))
    SELECT REGEXP_REPLACE(cnpj,'[^0-9]','') cnpj, ANY_VALUE(payer) payer,
           MIN(IF(status='pending' AND due >= CAST(CURRENT_DATE() AS STRING), due, NULL)) proximo_venc,
           MAX(IF(status IN ('paid','externally_paid'), due, NULL)) ultima_paga,
           ARRAY_AGG(IF(status IN ('paid','externally_paid') AND plano IS NOT NULL,
                        STRUCT(due, cents, plano), NULL)
                     IGNORE NULLS ORDER BY due DESC LIMIT 1)[SAFE_OFFSET(0)] ultima
    FROM inv2
    GROUP BY 1`);

  return { cadastro, cadastroFora, pedidos, ultimoPedido, vestipago, oraculoGmv, oraculoAtend,
           interchange, mensalidade, faturas };
}

// ============================================================ 1B. API DO TINO
/* Por que não vem mais do BigQuery: o espelho só tem os links compartilhados
   (`sucessodocliente_rankings`), e a partir dele a única resposta possível era
   "quem clicou". Quem TEM o Tino, quem nunca entrou, quantos eventos a marca
   gerou — isso só existe na base do produto, atrás desta API.
   O painel do Tino (admin.tino.vesti.com.br) usa exatamente estas rotas.

   Rotas usadas (todas POST, corpo {date_from, date_to, companies[], granularity}):
     customer_kpis     -> total_brands / active / inactive / never_accessed
     login_days        -> por marca: created_at, last_login, login_days, status
     company_list      -> marcas com atividade no período
     companies_chart   -> por marca: total_events e sessions no período
     event_types       -> composição dos eventos (product_expand, filter_apply…)

   Credencial: TINO_USER / TINO_PASS. NÃO tem valor padrão no código — este
   repositório é público. Em CI, secrets do vesti-mobi/dados. */
const TINO_URL = process.env.TINO_URL
  || 'https://allblue-tinindo-tracking-667335277398.us-central1.run.app';
const TINO_USER = process.env.TINO_USER || '';
const TINO_PASS = process.env.TINO_PASS || '';

function tinoPost(rota, corpo) {
  return new Promise((res, rej) => {
    const b = JSON.stringify(corpo);
    const u = new URL(TINO_URL + '/' + rota);
    const req = https.request({
      hostname: u.hostname, path: u.pathname, method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(b),
        'X-Dashboard-User': TINO_USER,
        'X-Dashboard-Password': TINO_PASS,
      },
    }, r => {
      let d = '';
      r.on('data', c => d += c);
      r.on('end', () => {
        if (r.statusCode !== 200) return rej(new Error(`Tino /${rota} -> ${r.statusCode}: ${d.slice(0, 200)}`));
        try { res(JSON.parse(d).data); } catch (e) { rej(e); }
      });
    });
    req.on('error', rej);
    req.setTimeout(60000, () => req.destroy(new Error(`Tino /${rota}: timeout`)));
    req.end(b);
  });
}

/* Segunda e domingo (UTC) de uma semana ISO — a API recorta por data, não por
   semana, então cada semana do painel vira uma janela de 7 dias. */
function limitesDaSemana(s, ano) {
  const base = new Date(Date.UTC(ano, 0, 4));
  const seg = new Date(base);
  seg.setUTCDate(base.getUTCDate() - ((base.getUTCDay() + 6) % 7) + (s - 1) * 7);
  const dom = new Date(seg);
  dom.setUTCDate(seg.getUTCDate() + 6);
  return [seg.toISOString().slice(0, 10), dom.toISOString().slice(0, 10)];
}

async function puxarTino() {
  console.log('\n[Tino] ' + TINO_URL);
  if (!TINO_USER || !TINO_PASS) {
    console.log('  sem TINO_USER/TINO_PASS — a aba do Tino fica vazia');
    return null;
  }
  const hoje = HOJE.toISOString().slice(0, 10);
  const tudo = { date_from: '2020-01-01', date_to: hoje, companies: [], granularity: 'day' };

  const kpis = await tinoPost('customer_kpis', tudo);
  const acessos = await tinoPost('login_days', tudo);
  const lista = await tinoPost('company_list', tudo);
  const tipos = await tinoPost('event_types',
    { date_from: ANO + '-01-01', date_to: hoje, companies: [], granularity: 'day' });

  const marcas = new Map();
  (acessos || []).forEach(a => marcas.set(a.company, a));
  (lista || []).forEach(c => { if (!marcas.has(c)) marcas.set(c, { company: c }); });
  const slugs = [...marcas.keys()];
  /* company_list = marcas que registraram alguma atividade. É por ela que a API
     define "nunca acessou" no KPI (total_brands - company_list = 12 hoje). */
  const comAtividade = new Set(lista || []);

  /* POR QUE UMA CHAMADA POR MARCA, e não uma companies_chart por semana:
     companies_chart devolve no MÁXIMO 20 linhas — é o top 20 da tela do Tino, e
     o corte vale mesmo passando a lista de empresas no corpo (25 slugs
     explícitos voltam 20). Nas semanas cheias (S29 em diante) isso truncava
     justamente as semanas que a CS mais olha, e as marcas de cauda sumiam da
     série. `timeline` com granularity=week e uma empresa por chamada não tem
     teto e já devolve a semana ISO pronta ("2026-W29"); `metrics` da mesma
     empresa fecha os totais (eventos e sessões). São 2 chamadas por marca,
     ~180 no total, rodando 4 em paralelo. */
  const porSemana = [], totalDe = new Map();
  const doAno = { date_from: ANO + '-01-01', date_to: hoje };
  let feitas = 0;
  async function puxarMarca(slug) {
    const [serie, tot] = await Promise.all([
      tinoPost('timeline', { ...doAno, granularity: 'week', companies: [slug] }),
      tinoPost('metrics', { ...doAno, granularity: 'day', companies: [slug] }),
    ]);
    (serie || []).forEach(l => {
      const m = String(l.period || '').match(/^(\d{4})-W(\d{1,2})$/);
      if (!m || Number(m[1]) !== ANO) return;
      porSemana.push({ sem: Number(m[2]), company: slug, eventos: num(l.cnt) });
    });
    totalDe.set(slug, { eventos: num((tot || {}).total_events), sessoes: num((tot || {}).sessions) });
    process.stdout.write('\r  eventos por marca: ' + (++feitas) + '/' + slugs.length + '   ');
  }
  for (let i = 0; i < slugs.length; i += 4) {
    await Promise.all(slugs.slice(i, i + 4).map(puxarMarca));
  }
  console.log('');

  console.log('  marcas com Tino'.padEnd(44) + String(marcas.size).padStart(8));
  console.log('  total_brands (KPI da própria API)'.padEnd(44) + String((kpis || {}).total_brands || 0).padStart(8));
  console.log('  eventos no ano'.padEnd(44)
    + String(porSemana.reduce((t, x) => t + x.eventos, 0)).padStart(8));
  console.log('  nunca acessaram (sem atividade nenhuma)'.padEnd(44)
    + String(slugs.filter(x => !comAtividade.has(x)).length).padStart(8));
  console.log('    nunca fizeram login (login_days = 0)'.padEnd(44)
    + String((acessos || []).filter(a => !num(a.login_days)).length).padStart(8));

  return { kpis: kpis || {}, acessos: acessos || [], marcas: [...marcas.values()],
           comAtividade, totalDe, porSemana, tipos: tipos || [] };
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

/* A busca do HubSpot só pagina até 10.000 resultados por consulta — acima disso
   o `after` simplesmente para de andar e o resto some sem erro. Tickets do ano
   passam desse teto, então a janela é quebrada MÊS A MÊS e cada pedaço fica
   folgadamente abaixo do limite. */
async function buscarPorMes(objeto, props, propData, ano, ateMes, filtroExtra) {
  const out = [];
  for (let m = 0; m <= ateMes; m++) {
    const ini = Date.UTC(ano, m, 1), fim = Date.UTC(ano, m + 1, 1);
    const filtros = [{ filters: [
      { propertyName: propData, operator: 'GTE', value: String(ini) },
      { propertyName: propData, operator: 'LT', value: String(fim) },
      ...(filtroExtra || []),
    ] }];
    const lote = await buscarTudo(objeto, props, filtros,
      [{ propertyName: propData, direction: 'ASCENDING' }]);
    if (lote.length >= 10000) console.log('    ! ' + MESES_LOG[m] + ' bateu o teto de 10.000');
    out.push(...lote);
  }
  return out;
}
const MESES_LOG = ['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'];

/* Empresa associada a cada objeto (deal, ticket). Vem em lotes de 100 e é mais
   confiável que tentar ler o nome da marca do assunto. */
async function empresasAssociadas(objeto, ids) {
  const empresaDo = {};
  for (let i = 0; i < ids.length; i += 100) {
    const lote = ids.slice(i, i + 100).map(id => ({ id: String(id) }));
    try {
      const j = await hs('POST', `/crm/v4/associations/${objeto}/companies/batch/read`, { inputs: lote });
      (j.results || []).forEach(r => {
        if (r.to && r.to.length) empresaDo[r.from.id] = r.to[0].toObjectId;
      });
    } catch (e) { /* associação é opcional */ }
  }
  const nome = {}, doc = {};
  const idsEmpresa = [...new Set(Object.values(empresaDo))];
  for (let i = 0; i < idsEmpresa.length; i += 100) {
    try {
      const j = await hs('POST', '/crm/v3/objects/companies/batch/read',
        { properties: ['name', 'cnpj', 'hs_tax_id'],
          inputs: idsEmpresa.slice(i, i + 100).map(id => ({ id: String(id) })) });
      (j.results || []).forEach(c => {
        nome[c.id] = c.properties.name;
        // o CNPJ mora em duas propriedades diferentes conforme quem cadastrou
        const d = [c.properties.cnpj, c.properties.hs_tax_id]
          .map(soDigitos).find(x => x.length >= 11);
        if (d) doc[c.id] = d;
      });
    } catch (e) { /* idem */ }
  }
  return { empresaDo, nome, doc };
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

/* Chave grosseira de marca: tira acento, pontuação, forma jurídica e as
   palavras que quase toda confecção usa. Serve só para casar a empresa do
   HubSpot com a marca do cadastro quando o nome não bate letra a letra
   ("Bella Modas Ltda" = "bella"). */
const chaveMarca = s => semAcento(s)
  .replace(/\b(ltda|me|epp|eireli|s\/a|sa|comercio|confeccoes|confeccao|moda|modas|store|oficial)\b/g, '')
  .replace(/[^a-z0-9]/g, '');

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
  if (!HS_TOKEN) { console.log('  sem HUBSPOT_TOKEN — negócios, reuniões e tickets ficam vazios'); return { negocios: [], reunioes: [], tickets: [] }; }

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
  const { empresaDo: empresaDoDeal, nome: nomeEmpresa } =
    await empresasAssociadas('deals', deals.map(d => d.id));
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

  const reunioes = await puxarReunioes(owners);

  const tickets = await puxarTickets(owners);

  return { negocios, reunioes, tickets };
}

/* ------------------------------------------------------------- reuniões
   Mesma leitura do painel PlanilhasEPainelCS
   (https://vesti-mobi.github.io/dados/PlanilhasEPainelCS/): reunião realizada
   pelo time de CS e, do outro lado, o negócio que foi fechado depois dela.

   A atribuição é a mesma de lá, para os dois painéis contarem a mesma coisa: o
   negócio ganho é creditado à ÚLTIMA reunião daquela empresa (com aquele CS)
   anterior ao fechamento. Sem isso, uma empresa com cinco reuniões e um negócio
   viraria cinco negócios. Reunião que não tem negócio depois fica "Sem negócio";
   reunião futura fica "Agendada". */
async function puxarReunioes(owners) {
  const desde = Date.UTC(ANO, 0, 1);
  const brutas = await buscarTudo('meetings',
    ['hs_meeting_title', 'hs_meeting_start_time', 'hs_meeting_outcome', 'hubspot_owner_id'],
    [{ filters: [{ propertyName: 'hs_meeting_start_time', operator: 'GTE', value: String(desde) }] }],
    [{ propertyName: 'hs_meeting_start_time', direction: 'DESCENDING' }]);

  const { empresaDo, nome: nomeEmpresa } =
    await empresasAssociadas('meetings', brutas.map(m => m.id));

  const reunioes = brutas.map(m => ({
    reuniao: m.properties.hs_meeting_title || '(sem assunto)',
    data: iso(m.properties.hs_meeting_start_time),
    cliente: nomeEmpresa[empresaDo[m.id]] || '(sem empresa)',
    empresaId: empresaDo[m.id] || null,
    cs: owners[m.properties.hubspot_owner_id] || '(sem responsável)',
    resultado: 'Sem negócio',
    negocio: null,
    valor: 0,
  })).filter(r => r.data && Number(r.data.slice(0, 4)) === ANO && CS_TIME.includes(r.cs));
  console.log('  reuniões (só o time de CS)'.padEnd(44) + String(reunioes.length).padStart(8));

  /* Negócios ganhos de QUALQUER pipeline — o time fecha filial, upgrade e
     Vesti Pago em pipelines diferentes, e o painel das planilhas conta todos. */
  const ganhos = await buscarTudo('deals',
    ['dealname', 'amount', 'closedate', 'hs_is_closed_won'],
    [{ filters: [
      { propertyName: 'hs_is_closed_won', operator: 'EQ', value: 'true' },
      { propertyName: 'closedate', operator: 'GTE', value: String(desde) },
    ] }],
    [{ propertyName: 'closedate', direction: 'DESCENDING' }]);
  const { empresaDo: empresaDoDeal } = await empresasAssociadas('deals', ganhos.map(d => d.id));

  // (empresa) -> reuniões ordenadas da mais antiga para a mais nova
  const porEmpresa = new Map();
  reunioes.forEach(r => {
    if (!r.empresaId) return;
    if (!porEmpresa.has(r.empresaId)) porEmpresa.set(r.empresaId, []);
    porEmpresa.get(r.empresaId).push(r);
  });
  porEmpresa.forEach(l => l.sort((a, b) => a.data.localeCompare(b.data)));

  let creditados = 0, semReuniao = 0;
  ganhos.forEach(d => {
    const emp = empresaDoDeal[d.id];
    const fecha = iso(d.properties.closedate);
    const lista = emp && fecha ? porEmpresa.get(emp) : null;
    if (!lista) { semReuniao++; return; }
    const anteriores = lista.filter(r => r.data <= fecha);
    if (!anteriores.length) { semReuniao++; return; }
    const r = anteriores[anteriores.length - 1];
    r.resultado = 'Fechou negócio';
    r.negocio = d.properties.dealname || '';
    r.valor = r2(r.valor + num(d.properties.amount));
    creditados++;
  });
  const hoje = HOJE.toISOString().slice(0, 10);
  reunioes.forEach(r => { if (r.resultado === 'Sem negócio' && r.data > hoje) r.resultado = 'Agendada'; });
  console.log('  negócios ganhos no ano'.padEnd(44) + String(ganhos.length).padStart(8));
  console.log('    creditados a uma reunião / sem reunião'.padEnd(44)
    + (creditados + ' / ' + semReuniao).padStart(8));
  return reunioes.map(({ empresaId, ...r }) => r);
}

/* ---------------------------------------------------------------- tickets
   Todos os pipelines de ticket (Suporte, VestiPago, Integrações, Marketing,
   Comercial, Aplicativo, Inadimplente, Oráculo) — o pipeline vira coluna e
   filtro no painel, então nenhum é descartado aqui. */
const ORIGEM_TICKET = { CHAT: 'Chat', EMAIL: 'E-mail', FORM: 'Formulário', PHONE: 'Telefone',
                        CONVERSATION: 'Conversa', INTEGRATION: 'Integração', API: 'API' };
const PRIORIDADE = { LOW: 'Baixa', MEDIUM: 'Média', HIGH: 'Alta', URGENT: 'Urgente' };

async function puxarTickets(owners) {
  const pipes = await hs('GET', '/crm/v3/pipelines/tickets');
  const nomePipe = {}, nomeEstagio = {}, estagioFecha = {};
  pipes.results.forEach(p => {
    nomePipe[p.id] = (p.label || '').trim();
    (p.stages || []).forEach(s => {
      nomeEstagio[s.id] = (s.label || '').trim();
      /* Quem decide se o ticket está encerrado é o próprio HubSpot
         (metadata.ticketState). O rótulo só entra como reserva, para pipeline
         customizado que não marcou o estágio final. */
      estagioFecha[s.id] = (s.metadata && s.metadata.ticketState === 'CLOSED')
        || /encerrad|fechad|finalizad|conclu[íi]d/i.test(s.label || '');
    });
  });

  const brutos = await buscarPorMes('tickets',
    ['subject', 'hs_pipeline', 'hs_pipeline_stage', 'hs_ticket_category', 'hs_ticket_priority',
     'source_type', 'createdate', 'closed_date', 'hubspot_owner_id'],
    'createdate', ANO, HOJE.getUTCMonth());
  console.log('  tickets (todos os pipelines)'.padEnd(44) + String(brutos.length).padStart(8));

  const { empresaDo, nome: nomeEmpresa, doc: cnpjEmpresa } =
    await empresasAssociadas('tickets', brutos.map(t => t.id));
  console.log('  tickets com empresa associada'.padEnd(44) + String(Object.keys(empresaDo).length).padStart(8));

  const tickets = brutos.map(t => {
    const p = t.properties;
    const fechado = !!p.closed_date || !!estagioFecha[p.hs_pipeline_stage];
    const abertura = iso(p.createdate), fim = iso(p.closed_date);
    return {
      ticket: p.subject || '(sem assunto)',
      empresa: nomeEmpresa[empresaDo[t.id]] || null,   // vira `cliente` na montagem
      empresaCnpj: cnpjEmpresa[empresaDo[t.id]] || null,
      pipeline: nomePipe[p.hs_pipeline] || 'Outro',
      estagio: nomeEstagio[p.hs_pipeline_stage] || '—',
      situacao: fechado ? 'Encerrado' : 'Aberto',
      categoria: p.hs_ticket_category || null,
      prioridade: PRIORIDADE[p.hs_ticket_priority] || null,
      origem: ORIGEM_TICKET[p.source_type] || p.source_type || null,
      cs: owners[p.hubspot_owner_id] || '(sem responsável)',
      data: abertura,
      fechadoEm: fim,
      diasParaFechar: (abertura && fim)
        ? Math.max(0, Math.round((new Date(fim) - new Date(abertura)) / 864e5)) : null,
    };
  }).filter(t => t.data && Number(t.data.slice(0, 4)) === ANO);
  const encerrados = tickets.filter(t => t.situacao === 'Encerrado').length;
  console.log('    encerrados / abertos'.padEnd(44)
    + (encerrados + ' / ' + (tickets.length - encerrados)).padStart(8));

  return tickets;
}

// ================================================================ 3. MONTAGEM
/* Slug do Tino ("opera_kids") -> marca da carteira. A API não devolve domínio
   nem CNPJ, só o slug, então o casamento é por nome, em três tentativas:
   nome igual, nome sem espaços e, por último, um nome contido no outro
   ("cambos" = "Cambos Jeans", "miss_manu" = "MissManu"). Numa conferência com
   as 92 marcas do Tino isso casou 90; as que sobram aparecem sem CS.
   Compara contra nome do domínio, razão social e nome fantasia da empresa. */
const RUIDO_TINO = /\b(ltda|me|epp|eireli|sa|e|comercio|confeccao|confeccoes|de|do|da|das|dos|pecas|vestuario|acessorios|moda|modas|store|oficial|atacado|jeans|demo)\b/g;
const chaveTino = s => String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '')
  .toLowerCase().replace(/[^a-z0-9]+/g, ' ').replace(RUIDO_TINO, ' ').replace(/\s+/g, ' ').trim();
const nomeBonito = slug => String(slug || '').replace(/_/g, ' ')
  .split(' ').filter(Boolean).map(p => p[0].toUpperCase() + p.slice(1)).join(' ');

function casarMarcaTino(porDom, fora) {
  const cands = [];
  porDom.forEach(m => {
    const chaves = [...new Set([m.nome, m.social, m.fantasia].map(chaveTino).filter(Boolean))];
    if (chaves.length) cands.push({ dom: m.dom, nome: m.nome, chaves });
  });
  const exato = new Map(), semEspaco = new Map();
  cands.forEach(c => c.chaves.forEach(k => {
    if (!exato.has(k)) exato.set(k, c);
    const k2 = k.replace(/ /g, '');
    if (!semEspaco.has(k2)) semEspaco.set(k2, c);
  }));
  /* Índice das marcas de fora da carteira: só nome exato (ver cadastroFora). */
  const foraIdx = new Map();
  (fora || []).forEach(f => {
    [f.nome, f.social_name, f.company_name].forEach(n => {
      const k = chaveTino(n);
      if (k && !foraIdx.has(k)) foraIdx.set(k, f);
      const k2 = k && k.replace(/ /g, '');
      if (k2 && !foraIdx.has(k2)) foraIdx.set(k2, f);
    });
  });

  return function (slug) {
    const k = chaveTino(slug.replace(/_/g, ' '));
    if (!k) return null;
    if (exato.has(k)) return exato.get(k).dom;
    if (semEspaco.has(k.replace(/ /g, ''))) return semEspaco.get(k.replace(/ /g, '')).dom;
    const toks = k.split(' ');
    const parciais = cands.filter(c => c.chaves.some(ck => {
      const ct = ck.split(' ');
      return toks.every(t => ct.includes(t)) || ct.every(t => toks.includes(t));
    }));
    if (!parciais.length) {
      const f = foraIdx.get(k) || foraIdx.get(k.replace(/ /g, ''));
      if (!f) return null;
      // Não é domínio da carteira: devolve só o rótulo, para a aba do Tino
      // mostrar o nome certo e o CS em vez de "Sem CS".
      return { fora: true, nome: (f.nome || '').trim(), cs: f.cs || 'Sem CS' };
    }
    // empate: o nome mais curto é o mais provável (menos qualificadores colados)
    return parciais.sort((a, b) => a.nome.length - b.nome.length)[0].dom;
  };
}

function montar(bqd, hsd, tinoDados) {
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
      social: c.social_name || null,
      fantasia: c.company_name || null,
      integracao: c.integracao_nome || c.integration_type || 'Sem integração',
      // canal = parceiro dono da conta; "N/A" no cadastro é o mesmo que sem canal
      canal: (c.canal && c.canal !== 'N/A') ? c.canal : 'Sem canal',
      cnpj: soDigitos(c.cnpj),
      statusEmpresa: c.status_empresa,
      criacao: c.criacao,
      temOraculo: !!c.tem_oraculo,
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
      receitaAntecipacao: num(r.antec) * FATOR_ANTECIPACAO_VESTI,
    });
  });

  /* Interchange já LÍQUIDO do banco. Vem de vestipago_transaction_detail (a única
     fonte com a quebra do MDR) e não mais dos pedidos — por isso ele entra aqui,
     numa passada própria, e não junto do laço acima. */
  let feeBruto = 0, taxaBanco = 0;
  bqd.interchange.forEach(r => {
    if (!semanaOk(r.sem) || !porDom.has(r.dom)) return;
    feeBruto += num(r.fee_bruto); taxaBanco += num(r.taxa_banco);
    somaEm(serieCli, r.dom + '|' + r.sem, {
      receitaInterchange: num(r.fee_vesti) + num(r.antifraude),
      taxaBanco: num(r.taxa_banco),
    });
  });
  console.log('  fee de cartão bruto no ano'.padEnd(44) + fmtBR(feeBruto).padStart(8));
  console.log('    taxa paga aos bancos (Iugu/Stark/Pagarme)'.padEnd(44) + fmtBR(taxaBanco).padStart(8));
  console.log('    interchange líquido + antifraude'.padEnd(44)
    + fmtBR(bqd.interchange.reduce((t, r) => t + num(r.fee_vesti) + num(r.antifraude), 0)).padStart(8));

  /* Mensalidade entra por CNPJ -> domínio. Quando o CNPJ da fatura não existe no
     cadastro, tenta pelo NOME: o Iugu grava o pagador como "Marca = RAZÃO SOCIAL
     LTDA", então tanto o apelido quanto a razão servem de chave.
     Por que isso existe: a Sawary paga R$ 4.078/mês no CNPJ 00.422.351/0001-90 e
     o cadastro dela tem outro CNPJ (82.364.623/0001-08) — a fatura não achava a
     marca e a receita sumia da linha dela. Em 2026 são 925 faturas pagas
     (R$ 564k, 13% do faturamento Iugu) com CNPJ fora do cadastro.
     O casamento por nome é só EXATO depois de normalizar (chaveMarca tira forma
     jurídica e ruído) e só vale quando aponta para UMA marca — nome parecido
     entre marcas diferentes é comum demais para arriscar palpite. */
  const domPorCnpj = new Map();
  porDom.forEach(m => { if (m.cnpj) if (!domPorCnpj.has(m.cnpj)) domPorCnpj.set(m.cnpj, m.dom); });

  const domPorNome = new Map(), nomeAmbiguo = new Set();
  porDom.forEach(m => {
    [m.nome, m.social, m.fantasia].forEach(n => {
      const k = chaveMarca(n || '');
      if (!k || k.length < 4) return;
      if (domPorNome.has(k) && domPorNome.get(k) !== m.dom) nomeAmbiguo.add(k);
      else domPorNome.set(k, m.dom);
    });
  });
  nomeAmbiguo.forEach(k => domPorNome.delete(k));

  /* "Sawary = SAWARY CONFECCOES LTDA" -> tenta "sawary" e "sawary confeccoes". */
  function domDaFatura(r) {
    const porCnpj = domPorCnpj.get(r.cnpj);
    if (porCnpj) return porCnpj;
    const partes = String(r.payer || '').split('=');
    for (const parte of partes) {
      const k = chaveMarca(parte);
      if (k && k.length >= 4 && domPorNome.has(k)) return domPorNome.get(k);
    }
    return null;
  }

  let mensAplicada = 0, mensPorNome = 0, mensPerdida = 0, valorPerdido = 0;
  const perdidas = new Map();
  bqd.mensalidade.forEach(r => {
    if (!semanaOk(r.sem)) return;
    const dom = domDaFatura(r);
    if (!dom) {
      mensPerdida++; valorPerdido += num(r.plano) + num(r.outros);
      const chave = (r.payer || r.cnpj || '(sem pagador)').slice(0, 46);
      perdidas.set(chave, r2((perdidas.get(chave) || 0) + num(r.plano) + num(r.outros)));
      return;
    }
    if (!domPorCnpj.get(r.cnpj)) mensPorNome++;
    somaEm(serieCli, dom + '|' + r.sem, { receitaMensalidade: r.plano, receitaOutrosIugu: r.outros });
    mensAplicada++;
  });
  console.log('  linhas de mensalidade casadas'.padEnd(44) + String(mensAplicada).padStart(8));
  console.log('    dessas, resgatadas pelo nome do pagador'.padEnd(44) + String(mensPorNome).padStart(8));
  console.log('    ainda sem marca'.padEnd(44)
    + (mensPerdida + ' / ' + fmtBR(valorPerdido)).padStart(8));
  [...perdidas.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5)
    .forEach(([q, v]) => console.log('      ' + q.padEnd(48) + fmtBR(v).padStart(12)));

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

  /* Plano, vencimento e churn saem da MESMA fatura, e por isso precisam do mesmo
     resgate por nome do pagador. Sem ele, a marca cujo CNPJ não bate ficava
     "Sem plano", sem vencimento e — pior — candidata a churn, porque a regra é
     "sem fatura paga há 45 dias e sem fatura futura". */
  const fatPorCnpj = new Map(), fatPorNome = new Map();
  bqd.faturas.forEach(r => {
    fatPorCnpj.set(r.cnpj, r);
    String(r.payer || '').split('=').forEach(parte => {
      const k = chaveMarca(parte);
      if (k && k.length >= 4 && !fatPorNome.has(k)) fatPorNome.set(k, r);
    });
  });
  const faturaDaMarca = (m) => (m.cnpj && fatPorCnpj.get(m.cnpj))
    || fatPorNome.get(chaveMarca(m.nome || ''))
    || fatPorNome.get(chaveMarca(m.social || ''))
    || null;

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
    const fat = faturaDaMarca(m);
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
      canal: m.canal,
      plano: (fat && fat.ultima && fat.ultima.plano) ? String(fat.ultima.plano) : 'Sem plano',
      // entrada da marca na Vesti = criação do domínio (odbc_domains.created_at)
      dataCadastro: m.criacao || null,
      ultimoAcesso: null,                    // sem fonte no BigQuery — ver README
      ultimoPedido: ultimoPed,
      vencimento: fat ? fat.proximo_venc : null,
      riscoChurn: av.risco,
      motivosRisco: av.motivos,
      dataChurn,
      /* Produtos contratados. Oráculo vem do próprio produto (tem atendimento
         registrado); Tino é preenchido logo abaixo, depois que a lista de marcas
         da API do Tino é casada com o cadastro. */
      temOraculo: !!m.temOraculo,
      temTino: false,
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

  /* ---- Tino (API do produto)
     A lista de marcas agora é a da PRÓPRIA base do Tino: quem tem o produto é
     quem está lá, não quem apareceu clicando no espelho do BigQuery. Cada marca
     do Tino é casada com o cadastro por nome (ver casarMarcaTino); marca que não
     casa continua na tabela, só sem CS — sumir com cliente por causa de cadastro
     seria pior que mostrá-lo sem CS. */
  const gmvSemana = new Map();     // dom|sem -> valor pago, para a linha de GMV do Tino
  bqd.pedidos.forEach(r => { if (semanaOk(r.sem)) gmvSemana.set(r.dom + '|' + r.sem, num(r.valor)); });

  const tinoSeries = [], tinoTab = [];
  let tinoKpis = {}, tinoTipos = [], domComTino = new Set();
  if (tinoDados) {
    const casar = casarMarcaTino(porDom, bqd.cadastroFora);
    const domDaCompany = new Map();     // slug do Tino -> domínio da carteira
    const nomeDaCompany = new Map();    // slug do Tino -> nome exibido
    const foraDaCarteira = new Map();   // slug do Tino -> { nome, cs } de quem só tem "compras"
    tinoDados.marcas.forEach(m => {
      const achado = casar(m.company);
      /* Três desfechos: domínio da carteira, marca que existe na Vesti mas está
         fora do painel (só módulo de compras), ou nada. */
      if (achado && achado.fora) {
        foraDaCarteira.set(m.company, achado);
        nomeDaCompany.set(m.company, achado.nome || nomeBonito(m.company));
      } else if (achado) {
        domDaCompany.set(m.company, achado); domComTino.add(achado);
        nomeDaCompany.set(m.company, nomeDe(achado));
      } else {
        nomeDaCompany.set(m.company, nomeBonito(m.company));
      }
    });
    if (foraDaCarteira.size) {
      console.log('  marcas do Tino fora da carteira'.padEnd(44) + String(foraDaCarteira.size).padStart(8));
      foraDaCarteira.forEach((v, k) => console.log('    ' + k.padEnd(28) + (v.nome + ' · ' + v.cs)));
    }

    const acc = new Map();              // slug -> {eventos} do período
    tinoDados.porSemana.forEach(r => {
      if (!semanaOk(r.sem)) return;
      const dom = domDaCompany.get(r.company);
      const cliente = nomeDaCompany.get(r.company) || nomeBonito(r.company);
      tinoSeries.push({
        semana: r.sem, cliente,
        cs: dom ? csDe(dom) : ((foraDaCarteira.get(r.company) || {}).cs || 'Sem CS'),
        eventos: r.eventos,
        receita: dom ? (gmvSemana.get(dom + '|' + r.sem) || 0) : 0,
      });
      somaEm(acc, r.company, { eventos: r.eventos });
    });


    tinoDados.marcas.forEach(m => {
      const dom = domDaCompany.get(m.company);
      const t = tinoDados.totalDe.get(m.company) || {};
      const a = acc.get(m.company) || {};
      const f = foraDaCarteira.get(m.company);
      tinoTab.push({
        cliente: nomeDaCompany.get(m.company),
        cs: dom ? csDe(dom) : (f ? f.cs : 'Sem CS'),
        eventos: a.eventos || 0,
        eventosAno: num(t.eventos),
        sessoesAno: num(t.sessoes),
        diasAcesso: num(m.login_days),
        ultimoAcessoTino: m.last_login || null,
        statusTino: m.status === 'inactive' ? 'Inativa' : 'Ativa',
        entrouEm: m.created_at ? String(m.created_at).slice(0, 10) : null,
        /* "Nunca acessou" = nenhuma atividade registrada, que é como o próprio
           Tino conta no card do admin. NÃO é "login_days = 0": quatro marcas
           entram sem que a API registre dia de login (SSO da Vesti) e por isso
           apareciam como se nunca tivessem entrado. */
        nuncaUsou: !tinoDados.comAtividade.has(m.company),
        nuncaLogou: !num(m.login_days),
        /* Só é "sem cadastro" quem não existe na Vesti. Quem existe mas está
           fora do painel (conta de compras) aparece com nome e CS de verdade. */
        semCadastro: !dom && !f,
        foraDaCarteira: !!f,
      });
    });
    tinoKpis = tinoDados.kpis;
    tinoTipos = tinoDados.tipos;
  }

  /* O fee da aba Vesti Pago segue a MESMA régua do interchange da tabela geral:
     já sem a taxa do banco. Manter um bruto aqui e um líquido lá faria a mesma
     receita aparecer com dois valores no mesmo painel. O valor transacionado e
     os links continuam vindo dos pedidos — só o fee troca de fonte. */
  const feeLiquido = new Map();
  bqd.interchange.forEach(r => {
    if (!semanaOk(r.sem)) return;
    feeLiquido.set(r.dom + '|' + r.sem, num(r.fee_vesti) + num(r.antifraude));
  });

  const vpSeries = [], vpAcc = new Map();
  bqd.vestipago.forEach(r => {
    if (!semanaOk(r.sem) || !porDom.has(r.dom)) return;
    const fee = feeLiquido.get(r.dom + '|' + r.sem) || 0;
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
  clientesFinal.forEach(c => { c.temTino = domComTino.has(c._dom); });
  console.log('  marcas com Tino na tabela geral'.padEnd(44)
    + String(clientesFinal.filter(c => c.temTino).length).padStart(8));
  console.log('  marcas com Oráculo na tabela geral'.padEnd(44)
    + String(clientesFinal.filter(c => c.temOraculo).length).padStart(8));
  clientesFinal.forEach(c => { delete c._dom; delete c._cnpj; });
  console.log('  marcas com atividade em ' + ANO + ''.padEnd(20) + String(clientesFinal.length).padStart(13));

  const csLista = [...new Set(clientesFinal.map(c => c.cs))].sort();
  const canaisLista = [...new Set(clientesFinal.map(c => c.canal))]
    .sort((a, b) => a === 'Sem canal' ? 1 : b === 'Sem canal' ? -1 : a.localeCompare(b, 'pt-BR'));

  /* Ticket -> marca do painel pelo nome da empresa associada no HubSpot
     (normalizado, sem acento). Casando, o ticket herda o canal e passa a
     obedecer ao filtro de canal; sem casar, fica "Sem canal" mas continua
     visível — sumir com ticket por causa de cadastro seria pior. */
  const marcaPorNome = new Map(), marcaPorChave = new Map(), marcaPorCnpj = new Map();
  porDom.forEach(m => {
    [m.nome, m.social].filter(Boolean).forEach(n => {
      const k = semAcento(n);
      if (k && !marcaPorNome.has(k)) marcaPorNome.set(k, m);
      const c = chaveMarca(n);
      if (c.length > 3 && !marcaPorChave.has(c)) marcaPorChave.set(c, m);
    });
    if (m.cnpj && !marcaPorCnpj.has(m.cnpj)) marcaPorCnpj.set(m.cnpj, m);
  });
  let ticketsCasados = 0;
  const tickets = (hsd.tickets || []).map(t => {
    /* Três tentativas, da mais para a menos exata. O nome cru resolve a maioria;
       a chave sem ruído ("Ltda", "Modas", pontuação) pega a diferença entre o
       nome da empresa no HubSpot e o do domínio; o CNPJ salva os casos em que
       o nome no HubSpot é outro ("Claribel Confeções - Starter - Uemtel"). */
    const m = (t.empresa && marcaPorNome.get(semAcento(t.empresa)))
           || (t.empresa && marcaPorChave.get(chaveMarca(t.empresa)))
           || (t.empresaCnpj && marcaPorCnpj.get(t.empresaCnpj))
           || null;
    if (m) ticketsCasados++;
    const o = Object.assign({}, t, {
      cliente: m ? m.nome : (t.empresa || '(sem marca)'),
      canal: m ? m.canal : 'Sem canal',
    });
    delete o.empresa; delete o.empresaCnpj;
    return o;
  });
  if (tickets.length) {
    console.log('  tickets ligados a marca do painel'.padEnd(44)
      + (ticketsCasados + '/' + tickets.length).padStart(8));
  }

  return {
    meta: {
      ano: ANO, semanaAtual: SEMANA_ATUAL, cs: csLista, canais: canaisLista,
      geradoEm: new Date().toISOString(),
      fatorAntecipacaoVesti: FATOR_ANTECIPACAO_VESTI,
      diasChurn: DIAS_CHURN,
      tetoPedido: TETO_PEDIDO,
      avisos: {
        ultimoAcesso: 'Sem fonte: não existe coluna de login/sessão do lojista no vestilake_BI.',
        dataCadastro: 'Data de cadastro = odbc_domains.created_at (criação do domínio da marca). '
                    + 'Completo em todas as marcas, de ago/2016 até hoje. Nenhuma empresa em '
                    + 'odbc_companies foi criada antes do domínio dela, então o domínio é mesmo a entrada na Vesti.',
        mensalidade: 'Receita mensalidade = SÓ as linhas de plano da fatura Iugu. Oráculo, Filial, '
               + 'Assistente e taxa de ativação saem em "Outros (Iugu)" — juntos eram 25% do que '
               + 'antes ia todo para a coluna de mensalidade. Os dois entram na Receita total.',
        receita: 'Interchange = fee do cartão MENOS a taxa do banco, mais o antifraude — ou seja, '
               + 'mdrVestiValue + antifraudValue em vestipago_transaction_detail. O que o lojista paga de fee '
               + '(vestiPagoValue) se divide em mdrCardBrandValue, que vai para o adquirente (Iugu, Starkbank, '
               + 'Pagarme), e mdrVestiValue, que fica com a Vesti — em 2026, 88% do fee é do banco. '
               + 'O antifraude continua inteiro: é cobrança da Vesti, não taxa de banco. '
               + 'Antecipação = payment_transaction_antecipationValue x ' + FATOR_ANTECIPACAO_VESTI
               + ' (parcela da Vesti, medida na mesma tabela) — essa também já é líquida do banco.',
        feeLiquido: 'Receita (fee) da aba Vesti Pago e Interchange da tabela geral são a MESMA régua: '
                  + 'fee do cartão menos o MDR do banco, mais o antifraude. A semana do fee é a do '
                  + 'pagamento (paidAt), a das outras colunas é a do pedido — daí uma diferença de ~1%.',
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
          + 'A aba Reuniões mostra só: ' + CS_TIME.join(', ') + '.',
        reunioes: 'Reuniões do HubSpot (objeto meetings) do time de CS, pela data de início. Mesma leitura do '
               + 'painel PlanilhasEPainelCS: o negócio ganho é creditado à ÚLTIMA reunião daquela empresa antes '
               + 'do fechamento, para uma empresa com cinco reuniões e um negócio não virar cinco negócios. '
               + 'Negócio ganho de qualquer pipeline conta. Reunião com data futura fica "Agendada".',
        produtos: 'Tem Tino = a marca está na base do próprio Tino (API do produto), casada com o cadastro pelo '
               + 'nome. Tem Oráculo = a marca tem atendimento registrado em oraculo_Atendimentos.',
        canal: 'Canal = parceiro dono da conta (odbc_domains.partner_id -> odbc_partners.name): '
             + 'Vesti, Attasoft, Uemtel, Trial, Starter, Varejo Vesti… Marca sem parceiro ou com "N/A" '
             + 'aparece como "Sem canal". O filtro aceita mais de um canal ao mesmo tempo.',
        tickets: 'Tickets de TODOS os pipelines do HubSpot (Suporte, VestiPago, Integrações, Marketing, '
               + 'Comercial, Aplicativo, Inadimplente, Oráculo), abertos no ano. Encerrado = o HubSpot '
               + 'marcou o estágio como fechado ou preencheu a data de fechamento. O cliente vem da '
               + 'empresa associada ao ticket; quando essa empresa não casa com nenhuma marca do '
               + 'cadastro, o ticket fica sem canal (mas continua na lista). O período filtra pela '
               + 'data de abertura.',
        churn: 'Derivado do Iugu: sem fatura paga há mais de ' + DIAS_CHURN + ' dias e sem fatura futura em aberto.',
        negocios: 'Pipeline "Expand (Upgrades)" do HubSpot. Cross-sell x upsell classificado pelo nome do negócio '
                + '(sem escopo de line items na API). Fechado = somente estágio "Ganho (Expand)".',
      },
    },
    clientes: clientesFinal,
    clientesSeries: clientesSeries.filter(x => ativos.has(x.cliente)),
    negocios: hsd.negocios,
    oraculo: { tabela: oraculoTab, series: oraculoSeries },
    /* A tabela do Tino traz as marcas que TÊM o produto (a lista vem da API do
       Tino), inclusive quem nunca entrou — é justamente essa lista que a CS
       precisa atacar. Não é filtrada por "teve atividade no painel". */
    tino: { tabela: tinoTab, series: tinoSeries, kpis: tinoKpis, tiposDeEvento: tinoTipos },
    vestiPago: { tabela: vpTab, series: vpSeries },
    churn: { series: churnSeries },
    reunioes: hsd.reunioes,
    tickets,
  };
}

// ==================================================================== MAIN
(async () => {
  console.log('Painel de Clientes — carga de dados');
  console.log('ano ' + ANO + ', semanas 1..' + SEMANA_ATUAL);

  const bqd = await puxarBQ();
  const hsd = await puxarHubSpot().catch(e => {
    console.log('  HubSpot falhou: ' + e.message.slice(0, 160));
    return { negocios: [], reunioes: [], tickets: [] };
  });
  /* Tino: se a API cair, o painel carrega sem a aba em vez de abortar a carga
     inteira — o resto dos dados não tem nada a ver com ela. */
  const tinoDados = await puxarTino().catch(e => {
    console.log('  Tino falhou: ' + e.message.slice(0, 160));
    return null;
  });
  const data = montar(bqd, hsd, tinoDados);

  const saida = path.join(__dirname, 'dados.js');
  fs.writeFileSync(saida, 'window.PAINEL_DATA = ' + JSON.stringify(data) + ';\n', 'utf8');
  const mb = (fs.statSync(saida).size / 1048576).toFixed(2);

  console.log('\n[pronto] dados.js  ' + mb + ' MB');
  console.log('  clientes        ' + data.clientes.length);
  console.log('  séries carteira ' + data.clientesSeries.length);
  console.log('  negócios        ' + data.negocios.length);
  console.log('  reuniões        ' + data.reunioes.length
    + ' (' + data.reunioes.filter(r => r.resultado === 'Fechou negócio').length + ' com negócio fechado)');
  console.log('  tickets         ' + data.tickets.length
    + ' (' + data.tickets.filter(t => t.situacao === 'Aberto').length + ' abertos)');
  console.log('  canais          ' + data.meta.canais.join(', '));
  console.log('  oráculo         ' + data.oraculo.tabela.length + ' marcas / ' + data.oraculo.series.length + ' semanas-marca');
  console.log('  tino            ' + data.tino.tabela.length + ' marcas com o produto / '
    + data.tino.series.length + ' semanas-marca');
  console.log('  vesti pago      ' + data.vestiPago.tabela.length + ' marcas / ' + data.vestiPago.series.length);
  console.log('  churn           ' + data.churn.series.length);
})().catch(e => { console.error('\nFALHOU:', e.message); process.exit(1); });
