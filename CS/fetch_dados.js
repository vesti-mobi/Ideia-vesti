/**
 * fetch_dados.js — popula o Painel de Clientes (CS) com dados reais.
 *
 * Fontes
 *   BigQuery  vesti-data-499015.vestilake_BI   (somente SELECT)
 *   HubSpot   pipeline "Expand (Upgrades)" + tasks
 *
 * Saída: dados.js  ->  window.PAINEL_DATA, no formato documentado no index.html.
 *
 * Grão temporal: DIA. Cada série sai como (marca × data ISO), e é o painel que
 * agrupa em dia/semana/mês conforme o tamanho do período escolhido. Era semana
 * ISO até 26/08/2026 — mudou porque "mês fechado" não cabe em semana: a semana
 * 31 tem dias de julho e de agosto, e todo filtro mensal vazava para o mês vizinho.
 *
 * Janela: 1º de janeiro do ANO ANTERIOR até hoje. O ano a mais existe para a aba
 * de Bonificação, que compara ago/25 com ago/26 — sem ele não há ano contra ano.
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
const HOJE_ISO = HOJE.toISOString().slice(0, 10);
/* Início da janela de dados. Um ano inteiro a mais que o corrente: é o que a
   aba de Bonificação precisa para comparar o mesmo mês do ano passado. */
const ANO_BASE = ANO - 1;
const INICIO = ANO_BASE + '-01-01';

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

/* Inadimplência: marca com fatura do Iugu vencida há mais de N dias e ainda não
   paga. A régua é 10 dias por decisão da Laura (24/08/2026): abaixo disso a
   maioria é só a fatura sentada uns dias — no dia da medição, 62 das 124 marcas
   com algo vencido estavam entre 1 e 5 dias, e chamar isso de inadimplente
   encheria a aba de quem já ia pagar. Os dias de atraso vão numa coluna, então
   quem quiser conferir o caso limítrofe consegue. */
const DIAS_INADIMPLENCIA = 10;

/* Status do Iugu que contam como fatura em aberto. 'pending' é o normal;
   os outros três são fatura que também não entrou (venceu de vez, entrou
   parcial, está em contestação). 'canceled' e 'refunded' ficam de fora
   de propósito: essas a Vesti não vai receber e não são dívida do lojista. */
const STATUS_EM_ABERTO = "'pending','expired','partially_paid','in_protest'";

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
/* 'YYYY-MM-DD' passa direto; 'YYYY-Www' vira a segunda-feira daquela semana ISO.
   A API do Tino devolve um ou outro conforme a granularidade, e o painel só
   entende dia. */
function diaDoPeriodo(p) {
  const t = String(p || '');
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t.slice(0, 10);
  const m = t.match(/^(\d{4})-W(\d{1,2})$/);
  if (!m) return null;
  const base = new Date(Date.UTC(Number(m[1]), 0, 4));
  const seg = new Date(base);
  seg.setUTCDate(base.getUTCDate() - ((base.getUTCDay() + 6) % 7) + (Number(m[2]) - 1) * 7);
  return seg.toISOString().slice(0, 10);
}
const fmtBR = v => 'R$ ' + Math.round(num(v)).toLocaleString('pt-BR');
const r2 = v => Math.round(num(v) * 100) / 100;
const soDigitos = v => String(v || '').replace(/\D/g, '');
/* 'YYYY-MM' menos n meses. Serve o comparativo da bonificação: mês anterior
   (n=1) e mesmo mês do ano passado (n=12). */
function mesAntes(mes, n) {
  const [y, m] = String(mes).split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1 - n, 1));
  return d.toISOString().slice(0, 7);
}
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

/* Janela de datas, reaproveitada em todas as queries de série. Substituiu o
   antigo FILTRO_SEMANA (ano corrente, semanas 1..atual). */
const FILTRO_PERIODO = (col) => `
  DATE(CAST(${col} AS TIMESTAMP)) BETWEEN DATE '${INICIO}' AND DATE '${HOJE_ISO}'`;
// a data do dia, já como texto ISO, que é a chave de toda série
const DIA = (col) => `FORMAT_DATE('%Y-%m-%d', DATE(CAST(${col} AS TIMESTAMP)))`;

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

  const pedidos = await q('pedidos por marca × dia', `
    SELECT CAST(domainId AS STRING) dom,
      ${DIA('settings_createdAt')} d,
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
      AND ${FILTRO_PERIODO('settings_createdAt')}
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
  const vestipago = await q('Vesti Pago por marca × dia', `
    SELECT CAST(domainId AS STRING) dom,
      ${DIA('settings_createdAt')} d,
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
      AND ${FILTRO_PERIODO('settings_createdAt')}
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
  const interchange = await q('interchange líquido por marca × dia', `
    SELECT CAST(domainId AS STRING) dom,
      ${DIA('paidAt')} d,
      ROUND(SUM(SAFE_CAST(vestiPagoValue AS FLOAT64)),2) fee_bruto,
      ROUND(SUM(SAFE_CAST(mdrCardBrandValue AS FLOAT64)),2) taxa_banco,
      ROUND(SUM(SAFE_CAST(mdrVestiValue AS FLOAT64)),2) fee_vesti,
      ROUND(SUM(SAFE_CAST(antifraudValue AS FLOAT64)),2) antifraude
    FROM ${DS}.vestipago_transaction_detail
    WHERE paidAt IS NOT NULL AND SAFE_CAST(domainId AS INT64) IS NOT NULL
      AND ${FILTRO_PERIODO('paidAt')}
    GROUP BY 1,2`);

  const oraculoGmv = await q('Oráculo: GMV por marca × dia', `
    SELECT CAST(domainId AS STRING) dom,
      ${DIA('settings_createdAt')} d,
      ROUND(SUM(SAFE_CAST(summary_total AS FLOAT64)),2) gmv_iniciado,
      ROUND(SUM(IF(Tipo_Venda_Oraculo='Venda Direta', SAFE_CAST(summary_total AS FLOAT64), 0)),2) gmv_finalizado
    FROM ${DS}.oraculo_Pedidos
    WHERE Tipo_Venda_Oraculo IS NOT NULL AND settings_createdAt IS NOT NULL
      AND SAFE_CAST(domainId AS INT64) IS NOT NULL
      AND ${FILTRO_PERIODO('settings_createdAt')}
    GROUP BY 1,2`);

  const oraculoAtend = await q('Oráculo: atendimentos por marca × dia', `
    SELECT CAST(domain_id AS STRING) dom,
      ${DIA('DataReferencia')} d,
      COUNTIF(source='IA') ia, COUNT(*) total
    FROM ${DS}.oraculo_Atendimentos
    WHERE DataReferencia IS NOT NULL AND SAFE_CAST(domain_id AS INT64) IS NOT NULL
      AND ${FILTRO_PERIODO('DataReferencia')}
    GROUP BY 1,2`);

  /* O Tino saiu do BigQuery (ver puxarTino, seção 1B). A tabela
     sucessodocliente_rankings só tinha os links compartilhados ("cliques"), e ela
     nem sabe quem tem o produto: quem manda nisso é a base do próprio Tino. */

  /* Mensalidade tem que ser mensalidade. A fatura do Iugu junta plano, Oráculo,
     Filial, Assistente e taxa de ativação no mesmo total — só 75% é plano. Por
     isso a soma é feita LINHA A LINHA do item, separando o plano do resto.
     O "resto" continua sendo receita e entra na Receita total, em coluna própria. */
  const mensalidade = await q('Iugu: plano × outros, por CNPJ × dia', `
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
           FORMAT_DATE('%Y-%m-%d', DATE(due)) d,
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
      AND DATE(due) BETWEEN DATE '${INICIO}' AND DATE '${HOJE_ISO}'
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
           /* Inadimplência. A consulta só olhava fatura PAGA e pendente com
              vencimento FUTURO — a vencida era invisível, e era justamente ela
              que faltava na aba Churn. O vencimento mais antigo é o que vira
              "dias de atraso": é por ele que se mede há quanto tempo a marca
              está devendo, não pela fatura mais recente. */
           COUNTIF(status IN (${STATUS_EM_ABERTO})
                   AND due < CAST(CURRENT_DATE() AS STRING)) faturas_vencidas,
           ROUND(SUM(IF(status IN (${STATUS_EM_ABERTO})
                        AND due < CAST(CURRENT_DATE() AS STRING), cents, 0))/100, 2) valor_vencido,
           MIN(IF(status IN (${STATUS_EM_ABERTO})
                  AND due < CAST(CURRENT_DATE() AS STRING), due, NULL)) venc_mais_antigo,
           ARRAY_AGG(IF(status IN ('paid','externally_paid') AND plano IS NOT NULL,
                        STRUCT(due, cents, plano), NULL)
                     IGNORE NULLS ORDER BY due DESC LIMIT 1)[SAFE_OFFSET(0)] ultima
    FROM inv2
    GROUP BY 1`);

  /* ---- DATA DE IMPLANTAÇÃO DE CADA PRODUTO
     Pedido da Laura (26/08/2026): saber, em Tino / Vesti Pago / Oráculo, desde
     quando a marca tem o produto — sem isso não dá para ler "eventos no mês" de
     quem entrou dia 20.

     Vesti Pago: MongoDB_Payment_Companies.createdAt é a criação da conta de
     pagamento da marca — 1.841 contas, todas com data, de mar/2023 até hoje.
     Oráculo: o-configurations.created_at é a criação da configuração do produto.
     Cuidado ao ler: 699 dos 1.055 domínios têm created_at em jan/2026, que é
     quando a tabela foi criada — para esses, a data é do espelho, não da venda.
     Por isso o fetcher fica com a MENOR entre ela e o primeiro atendimento
     registrado, e marca a origem em `origem` para o painel avisar.
     O Tino já vinha com created_at na própria API (campo `entrouEm`). */
  const implantacaoVP = await q('Vesti Pago: data de implantação', `
    SELECT CAST(domainId AS STRING) dom,
           FORMAT_DATE('%Y-%m-%d', MIN(DATE(CAST(createdAt AS TIMESTAMP)))) implantado
    FROM ${DS}.MongoDB_Payment_Companies
    WHERE createdAt IS NOT NULL AND SAFE_CAST(domainId AS INT64) IS NOT NULL
    GROUP BY 1`);

  const implantacaoOraculo = await q('Oráculo: data de implantação', `
    WITH cfg AS (
      SELECT CAST(domain_id AS STRING) dom, MIN(DATE(created_at)) dt
      FROM ${DS}.\`o-configurations\`
      WHERE created_at IS NOT NULL AND SAFE_CAST(domain_id AS INT64) IS NOT NULL
      GROUP BY 1
    ),
    prim AS (
      SELECT CAST(domain_id AS STRING) dom, MIN(DATE(CAST(DataReferencia AS TIMESTAMP))) dt
      FROM ${DS}.oraculo_Atendimentos
      WHERE DataReferencia IS NOT NULL AND SAFE_CAST(domain_id AS INT64) IS NOT NULL
      GROUP BY 1
    )
    SELECT COALESCE(c.dom, p.dom) dom,
           FORMAT_DATE('%Y-%m-%d', LEAST(IFNULL(c.dt, p.dt), IFNULL(p.dt, c.dt))) implantado,
           FORMAT_DATE('%Y-%m-%d', c.dt) config,
           FORMAT_DATE('%Y-%m-%d', p.dt) primeiro_atendimento
    FROM cfg c FULL OUTER JOIN prim p USING(dom)`);

  /* ---- Filiais novas ("varejo"), para a aba de Bonificação.
     Definição da Laura (26/08/2026): varejo novo = FILIAL nova de uma marca que
     já existe. Não é conta nova no canal "Varejo Vesti" — esse canal está zerado
     desde jul/2025 — nem varejista cadastrado pela marca.

     Como se reconhece uma filial: odbc_companies.parent_id nunca é preenchido no
     espelho (conferido: zero linhas em 20 meses), então filial é a 2ª empresa em
     diante do mesmo domínio, por ordem de criação. É a mesma régua que o
     PainelElisa usa para deduplicar filial, e os nomes confirmam ("São Luis 2",
     "Teresina 4"). Dá de 2 a 22 por mês na carteira inteira.
     O CS é o do domínio: a filial não tem anjo próprio. */
  const filiaisNovas = await q('filiais novas por mês (varejo)', `
    WITH emp AS (
      SELECT CAST(domain_id AS STRING) dom, id,
             ANY_VALUE(company_name) company_name, ANY_VALUE(social_name) social_name,
             MIN(DATE(CAST(created_at AS TIMESTAMP))) criado
      FROM ${DS}.odbc_companies
      WHERE created_at IS NOT NULL AND SAFE_CAST(domain_id AS INT64) IS NOT NULL
      GROUP BY 1,2
    ),
    rk AS (
      SELECT e.*, ROW_NUMBER() OVER (PARTITION BY dom ORDER BY criado, e.id) rn FROM emp e
    )
    SELECT dom, FORMAT_DATE('%Y-%m-%d', criado) criado, rn,
           COALESCE(company_name, social_name) nome
    FROM rk
    WHERE rn > 1 AND criado BETWEEN DATE '${INICIO}' AND DATE '${HOJE_ISO}'`);

  return { cadastro, cadastroFora, pedidos, ultimoPedido, vestipago, oraculoGmv, oraculoAtend,
           interchange, mensalidade, faturas, implantacaoVP, implantacaoOraculo, filiaisNovas };
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
  /* `granularity: 'day'` no lugar de 'week': o painel passou a filtrar por data
     início/fim, então a série do Tino precisa do mesmo grão das outras. A rota
     devolve `period` como 'YYYY-MM-DD' no modo diário (e 'YYYY-Www' no semanal);
     o parser aceita os dois e converte a semana para a segunda-feira dela, para
     o dia continuar existindo caso a API ignore a granularidade pedida. */
  const porDia = [], totalDe = new Map();
  const daJanela = { date_from: INICIO, date_to: hoje };
  let feitas = 0;
  async function puxarMarca(slug) {
    const [serie, tot] = await Promise.all([
      tinoPost('timeline', { ...daJanela, granularity: 'day', companies: [slug] }),
      tinoPost('metrics', { ...daJanela, granularity: 'day', companies: [slug] }),
    ]);
    (serie || []).forEach(l => {
      const d = diaDoPeriodo(l.period);
      if (d) porDia.push({ d, company: slug, eventos: num(l.cnt) });
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
  console.log('  eventos na janela'.padEnd(44)
    + String(porDia.reduce((t, x) => t + x.eventos, 0)).padStart(8));
  console.log('  nunca acessaram (sem atividade nenhuma)'.padEnd(44)
    + String(slugs.filter(x => !comAtividade.has(x)).length).padStart(8));
  console.log('    nunca fizeram login (login_days = 0)'.padEnd(44)
    + String((acessos || []).filter(a => !num(a.login_days)).length).padStart(8));

  return { kpis: kpis || {}, acessos: acessos || [], marcas: [...marcas.values()],
           comAtividade, totalDe, porDia, tipos: tipos || [] };
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

/* ---- Retrato diário de quem tem integração.
   A Laura perguntou, em 26/08/2026, se dava para "passar a registrar" as
   integrações novas. Dava — mas só daqui para a frente: `odbc_domains` guarda
   qual integração a marca tem HOJE e não guarda desde quando, então não existe
   histórico para reconstruir o passado.

   O que este bloco faz: grava `integracoes_snapshot.json` (domínio -> integração,
   como está agora) e, comparando com o retrato da carga anterior, acrescenta as
   marcas que ganharam integração em `integracoes_novas.json`, com a data da
   carga. O histórico vai se formando sozinho, uma carga por dia. Enquanto ele
   for curto, o número da aba de Bonificação vem quase todo do HubSpot.

   Os dois arquivos são commitados pelo workflow junto do dados.js — sem isso o
   retrato nasceria vazio a cada execução e nada seria detectado. */
function registrarIntegracoes(porDom) {
  const fSnap = path.join(__dirname, 'integracoes_snapshot.json');
  const fHist = path.join(__dirname, 'integracoes_novas.json');
  const leJson = (f, padrao) => {
    try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return padrao; }
  };

  const agora = {};
  porDom.forEach(m => {
    const i = (m.integracao || '').trim();
    if (i && i !== 'Sem integração') agora[m.dom] = i;
  });

  const anterior = leJson(fSnap, null);
  const historico = leJson(fHist, []);
  const novas = [];

  /* Na PRIMEIRA carga não existe retrato anterior: aí tudo que tem integração
     pareceria "novo hoje", e o mês inteiro apareceria inflado. Então a primeira
     carga só fotografa, sem detectar nada. */
  if (anterior && anterior.doms) {
    const hoje = HOJE_ISO;
    const jaNoHistorico = new Set(historico.map(h => h.dom + '|' + h.integracao));
    Object.keys(agora).forEach(dom => {
      const antes = anterior.doms[dom];
      if (antes === agora[dom]) return;
      if (jaNoHistorico.has(dom + '|' + agora[dom])) return;
      const m = porDom.get(dom);
      novas.push({
        dom, data: hoje, integracao: agora[dom], anterior: antes || null,
        cliente: m ? m.nome : dom, cs: m ? m.cs : 'Sem CS',
      });
    });
  }

  const historicoNovo = historico.concat(novas);
  try {
    fs.writeFileSync(fSnap, JSON.stringify({ geradoEm: HOJE_ISO, doms: agora }, null, 0) + '\n', 'utf8');
    fs.writeFileSync(fHist, JSON.stringify(historicoNovo, null, 0) + '\n', 'utf8');
  } catch (e) {
    console.log('  não consegui gravar o retrato de integrações: ' + e.message);
  }
  console.log('  integrações: marcas com integração'.padEnd(44) + String(Object.keys(agora).length).padStart(8));
  console.log('    detectadas como novas nesta carga'.padEnd(44)
    + String(novas.length).padStart(8) + (anterior ? '' : '   (primeira carga: só fotografou)'));
  return historicoNovo;
}

/* ---- Empacotamento das séries diárias.
   Ir de semana para dia multiplicou as linhas por ~6 e o dados.js saltou de
   6,5 MB para 37 MB — inviável para um arquivo que é commitado todo dia num
   repositório público. Duas medidas resolveram:

     1. as séries guardam só o ANO CORRENTE. O ano a mais que a carga puxa
        serve à aba de Bonificação, e essa já sai pronta e agregada por mês.
     2. o que sobra vai em formato colunar: uma lista de nomes de coluna e uma
        matriz de valores, com dicionário para as colunas de texto (nome do
        cliente e domínio se repetem centenas de vezes). Corta ~75%.

   O painel desempacota no boot, em `desempacotar()`. Formato:
     { _p:1, c:['data','cliente',…], dic:{ cliente:[…nomes…] }, r:[[…],[…]] }
   Numa coluna com dicionário o valor guardado é o índice; null continua null. */
function empacotar(linhas) {
  if (!Array.isArray(linhas) || !linhas.length) return linhas;
  const cols = [];
  linhas.forEach(o => { for (const k in o) if (!cols.includes(k)) cols.push(k); });

  const dic = {};
  cols.forEach(k => {
    let texto = false;
    for (const o of linhas) {
      const v = o[k];
      if (v == null) continue;
      if (typeof v === 'string') { texto = true; break; }
      if (typeof v !== 'number' && typeof v !== 'boolean') return;   // objeto: deixa cru
    }
    if (!texto) return;
    const vistos = new Map();
    linhas.forEach(o => { const v = o[k]; if (typeof v === 'string' && !vistos.has(v)) vistos.set(v, vistos.size); });
    /* Dicionário só compensa quando o texto se repete muito. Num campo quase
       todo único (um id por linha) ele só acrescentaria uma indireção. */
    if (vistos.size <= linhas.length * 0.6) dic[k] = { idx: vistos, lista: [...vistos.keys()] };
  });

  const r = linhas.map(o => cols.map(k => {
    const v = o[k];
    if (v === undefined) return null;
    const d = dic[k];
    if (d && typeof v === 'string') return d.idx.get(v);
    return v;
  }));
  const dicPlano = {};
  for (const k in dic) dicPlano[k] = dic[k].lista;
  return { _p: 1, c: cols, dic: dicPlano, r };
}

function montar(bqd, hsd, tinoDados) {
  console.log('\n[montagem]');
  /* Toda série é (marca × dia). A guarda troca "semana entre 1 e a atual" por
     "data dentro da janela": é a mesma proteção contra linha fora do período,
     só que agora em dia. */
  const dataOk = d => typeof d === 'string' && d.length === 10 && d >= INICIO && d <= HOJE_ISO;

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

  // ---- séries diárias da carteira
  const serieCli = new Map();               // dom|data -> {}
  bqd.pedidos.forEach(r => {
    if (!dataOk(r.d) || !porDom.has(r.dom)) return;
    somaEm(serieCli, r.dom + '|' + r.d, {
      pedidos: r.pedidos, pedidosPagos: r.pagos, valorPedidos: r.valor,
      receitaAntecipacao: num(r.antec) * FATOR_ANTECIPACAO_VESTI,
    });
  });

  /* Interchange já LÍQUIDO do banco. Vem de vestipago_transaction_detail (a única
     fonte com a quebra do MDR) e não mais dos pedidos — por isso ele entra aqui,
     numa passada própria, e não junto do laço acima. */
  let feeBruto = 0, taxaBanco = 0;
  bqd.interchange.forEach(r => {
    if (!dataOk(r.d) || !porDom.has(r.dom)) return;
    feeBruto += num(r.fee_bruto); taxaBanco += num(r.taxa_banco);
    somaEm(serieCli, r.dom + '|' + r.d, {
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
    if (!dataOk(r.d)) return;
    const dom = domDaFatura(r);
    if (!dom) {
      mensPerdida++; valorPerdido += num(r.plano) + num(r.outros);
      const chave = (r.payer || r.cnpj || '(sem pagador)').slice(0, 46);
      perdidas.set(chave, r2((perdidas.get(chave) || 0) + num(r.plano) + num(r.outros)));
      return;
    }
    if (!domPorCnpj.get(r.cnpj)) mensPorNome++;
    somaEm(serieCli, dom + '|' + r.d, { receitaMensalidade: r.plano, receitaOutrosIugu: r.outros });
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
    const [dom, dia] = k.split('|');
    const m = porDom.get(dom);
    clientesSeries.push({
      data: dia, cliente: m.nome, dominio: dom,
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

  /* ---- inadimplência (Iugu)
     Devolve os números crus para o painel: quantas faturas venceram sem pagar,
     quanto está em aberto e há quantos dias vence a mais antiga. "Inadimplente"
     é só o atraso passar de DIAS_INADIMPLENCIA — mas os dias vão junto, para a
     coluna mostrar o caso de 8 dias sem precisar dizer que ele é inadimplente. */
  function avaliarFatura(fat) {
    const n = fat ? num(fat.faturas_vencidas) : 0;
    if (!n) return { faturasVencidas: 0, valorEmAberto: 0, vencMaisAntigo: null, diasAtraso: null, inadimplente: false };
    const venc = fat.venc_mais_antigo || null;
    const diasAtraso = venc ? Math.round((HOJE - new Date(venc)) / 864e5) : null;
    return {
      faturasVencidas: n,
      valorEmAberto: num(fat.valor_vencido),
      vencMaisAntigo: venc,
      diasAtraso,
      inadimplente: diasAtraso != null && diasAtraso >= DIAS_INADIMPLENCIA,
    };
  }

  // ---- risco de churn (regra explícita, sem caixa-preta)
  /* Cada critério que dispara vira uma frase em `motivos`, e é ela que aparece
     na coluna "Por que" da aba Churn. Regra: se algum critério GRAVE bate, o
     risco é Alto; senão, qualquer critério MÉDIO deixa Médio; sem nenhum, Baixo. */
  function avaliarRisco(m, ultimoPed, fat, inad) {
    const motivos = [];
    const diasPed = ultimoPed ? Math.round((HOJE - new Date(ultimoPed)) / 864e5) : null;
    if (diasPed == null) motivos.push('nunca vendeu');
    else if (diasPed > 15) motivos.push('sem pedido há ' + diasPed + ' dias');
    if (fat && fat.ultima_paga && fat.ultima_paga < limiteChurn) motivos.push('sem fatura paga desde ' + fat.ultima_paga);
    if (fat && !fat.proximo_venc) motivos.push('sem fatura futura em aberto');
    /* Inadimplência entra como critério de risco, não só como coluna: marca
       devendo há mais de DIAS_INADIMPLENCIA dias é candidata a churn mesmo
       vendendo bem. Abaixo da régua ainda conta, mas só como sinal médio. */
    if (inad.faturasVencidas) {
      motivos.push(inad.faturasVencidas + (inad.faturasVencidas > 1 ? ' faturas vencidas' : ' fatura vencida')
        + ' há ' + inad.diasAtraso + ' dias (' + fmtBR(inad.valorEmAberto) + ')');
    }
    const grave = (diasPed == null || diasPed > 45)
      || (fat && fat.ultima_paga && fat.ultima_paga < limiteChurn)
      || inad.inadimplente;
    const medio = (diasPed != null && diasPed > 15)
      || (fat && !fat.proximo_venc)
      || inad.faturasVencidas > 0;
    return { risco: grave ? 'Alto' : medio ? 'Médio' : 'Baixo', motivos, diasSemPedido: diasPed };
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
    /* A inadimplência é medida para TODA marca, inclusive a que já cancelou:
       fatura vencida é fato, não avaliação. O risco é que continua sem sentido
       para quem já saiu — por isso ele fica null e os motivos, vazios. */
    const inad = avaliarFatura(fat);
    const av = dataChurn
      ? { risco: null, motivos: [], diasSemPedido: ultimoPed ? Math.round((HOJE - new Date(ultimoPed)) / 864e5) : null }
      : avaliarRisco(m, ultimoPed, fat, inad);
    clientes.push({
      nome: m.nome, dominio: m.dom, cs: m.cs,
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
      /* Os números por trás do risco, cada um em campo próprio: a aba Churn
         mostra o racional em coluna, não só a etiqueta Alto/Médio/Baixo. */
      diasSemPedido: av.diasSemPedido,
      faturasVencidas: inad.faturasVencidas,
      valorEmAberto: inad.valorEmAberto,
      vencMaisAntigo: inad.vencMaisAntigo,
      diasAtraso: inad.diasAtraso,
      inadimplente: inad.inadimplente,
      dataChurn,
      /* Produtos contratados. Oráculo vem do próprio produto (tem atendimento
         registrado); Tino é preenchido logo abaixo, depois que a lista de marcas
         da API do Tino é casada com o cadastro. */
      temOraculo: !!m.temOraculo,
      temTino: false,
      _dom: m.dom, _cnpj: m.cnpj,
    });
  });

  /* ---- data de implantação de cada produto, por domínio.
     Entra como coluna nas abas Tino, Vesti Pago e Oráculo: sem ela não dá para
     ler "eventos no mês" de quem só tem o produto desde o dia 20. */
  const implVP = new Map();
  (bqd.implantacaoVP || []).forEach(r => { if (r.implantado) implVP.set(r.dom, r.implantado); });
  const implOra = new Map();
  (bqd.implantacaoOraculo || []).forEach(r => {
    if (!r.implantado) return;
    /* `origem` diz se a data é confiável. 699 dos 1.055 domínios têm config
       criada em jan/2026, que é quando a tabela nasceu — nesses, a data é do
       espelho e não da venda. Quando o primeiro atendimento é anterior, ele
       manda; quando os dois batem em jan/2026, o painel avisa. */
    const origem = (r.primeiro_atendimento && r.primeiro_atendimento === r.implantado)
      ? 'primeiro atendimento' : 'configuração do produto';
    implOra.set(r.dom, { data: r.implantado, origem });
  });
  console.log('  domínios com data de implantação'.padEnd(44)
    + ('VP ' + implVP.size + ' / Oráculo ' + implOra.size).padStart(8));

  // ---- produtos: tabelas + séries
  const nomeDe = dom => (porDom.get(dom) || {}).nome;
  const csDe = dom => (porDom.get(dom) || {}).cs;

  const oraSerie = new Map();
  bqd.oraculoGmv.forEach(r => {
    if (!dataOk(r.d) || !porDom.has(r.dom)) return;
    somaEm(oraSerie, r.dom + '|' + r.d, { gmvIniciado: r.gmv_iniciado, gmvFinalizado: r.gmv_finalizado });
  });
  bqd.oraculoAtend.forEach(r => {
    if (!dataOk(r.d) || !porDom.has(r.dom)) return;
    somaEm(oraSerie, r.dom + '|' + r.d, { atendimentos: r.total, atendimentosIA: r.ia });
  });
  const oraculoSeries = [], oraAcc = new Map();
  oraSerie.forEach((v, k) => {
    const [dom, dia] = k.split('|');
    oraculoSeries.push({
      data: dia, cliente: nomeDe(dom), dominio: dom,
      atendimentos: v.atendimentos || 0, atendimentosIA: v.atendimentosIA || 0,
      gmvIniciado: v.gmvIniciado || 0, gmvFinalizado: v.gmvFinalizado || 0,
    });
    somaEm(oraAcc, dom, { at: v.atendimentos || 0, ia: v.atendimentosIA || 0, gi: v.gmvIniciado || 0, gf: v.gmvFinalizado || 0 });
  });
  const oraculoTab = [...oraAcc.entries()].map(([dom, v]) => ({
    cliente: nomeDe(dom), dominio: dom, cs: csDe(dom),
    implantado: (implOra.get(dom) || {}).data || null,
    implantadoOrigem: (implOra.get(dom) || {}).origem || null,
    pctIA: v.at ? Math.round(v.ia / v.at * 100) : null,
    atendimentos: v.at, gmvIniciado: r2(v.gi), gmvFinalizado: r2(v.gf),
  })).filter(x => x.atendimentos || x.gmvIniciado);

  /* ---- Tino (API do produto)
     A lista de marcas agora é a da PRÓPRIA base do Tino: quem tem o produto é
     quem está lá, não quem apareceu clicando no espelho do BigQuery. Cada marca
     do Tino é casada com o cadastro por nome (ver casarMarcaTino); marca que não
     casa continua na tabela, só sem CS — sumir com cliente por causa de cadastro
     seria pior que mostrá-lo sem CS. */
  const gmvDia = new Map();        // dom|data -> valor pago, para a linha de GMV do Tino
  bqd.pedidos.forEach(r => { if (dataOk(r.d)) gmvDia.set(r.dom + '|' + r.d, num(r.valor)); });

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
    tinoDados.porDia.forEach(r => {
      if (!dataOk(r.d)) return;
      const dom = domDaCompany.get(r.company);
      const cliente = nomeDaCompany.get(r.company) || nomeBonito(r.company);
      tinoSeries.push({
        data: r.d, cliente, dominio: dom || null,
        cs: dom ? csDe(dom) : ((foraDaCarteira.get(r.company) || {}).cs || 'Sem CS'),
        eventos: r.eventos,
        receita: dom ? (gmvDia.get(dom + '|' + r.d) || 0) : 0,
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
        dominio: dom || null,
        cs: dom ? csDe(dom) : (f ? f.cs : 'Sem CS'),
        eventos: a.eventos || 0,
        eventosAno: num(t.eventos),
        sessoesAno: num(t.sessoes),
        diasAcesso: num(m.login_days),
        ultimoAcessoTino: m.last_login || null,
        statusTino: m.status === 'inactive' ? 'Inativa' : 'Ativa',
        /* A própria API do Tino já dá a data de entrada da marca na base — é
           essa a "data de implantação" do produto, sem precisar de outra fonte. */
        entrouEm: m.created_at ? String(m.created_at).slice(0, 10) : null,
        implantado: m.created_at ? String(m.created_at).slice(0, 10) : null,
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
    if (!dataOk(r.d)) return;
    feeLiquido.set(r.dom + '|' + r.d, num(r.fee_vesti) + num(r.antifraude));
  });

  const vpSeries = [], vpAcc = new Map();
  bqd.vestipago.forEach(r => {
    if (!dataOk(r.d) || !porDom.has(r.dom)) return;
    const fee = feeLiquido.get(r.dom + '|' + r.d) || 0;
    const antec = num(r.antec) * FATOR_ANTECIPACAO_VESTI;
    vpSeries.push({
      data: r.d, cliente: nomeDe(r.dom), dominio: r.dom,
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
    cliente: nomeDe(dom), dominio: dom, cs: csDe(dom),
    implantado: implVP.get(dom) || null,
    valorTransacionado: v.valorTransacionado,
    valorCartao: v.valorCartao, valorPix: v.valorPix,
    receitaFee: r2(v.receitaFee), receitaAntecipacao: r2(v.receitaAntecipacao),
    linksGerados: v.linksGerados, linksPagos: v.linksPagos,
  })).filter(x => x.linksGerados > 0 || x.valorTransacionado > 0);

  // ---- churn por dia
  const churnPorDia = new Map();
  clientes.forEach(c => {
    if (!c.dataChurn || !dataOk(c.dataChurn)) return;
    churnPorDia.set(c.nome + '|' + c.dataChurn, c._dom || null);
  });
  const churnSeries = [...churnPorDia.entries()].map(([k, dom]) => {
    const i = k.lastIndexOf('|');
    return { data: k.slice(i + 1), cliente: k.slice(0, i), dominio: dom, churns: 1 };
  });

  /* ---- só quem teve alguma atividade no ANO CORRENTE entra no painel.
     A janela de dados agora vai até o ano anterior (a bonificação precisa dele
     para comparar ago/25 com ago/26), mas a carteira continua sendo a de 2026 —
     senão o painel encheria de marca que morreu no ano passado. As séries de
     2025 ficam guardadas: quem está na carteira leva o histórico junto. */
  const doAnoCorrente = x => String(x.data || '').slice(0, 4) === String(ANO);
  const ativos = new Set([
    ...clientesSeries.filter(doAnoCorrente).map(x => x.cliente),
    ...oraculoSeries.filter(doAnoCorrente).map(x => x.cliente),
    ...tinoSeries.filter(doAnoCorrente).map(x => x.cliente),
    ...vpSeries.filter(doAnoCorrente).map(x => x.cliente),
    ...churnSeries.filter(doAnoCorrente).map(x => x.cliente),
  ]);
  const clientesFinal = clientes.filter(c => ativos.has(c.nome));
  clientesFinal.forEach(c => { c.temTino = domComTino.has(c._dom); });
  console.log('  marcas com Tino na tabela geral'.padEnd(44)
    + String(clientesFinal.filter(c => c.temTino).length).padStart(8));
  console.log('  marcas com Oráculo na tabela geral'.padEnd(44)
    + String(clientesFinal.filter(c => c.temOraculo).length).padStart(8));
  clientesFinal.forEach(c => { delete c._dom; delete c._cnpj; });
  console.log('  marcas com atividade em ' + ANO + ''.padEnd(20) + String(clientesFinal.length).padStart(13));


  /* ============================================================ BONIFICAÇÃO
     Pedido da Laura (26/08/2026): "vamos trazer primeiro o número; depois eu
     penso nas pontuações". Então aqui NÃO existe peso, nota nem ponto — só a
     medida crua de cada regra, por CS e por mês-calendário, com o valor do
     comparativo ao lado para o painel mostrar quanto subiu ou caiu.

     As sete regras vieram da planilha dela. Duas comparam com o mês anterior,
     duas com o mesmo mês do ano passado, três são contagem do próprio mês.
     Mês-calendário, não semana — foi justamente por isso que o painel inteiro
     trocou de grão. */
  const METRICAS_BONIFICACAO = [
    { k: 'tino60', titulo: 'Marcas com +60 eventos no Tino', unidade: 'marcas',
      comparacao: 'mesAnterior',
      regra: 'Marcas da carteira do CS que passaram de 60 eventos no Tino dentro do mês. '
           + 'O comparativo é o mesmo número no mês anterior — a régua da Laura é "a cada cliente extra".' },
    { k: 'mensalidade', titulo: 'Receita de mensalidade', unidade: 'R$',
      comparacao: 'mesAnterior',
      regra: 'Soma das linhas de PLANO das faturas Iugu pagas das marcas do CS, pelo vencimento. '
           + 'Não inclui Oráculo, Filial, Assistente nem ativação (esses são "Outros (Iugu)").' },
    { k: 'vestipago', titulo: 'Vesti Pago transacionado', unidade: 'R$',
      comparacao: 'anoAnterior',
      regra: 'Valor pago com provider Vesti Pago (cartão + PIX) nas marcas do CS. '
           + 'Comparado com o MESMO mês do ano anterior, como na planilha (ago/25 × ago/26).' },
    { k: 'reunioes', titulo: 'Reuniões com cliente', unidade: 'reuniões',
      comparacao: 'nenhuma',
      regra: 'Reuniões do HubSpot no mês, pelo dono do registro. Inclui as presenciais e as de evento — '
           + 'a base não separa uma coisa da outra.' },
    { k: 'integracoes', titulo: 'Novas integrações', unidade: 'integrações',
      comparacao: 'nenhuma',
      regra: 'Negócio de Integração GANHO no HubSpot dentro do mês (pipeline Expand). '
           + 'É o único registro de integração nova que existe hoje: o cadastro guarda quem TEM integração, '
           + 'não quando passou a ter. Por isso o fetcher começou a fotografar a carteira todo dia — '
           + 'ver integracoes_snapshot.json.' },
    { k: 'gmv', titulo: 'GMV da carteira', unidade: 'R$',
      comparacao: 'anoAnterior',
      regra: 'Valor dos pedidos pagos das marcas do CS no mês, comparado com o mesmo mês do ano anterior.' },
    { k: 'filiais', titulo: 'Varejos novos (filiais)', unidade: 'filiais',
      comparacao: 'nenhuma',
      regra: 'Filial nova de marca que já existia: a 2ª empresa em diante do mesmo domínio, criada no mês. '
           + 'O CS é o do domínio.' },
  ];

  /* O HubSpot grava o mesmo responsável com nome curto na carteira e completo no
     dono do registro ("Jennyfer Rabelo" × "Jennyfer Rabelo dos Santos"). Sem
     juntar os dois, a mesma pessoa vira duas linhas na tabela de bonificação.
     Mesma regra que o index.html usa nas abas de HubSpot. */
  const NOMES_CS_CARTEIRA = [...new Set([...porDom.values()].map(m => m.cs))]
    .filter(n => n && n !== 'Sem CS');
  const normalizarCsNome = n => {
    if (!n) return 'Sem CS';
    return NOMES_CS_CARTEIRA.find(c => c !== n && (n.startsWith(c + ' ') || c.startsWith(n + ' '))) || n;
  };

  const mesDe = d => String(d || '').slice(0, 7);
  const bonAcc = new Map();      // mes|cs|k -> valor
  const bonDet = new Map();      // mes|cs|k|cliente -> valor
  const somaBon = (mes, cs, k, v, cliente) => {
    if (!mes || mes.length !== 7 || !v) return;
    const nome = cs || 'Sem CS';
    const ch = mes + '|' + nome + '|' + k;
    bonAcc.set(ch, r2((bonAcc.get(ch) || 0) + num(v)));
    if (cliente) {
      const cd = ch + '|' + cliente;
      bonDet.set(cd, r2((bonDet.get(cd) || 0) + num(v)));
    }
  };

  // GMV e Vesti Pago: direto das séries diárias já filtradas pela carteira
  bqd.pedidos.forEach(r => {
    const m = porDom.get(r.dom); if (!m || !dataOk(r.d)) return;
    somaBon(mesDe(r.d), m.cs, 'gmv', num(r.valor), m.nome);
  });
  bqd.vestipago.forEach(r => {
    const m = porDom.get(r.dom); if (!m || !dataOk(r.d)) return;
    somaBon(mesDe(r.d), m.cs, 'vestipago', num(r.valor), m.nome);
  });
  // Mensalidade: passa pelo mesmo casamento CNPJ -> nome do pagador da tabela geral
  bqd.mensalidade.forEach(r => {
    if (!dataOk(r.d)) return;
    const dom = domDaFatura(r); if (!dom) return;
    const m = porDom.get(dom); if (!m) return;
    somaBon(mesDe(r.d), m.cs, 'mensalidade', num(r.plano), m.nome);
  });
  // Filiais novas: contagem, com o nome da filial no detalhe
  (bqd.filiaisNovas || []).forEach(r => {
    const m = porDom.get(r.dom); if (!m || !dataOk(r.criado)) return;
    somaBon(mesDe(r.criado), m.cs, 'filiais', 1, (r.nome || m.nome));
  });
  // Reuniões e integrações vêm do HubSpot
  (hsd.reunioes || []).forEach(r => somaBon(mesDe(r.data), normalizarCsNome(r.cs), 'reunioes', 1, r.cliente));
  /* Integração nova tem duas fontes e elas se somam sem contar duas vezes: o
     negócio ganho no HubSpot (o registro que existe hoje) e a detecção pelo
     retrato diário do cadastro (o registro que passa a existir a partir de
     agora — ver `registrarIntegracoes`). A chave de dedupe é mês+CS+marca. */
  const jaContada = new Set();
  const contarIntegracao = (mes, cs, cliente) => {
    const ch = mes + '|' + cs + '|' + (cliente || '');
    if (!mes || jaContada.has(ch)) return;
    jaContada.add(ch);
    somaBon(mes, cs, 'integracoes', 1, cliente);
  };
  (hsd.negocios || []).forEach(r => {
    if (r.status !== 'Ganho' || r.produto !== 'Integração') return;
    contarIntegracao(mesDe(r.data), normalizarCsNome(r.cs), r.cliente);
  });
  const integracoesDetectadas = registrarIntegracoes(porDom);
  integracoesDetectadas.forEach(r => contarIntegracao(mesDe(r.data), r.cs, r.cliente));
  /* Tino: a régua é por MARCA — só entra quem passou de 60 eventos no mês, e o
     que se conta é quantas marcas passaram, não quantos eventos. Por isso a
     soma é feita em dois tempos. */
  const tinoMes = new Map();     // mes|dominio -> eventos
  tinoSeries.forEach(x => {
    if (!x.dominio || !dataOk(x.data)) return;
    const ch = mesDe(x.data) + '|' + x.dominio;
    tinoMes.set(ch, (tinoMes.get(ch) || 0) + num(x.eventos));
  });
  const LIMITE_TINO = 60;
  tinoMes.forEach((eventos, ch) => {
    if (eventos < LIMITE_TINO) return;
    const [mes, dom] = ch.split('|');
    const m = porDom.get(dom); if (!m) return;
    somaBon(mes, m.cs, 'tino60', 1, m.nome);
  });

  const bonMeses = [...new Set([...bonAcc.keys()].map(k => k.split('|')[0]))].sort();
  const bonLinhas = [...bonAcc.entries()].map(([ch, valor]) => {
    const [mes, cs, k] = ch.split('|');
    const met = METRICAS_BONIFICACAO.find(x => x.k === k);
    let base = null;
    if (met && met.comparacao === 'mesAnterior') base = bonAcc.get(mesAntes(mes, 1) + '|' + cs + '|' + k) ?? 0;
    if (met && met.comparacao === 'anoAnterior') base = bonAcc.get(mesAntes(mes, 12) + '|' + cs + '|' + k) ?? 0;
    return { mes, cs, k, valor, base };
  });
  const bonDetalhe = [...bonDet.entries()].map(([ch, valor]) => {
    const [mes, cs, k, cliente] = ch.split('|');
    return { mes, cs, k, cliente, valor };
  });
  console.log('  bonificação'.padEnd(44)
    + (bonLinhas.length + ' linhas / ' + bonMeses.length + ' meses').padStart(8));

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
      dominio: m ? m.dom : null,
      canal: m ? m.canal : 'Sem canal',
    });
    delete o.empresa; delete o.empresaCnpj;
    return o;
  });
  if (tickets.length) {
    console.log('  tickets ligados a marca do painel'.padEnd(44)
      + (ticketsCasados + '/' + tickets.length).padStart(8));
  }

  /* ---- Domínio nas abas que vêm do HubSpot (Cross-sell, Upsell, Reuniões)
     Ali a linha nasce com o nome da empresa no HubSpot, não com o domínio, então
     o id é resolvido pelo mesmo casamento em três tentativas usado nos tickets.
     Sem casar fica null e a coluna mostra "—": inventar um id seria pior, porque
     a coluna existe justamente para ser chave de busca no admin. */
  const domDoNome = nome => {
    if (!nome) return null;
    const m = marcaPorNome.get(semAcento(nome)) || marcaPorChave.get(chaveMarca(nome));
    return m ? m.dom : null;
  };
  let hsCasados = 0, hsTotal = 0;
  [hsd.negocios, hsd.reunioes].forEach(lista => (lista || []).forEach(r => {
    hsTotal++;
    r.dominio = domDoNome(r.cliente);
    if (r.dominio) hsCasados++;
  }));
  if (hsTotal) {
    console.log('  negócios/reuniões com domínio'.padEnd(44)
      + (hsCasados + '/' + hsTotal).padStart(8));
  }

  return {
    meta: {
      ano: ANO, semanaAtual: SEMANA_ATUAL, cs: csLista, canais: canaisLista,
      /* A janela dos dados. O painel usa para não deixar escolher uma data
         anterior ao que existe no arquivo, e para o rótulo do filtro. */
      inicio: INICIO, hoje: HOJE_ISO, grao: 'dia',
      geradoEm: new Date().toISOString(),
      fatorAntecipacaoVesti: FATOR_ANTECIPACAO_VESTI,
      diasChurn: DIAS_CHURN,
      diasInadimplencia: DIAS_INADIMPLENCIA,
      tetoPedido: TETO_PEDIDO,
      avisos: {
        periodo: 'O painel passou a ser filtrado por DATA de início e fim (26/08/2026), no lugar da '
               + 'escolha de semanas. Motivo: semana ISO atravessa a virada do mês — a semana 31 de 2026 '
               + 'tem dias de julho e de agosto — e por isso nenhum filtro semanal fechava um mês. '
               + 'Todas as séries agora são por dia; o gráfico é que agrupa em dia, semana ou mês conforme '
               + 'o tamanho do período. A janela de dados começa em ' + INICIO + ' (um ano a mais que o '
               + 'corrente, para a aba de Bonificação comparar mês contra o mesmo mês do ano passado).',
        implantacao: 'Data de implantação por produto: Tino = created_at da marca na base do próprio Tino; '
               + 'Vesti Pago = criação da conta de pagamento (MongoDB_Payment_Companies.createdAt); '
               + 'Oráculo = a MENOR entre a criação da configuração (o-configurations.created_at) e o '
               + 'primeiro atendimento registrado. Cuidado no Oráculo: 699 dos 1.055 domínios têm '
               + 'configuração criada em jan/2026, que é quando a tabela nasceu no espelho — nesses a data '
               + 'é do espelho e não da venda. A coluna mostra de onde veio cada data.',
        bonificacao: 'A aba traz só o NÚMERO de cada regra por CS e por mês; não existe peso nem pontuação '
               + 'ainda (decidido com a Laura em 26/08/2026: "vamos trazer primeiro o número"). '
               + 'Integração nova sai do negócio ganho no HubSpot; o cadastro não guarda desde quando a '
               + 'marca tem integração, então a carga passou a fotografar isso todo dia e o histórico vai '
               + 'se formando (integracoes_novas.json). Varejo novo = filial nova de marca já existente.',
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
        churn: 'Derivado do Iugu: sem fatura paga há mais de ' + DIAS_CHURN + ' dias e sem fatura futura em aberto. '
             + 'NÃO vem de cancelamento formalizado no HubSpot — não existe pipeline de cancelamento chegando ao painel.',
        inadimplencia: 'Inadimplente = marca com fatura do Iugu vencida há mais de ' + DIAS_INADIMPLENCIA
             + ' dias e ainda não paga (status ' + STATUS_EM_ABERTO.replace(/'/g, '') + '). Fatura cancelada e '
             + 'estornada ficam de fora: essas a Vesti não vai receber e não são dívida do lojista. Os dias de '
             + 'atraso contam do vencimento MAIS ANTIGO em aberto, não do mais recente. A régua de '
             + DIAS_INADIMPLENCIA + ' dias foi decidida em 24/08/2026 para não encher a lista de fatura que '
             + 'venceu ontem — a coluna mostra os dias, então o caso limítrofe continua visível.',
        risco: 'Risco de churn é regra fixa, não modelo. ALTO quando bate qualquer um: nunca vendeu, sem pedido '
             + 'há mais de 45 dias, sem fatura paga há mais de ' + DIAS_CHURN + ' dias, ou inadimplente (fatura '
             + 'vencida há mais de ' + DIAS_INADIMPLENCIA + ' dias). MÉDIO quando bate qualquer um: sem pedido há '
             + 'mais de 15 dias, sem nenhuma fatura futura em aberto, ou com fatura vencida ainda dentro da régua. '
             + 'BAIXO quando não bate nenhum. Quem já cancelou fica sem risco — medir risco de quem já saiu não '
             + 'diz nada. O que disparou em cada marca vai na coluna "Por que".',
        negocios: 'Pipeline "Expand (Upgrades)" do HubSpot. Cross-sell x upsell classificado pelo nome do negócio '
                + '(sem escopo de line items na API). Fechado = somente estágio "Ganho (Expand)".',
      },
    },
    clientes: clientesFinal,
    /* As séries que vão para o painel são só do ano corrente — o ano anterior
       ficou na carga para alimentar a bonificação, que já sai agregada. */
    clientesSeries: empacotar(clientesSeries.filter(x => ativos.has(x.cliente) && doAnoCorrente(x))),
    negocios: hsd.negocios,
    oraculo: { tabela: oraculoTab, series: empacotar(oraculoSeries.filter(doAnoCorrente)) },
    /* A tabela do Tino traz as marcas que TÊM o produto (a lista vem da API do
       Tino), inclusive quem nunca entrou — é justamente essa lista que a CS
       precisa atacar. Não é filtrada por "teve atividade no painel". */
    tino: { tabela: tinoTab, series: empacotar(tinoSeries.filter(doAnoCorrente)),
            kpis: tinoKpis, tiposDeEvento: tinoTipos },
    vestiPago: { tabela: vpTab, series: empacotar(vpSeries.filter(doAnoCorrente)) },
    churn: { series: churnSeries.filter(doAnoCorrente) },
    bonificacao: {
      meses: bonMeses,
      metricas: METRICAS_BONIFICACAO,
      linhas: bonLinhas,
      detalhe: bonDetalhe,
      limiteTino: LIMITE_TINO,
    },
    reunioes: hsd.reunioes,
    tickets,
  };
}

// ==================================================================== MAIN
(async () => {
  console.log('Painel de Clientes — carga de dados');
  console.log('janela ' + INICIO + ' a ' + HOJE_ISO + ' (grão: dia)');

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
  const tam = s => (s && s._p ? s.r.length : (s || []).length);
  console.log('  séries carteira ' + tam(data.clientesSeries));
  console.log('  negócios        ' + data.negocios.length);
  console.log('  reuniões        ' + data.reunioes.length
    + ' (' + data.reunioes.filter(r => r.resultado === 'Fechou negócio').length + ' com negócio fechado)');
  console.log('  tickets         ' + data.tickets.length
    + ' (' + data.tickets.filter(t => t.situacao === 'Aberto').length + ' abertos)');
  console.log('  canais          ' + data.meta.canais.join(', '));
  console.log('  oráculo         ' + data.oraculo.tabela.length + ' marcas / ' + tam(data.oraculo.series) + ' dias-marca');
  console.log('  tino            ' + data.tino.tabela.length + ' marcas com o produto / '
    + tam(data.tino.series) + ' dias-marca');
  console.log('  vesti pago      ' + data.vestiPago.tabela.length + ' marcas / ' + tam(data.vestiPago.series));
  console.log('  churn           ' + data.churn.series.length);
  console.log('  bonificação     ' + data.bonificacao.linhas.length + ' linhas / '
    + data.bonificacao.meses.length + ' meses / ' + data.bonificacao.metricas.length + ' regras');
})().catch(e => { console.error('\nFALHOU:', e.message); process.exit(1); });
