# PainelElisa — Migração Fabric → BigQuery (status / handoff)

**Objetivo:** dashboard igual ao de antes, mas puxando do BigQuery. Decisão: **BQ é a fonte da verdade** (opção B — mais atual/correto que o snapshot). Piso de dados = **junho/2025** (sem backfill). Âncora de reconciliação = **maio/2026** (mês fechado, dados completos nos dois).

**Acesso BQ:** projeto `vesti-data-499015`, dataset `vestilake_BI`, região us-central1. SA key: `C:\Users\Laura\Downloads\vesti-data-499015-7ea468dae45e.json`. Rodar com `py` + `google-cloud-bigquery` (GOOGLE_APPLICATION_CREDENTIALS). JSON de produção via `git show origin/main:PainelElisa/<arquivo>.json` (o clone local está com merge em conflito; NÃO usar os .json locais).

## Placar das 6 queries (fetch_elisa.py → fetch_elisa_bq.py)

| Query | Fonte BQ | Status maio/2026 |
|---|---|---|
| **Empresas** | odbc_domains/companies/angels/**partners(DEDUP)**/integrations + silver + iugu_subscriptions + iugu_invoices | ✅ 659≈656; plano 0 contradições; valor diffs = staleness (BQ mais atual) |
| **GMV** | MongoDB_Pedidos_Geral | ✅ qtTotal 19836=19836 EXATO; valTotal dif 0,00% |
| **Links** | sucessodocliente_products + cadastrouser | ✅ 430/442, total −0,6% |
| **Cliques** | sucessodocliente_rankings + cadastrouser | ⚠️ BQ CORRETO; Fabric inflava 2–8× (fan-out no CadastroUser; SUM inflava, COUNT DISTINCT dos links não). DECISÃO pendente |
| **Reativação** | iugu_invoices(DISTINCT id) + silver | ⚠️ BQ CORRETO; Fabric inflava ~150× (explosão do iugu_invoices; dom16 real=1, Fabric=150). DECISÃO pendente |
| **Produtos** | ❌ BLOQUEADO: falta ingerir `odbc_products` no BQ | RESOLVIDO O DIAGNÓSTICO (07/07): `active=true` NÃO corrige (tira só 182/1,28M linhas). Sem dup (dist_id==all_rows). Fator vs JSON varia 1.0–47× por domínio (mediana 1.09, média 2.63) → é grão de VARIANTE mesmo, insalvável. Dashboard antigo usa `dbo.ODBC_Products` (grão produto); no BQ só há `odbc_product_details`(variantes) e `sucessodocliente_products`(links). **AÇÃO: ingerir `ODBC_Products`→`odbc_products` no BQ** (db-crons), aí query = COUNT(*) por domain_id/mês igual Fabric. |
| 1º pedido | MongoDB_Pedidos_Geral | pendente (BQ só de 2025-07; piso jun/2025 aceito) |

## Decisões (RESOLVIDAS 07/07 c/ Laura)
1. **Cliques + Reativação → usar valor do BQ** (Laura confirmou "use bq os dois").
   - **Reativação:** painel antigo ~62k vs BQ ~5,6k (antigo 11× inflado por iugu_invoices explodido). Fix = DISTINCT id.
   - **Cliques:** painel antigo 10,2mi vs BQ 16,4mi. NÃO era fan-out do join (cadastrouser é 1:1; SUM cru≈SUM c/ join). A `rankings` é SNAPSHOT DIÁRIO (shared_links sobe/desce dia a dia, não é acumulado) → **SUM por dia é a agregação CORRETA** (igual PBIX). BQ maior só por cobertura melhor de snapshots. Query do handoff está certa.
2. **Produtos → BLOQUEADO em ingestão.** `odbc_products` não existe no BQ; `odbc_product_details` é grão de variante (fator 1–47× por loja, insalvável). AÇÃO: ingerir `ODBC_Products`→`odbc_products`. Enquanto isso, fetch_elisa_bq.py sai com Produtos como TODO.

## Gaps de dados achados (ingestão)
- ⚠️ `odbc_partners` DUPLICADA no BQ (48 linhas/24 ids = 2×, do WRITE_APPEND do full_load). Contornado com dedup na query; TABELA precisa dedup na origem. Auditar outras full_load.
- `MongoDB_Pedidos_Geral` só de 2025-07 (piso jun/2025 aceito, sem backfill).
- `products` possivelmente não ingerida (confirmar no teste de produtos).

## SQL BQ validado (montar o fetch_elisa_bq.py com isto; trocar janela pela do piso jun/2025)

### EMPRESAS (dedup partners é OBRIGATÓRIO)
```sql
WITH partners AS (
  SELECT id,name FROM (SELECT id,name,ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rn FROM `vestilake_BI.odbc_partners`) WHERE rn=1),
active_domains AS (
  SELECT d.ID id,d.name,d.angel_id,d.integration_id,d.partner_id,d.modulos,d.created_at
  FROM `vestilake_BI.odbc_domains` d
  WHERE LOWER(d.modulos) LIKE '%vendas%'
    AND (d.partner_id IS NULL OR d.partner_id NOT IN ('ff66c2f1-1f9f-456c-9308-028e48c89582','25fec57c-620c-4ecd-ae7d-cd4fee27b158'))
    AND LOWER(d.name) NOT LIKE '%teste%'),
atta_domains AS (
  SELECT d.ID id,d.name,d.angel_id,d.integration_id,d.partner_id,d.modulos,d.created_at
  FROM `vestilake_BI.odbc_domains` d JOIN partners p ON p.id=d.partner_id
  WHERE LOWER(d.modulos) LIKE '%vendas%' AND LOWER(d.name) NOT LIKE '%teste%' AND LOWER(p.name) IN ('atta','attasoft')),
elisa_domains AS (
  SELECT ad.id,ad.name,ad.angel_id,ad.integration_id,ad.partner_id,ad.modulos,ad.created_at domain_created_at
  FROM active_domains ad JOIN `vestilake_BI.odbc_angels` a ON a.id=ad.angel_id
  WHERE a.name IN ('Elisa Marques','Jennyfer Rabelo')
  UNION DISTINCT
  SELECT atd.id,atd.name,atd.angel_id,atd.integration_id,atd.partner_id,atd.modulos,atd.created_at FROM atta_domains atd),
ranked_companies AS (
  SELECT c.domain_id,c.tax_document,c.social_name,c.company_name,c.created_at,
    ROW_NUMBER() OVER(PARTITION BY c.domain_id ORDER BY c.created_at ASC) rn
  FROM `vestilake_BI.odbc_companies` c WHERE c.domain_id IN (SELECT id FROM elisa_domains)),
sub_best AS (SELECT domain_id,plan_name FROM (
    SELECT sc.domain_id,s.plan_name,ROW_NUMBER() OVER(PARTITION BY sc.domain_id ORDER BY
      CASE WHEN LOWER(s.active)='true' AND LOWER(s.suspended)='false' THEN 0 ELSE 1 END, s.updated_at DESC) rn
    FROM `vestilake_BI.silver_companiesativos_iugu` sc JOIN `vestilake_BI.iugu_subscriptions` s ON s.customer_id=sc.Customer_ID_Iugu) WHERE rn=1),
inv_best AS (SELECT domain_id,total_cents FROM (
    SELECT sc.domain_id,inv.total_cents,ROW_NUMBER() OVER(PARTITION BY sc.domain_id ORDER BY inv.created_at_iso DESC) rn
    FROM `vestilake_BI.silver_companiesativos_iugu` sc
    JOIN (SELECT DISTINCT id,customer_id,total_cents,status,created_at_iso FROM `vestilake_BI.iugu_invoices` WHERE status='paid') inv
      ON inv.customer_id=sc.Customer_ID_Iugu) WHERE rn=1)
SELECT d.id domain_id,d.name domain_name,d.domain_created_at,rc.tax_document cnpj,rc.social_name razao_social,
  rc.company_name,rc.rn row_num,rc.created_at company_created_at,a.name angel_name,i.name integration_name,
  p.name partner_name,sub_best.plan_name plano,inv_best.total_cents last_invoice_cents,d.modulos
FROM elisa_domains d JOIN ranked_companies rc ON rc.domain_id=d.id
LEFT JOIN `vestilake_BI.odbc_angels` a ON a.id=d.angel_id
LEFT JOIN `vestilake_BI.odbc_integrations` i ON i.id=SAFE_CAST(d.integration_id AS INT64)
LEFT JOIN partners p ON p.id=d.partner_id
LEFT JOIN sub_best ON sub_best.domain_id=d.id
LEFT JOIN inv_best ON inv_best.domain_id=d.id
```

### GMV (por domainId e dia; agregação mensal/semanal em Python igual ao build_gmv)
```sql
SELECT domainId domain_id, DATE(CAST(settings_createdAt AS TIMESTAMP)) dia,
  SUM(CASE WHEN payment_method='PIX' THEN CAST(summary_total AS FLOAT64) ELSE 0 END) val_pix,
  SUM(CASE WHEN payment_method='CREDIT_CARD' THEN CAST(summary_total AS FLOAT64) ELSE 0 END) val_cartao,
  SUM(CAST(summary_total AS FLOAT64)) val_total,
  SUM(CASE WHEN payment_method='PIX' THEN 1 ELSE 0 END) qt_pix,
  SUM(CASE WHEN payment_method='CREDIT_CARD' THEN 1 ELSE 0 END) qt_cartao,
  COUNT(*) qt_total,
  SUM(CASE WHEN payment_paidAt IS NOT NULL AND payment_paidAt<>'' THEN 1 ELSE 0 END) qt_paid
FROM `vestilake_BI.MongoDB_Pedidos_Geral`
WHERE SAFE_CAST(summary_total AS FLOAT64) > 0 AND SAFE_CAST(summary_total AS FLOAT64) < 50000
  AND CAST(settings_createdAt AS TIMESTAMP) >= '2025-06-01'
GROUP BY domainId, DATE(CAST(settings_createdAt AS TIMESTAMP))
```

### LINKS (por domínio/mês)
```sql
SELECT u.DomainId domain_id, FORMAT_TIMESTAMP('%Y-%m', p.product_sent_lists_created_at) mes,
  COUNT(DISTINCT p.product_sent_lists_id) links
FROM `vestilake_BI.sucessodocliente_products` p
JOIN `vestilake_BI.sucessodocliente_cadastrouser` u ON u.UserId = p.USERS_ID
WHERE p.product_sent_lists_created_at >= '2025-06-01' AND p.product_sent_lists_created_at < '2100-01-01'
GROUP BY 1,2
```

### CLIQUES (BQ correto; SUM não deduplica no Fabric → antigo inflado)
```sql
SELECT u.DomainId domain_id, FORMAT_TIMESTAMP('%Y-%m', r.rankings_created_at) mes,
  SUM(SAFE_CAST(r.rankings_shared_links AS INT64)) cliques
FROM `vestilake_BI.sucessodocliente_rankings` r
JOIN `vestilake_BI.sucessodocliente_cadastrouser` u ON u.UserId = r.USERS_ID
WHERE r.rankings_created_at >= '2025-06-01'
GROUP BY 1,2
```

### REATIVAÇÃO (DISTINCT id OBRIGATÓRIO — iugu_invoices é explodido)
```sql
WITH inv AS (
  SELECT DISTINCT id, customer_id, DATE(SUBSTR(paid_at,1,10)) paid_dt,
    SAFE.PARSE_DATE('%Y-%m-%d', due_date) due_dt, SUBSTR(paid_at,1,7) mes_pago
  FROM `vestilake_BI.iugu_invoices`
  WHERE status='paid' AND paid_at IS NOT NULL AND paid_at NOT IN ('None','')
    AND due_date IS NOT NULL AND due_date NOT IN ('None',''))
SELECT sc.domain_id domain_id, inv.mes_pago mes, COUNT(*) qt_reativ,
  SUM(DATE_DIFF(inv.paid_dt, inv.due_dt, DAY)) dias_atraso_total
FROM inv JOIN `vestilake_BI.silver_companiesativos_iugu` sc ON sc.Customer_ID_Iugu = inv.customer_id
WHERE inv.paid_dt IS NOT NULL AND inv.due_dt IS NOT NULL AND DATE_DIFF(inv.paid_dt, inv.due_dt, DAY) >= 1
GROUP BY 1,2
```

### PRODUTOS (❓ resolver o 2×: testar active=true; senão ingerir `products`)
```sql
-- odbc_product_details dá 2× (variantes). Provável fix: WHERE active=true. CONFIRMAR.
SELECT domain_id, FORMAT_DATETIME('%Y-%m', created_at) mes, COUNT(*) qt_produtos, MIN(created_at) primeiro_cadastro
FROM `vestilake_BI.odbc_product_details`
WHERE created_at IS NOT NULL /* AND active=true ? */
GROUP BY 1,2
```

## Views auxiliares já criadas no BQ (CREATE OR REPLACE VIEW)
- `silver_companiesativos_iugu` = custom_variables domain_id UNION fallback CNPJ (tax_document↔cpf_cnpj só-dígitos). Cobertura 50%.
- `sucessodocliente_cadastrouser` = odbc_users (id→domain_id).

## FEITO 07/07
- ✅ `fetch_elisa_bq.py` criado e rodado ponta a ponta (self-contained, sem pyodbc; build_* copiadas). + build_data.py OK → dashboard_data.js (659 marcas). Números novos vs prod:
  Empresas 656→659 ✅ | Reativação 82.370→2.312 ✅ (dedup+piso) | Links 1.272.544→983.880 ✅ (piso) | Cliques 14.371.831→5.203.666 ✅ (piso, SUM diário correto) | 1º pedido 483 domínios ✅.
- ✅ **Paridade de campos conferida**: os 6 JSONs têm EXATAMENTE os mesmos campos do painel antigo (script _fieldcheck).
- ✅ **Produtos com fallback**: enquanto `odbc_products` não existe, o fetcher PRESERVA os valores de Produtos do cadastros_elisa.json anterior (494 domínios, qtProdutos 270.842) em vez de zerar; `_garante_campos_produtos` mantém os 5 campos sempre. Troca por BQ sozinho quando a tabela for ingerida.
- ✅ **Multi-mês no painel** (pedido Laura): seletor de Período agora é dropdown de checkboxes (Todos/Limpar). `state.chaves[]` soma vários períodos; `bucket`/`mesMatch`/`reativNoPeriodo` generalizados; rótulo "N períodos (min … max)". 1 seleção = comportamento antigo. Testado (node vm: soma 5150+4005=9155 ✅, labels ✅). Arquivos: index.html, app.js, styles.css.
- ✅ `painel-elisa.yml` reescrito p/ BQ (pip google-cloud-bigquery; SA key via secret `GCP_SA_KEY`→GOOGLE_APPLICATION_CREDENTIALS; roda fetch_elisa_bq.py).

## PUSH: usar o clone limpo `C:\Users\adria\dados_clone` (o working dir Ideia-vesti está com merge conflict `UU` em vários projetos — NÃO commitar por lá). Arquivos a subir: PainelElisa/{fetch_elisa_bq.py, app.js, index.html, styles.css, dashboard_data.js, *_elisa.json} + .github/workflows/painel-elisa.yml.

## Falta p/ o cutover em produção (depende de Laura)
1. Adicionar secret **`GCP_SA_KEY`** no repo (conteúdo do JSON da SA read-only). Sem isso o workflow BQ falha.
2. Decidir: cutover agora (Produtos fica vazio até ingerir) OU esperar ingerir `odbc_products` antes de flipar.
3. Commitar `fetch_elisa_bq.py` + `painel-elisa.yml` (+ opcional: rodar workflow_dispatch p/ smoke test).
- OBS: piso jun/2025 é UNIFORME. Links/Cliques/Reativação têm dados <jun/2025 no BQ — dá p/ estender o piso só dessas 3 se quiser mais histórico.

## Depois
- Migrar CS-Sucesso-do-cliente (bem maior, semantic models Power BI DAX).
