# Métricas Vesti Pago

Painel das métricas financeiras do VestiPago (cartão de crédito), no ar em
GitHub Pages, **+** o pipeline que ingere as transações da **API Vesti** para o Fabric.

> Projeto independente — **não** tem relação com movimentação de estoque.

## Painel (dashboard)
- `index.html` — dashboard estático (login `Mudar123`). 5 abas, com **toggle Semana/Mês**:
  - **Completo** — KPIs + detalhamento financeiro por período (value → cliente pagou,
    netValue → marca recebe, MDR banco, antecipação) com os **ganhos da Vesti destacados**
    (★ antifraudValue, ★ mdrVestiValue, ★ antecipationVestiFee).
  - **Method** — por método de pagamento e por bandeira do cartão.
  - **BDR / Antecipação** — antecipação por provedor (BDR/Starkbank/…) + ledger `dbo.bdr`.
  - **Métodos de pagamento** — marcas por método habilitado (`ODBC_Company_Method_Payments`).
  - **Duplicata de Method** — pivot marca × período do valor transacionado.
- `build_data.js` (Node + tedious) — agrega **server-side** (por Semana ISO e por Mês) e
  gera `dados.js`. Financeiro de cartão vem da `dbo.vestipago_transaction_detail`
  (alimentada pela API); antecipação/BDR e métodos vêm do datalake (VestiHouse/Fabric).
- Workflow `.github/workflows/metricas-vestipago.yml` — diário, secrets `FABRIC_*`.

Reconstrução do PBIX "Metricas VestiPago" (abas Method/BDR/Completo juntando semanal+mensal
num filtro único), com a API de detalhamento incluída e visual reformulado.

## Pipeline de ingestão (Fabric)
Ingestão das transações de pagamento da **API Vesti** para a tabela `dbo.vestipago_transaction_detail`.

## Fonte
- Endpoint: `GET https://apivesti.vesti.mobi/payment/v1/transaction-detail/orders`
- Auth: `Authorization: Bearer <token>`
- Paginação (mongoose-paginate): `?page=N&limit=M`; resposta traz `data`, `totalDocs`,
  `totalPages`, `page`, `hasNextPage`, `nextPage`.
- **Sem filtro de data** — a API devolve a janela que ela decide (hoje ~maio→atual).
- Cada registro tem `_id` (Mongo) estável → usado como chave de upsert.

## Campos
`_id, domainId, companyId, orderId, source, transactionId, method, paidAt,
value, netValue, antifraudValue, vestiPagoValue, vestiPagoProvider, installments,
cardBrand, vestiValue, providerValue, mdrCardBrandValue, mdrVestiValue,
antecipationValue, antecipationProvider, antecipationProviderFee,
antecipationVestiFee, createdAt, updatedAt`

## Notebook
`fabric_notebook_vestipago.py` — cole cada bloco `# CELL N` como uma célula numa
Notebook do Fabric (PySpark), com um Lakehouse anexado.

1. Token no **Key Vault** (secret `vesti-api-token`); ajuste `KV_URL` na Célula 1.
2. **Add Lakehouse** na notebook (onde a tabela `vestipago_transaction_detail` vai morar).
3. *Run all* — busca todas as páginas e faz **MERGE por `_id`** (idempotente: não duplica,
   atualiza linhas com `updatedAt` novo, insere as novas). Partição por `paid_month`.
4. Agendar diário (Schedule da notebook ou Data Factory Pipeline + trigger).

Se a API retornar **401**, a Célula 2 renova o token via login (confirmar payload/endpoint
de login com a equipe Vesti).
