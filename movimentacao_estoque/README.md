# Movimentação de Estoque — relatório

Dashboard que agrega a tabela **`dbo.movimentacao_estoque`** do **VestiHouse (Fabric)**
e publica via GitHub Pages.

A tabela é um log de movimentação de estoque com ~1,5M linhas/dia (snapshot do dia
mais recente), então **toda a agregação é feita no servidor (SQL `GROUP BY`)** —
nada de linha crua no `dados.js`.

## Arquivos
- `build_data.js` — fetcher Node + `tedious`: autentica no Fabric (refresh token →
  token `database.windows.net`), roda as queries de agregação e gera `dados.js`.
  Mantém `historico.json` (1 snapshot por dia) para a série temporal.
- `index.html` — dashboard (KPIs, por ação, por origem, histórico, top 50 empresas).
- `dados.js` / `historico.json` — gerados pelo fetcher (commitados pelo workflow).
- O workflow fica na **raiz** do repo: `.github/workflows/movimentacao-estoque.yml`.

## O que o relatório mostra
- **🔎 Busca** por domínio, empresa, produto, SKU e data (client-side, instantânea).
  Carrega `busca.js` sob demanda (~41k movimentações "de gente": reserva/venda/
  separação/app). As sincronizações de integração em massa (`INTEGRATION_BULK_*`,
  95% do volume) **não** entram na busca linha a linha — só nos totais.
- **KPIs:** movimentações, empresas, domínios, SKUs, produtos, pedidos.
- **Por ação:** INSERT / UPDATE / DELETE.
- **Por origem:** INTEGRATION_BULK_*, RESERVE_*, STOCK_SEPARATION_*, APP_*, etc.
- **Histórico diário:** acumulado a cada execução (a tabela só guarda o dia atual).
- **Top 50 empresas:** nome (via JOIN `dbo.ODBC_Companies`), total, split por ação,
  SKUs e saldo líquido (`Σ new_balance − old_balance`).

> Busca ao vivo (consultar as 1,5M linhas em tempo real, incl. as bulk) exigiria
> um backend que alcance o Fabric SQL — o Vercel (AWS) não alcança a porta 1433
> do Fabric (cross-cloud); só hosts no Azure / GitHub Actions / rede local. Por
> isso a busca é estática sobre o subconjunto não-bulk.

## Execução
- **Automática:** workflow diário às **11:00 UTC (08:00 BRT)** + `workflow_dispatch`.
- **Manual local:**
  ```bash
  # da raiz do repo (tedious vem do node_modules da raiz)
  node movimentacao_estoque/build_data.js
  ```
  Usa `FABRIC_REFRESH_TOKEN/FABRIC_TENANT_ID/FABRIC_CLIENT_ID` do ambiente, com
  fallback para o `.env` da pasta ou de `../CS-Sucesso-do-cliente/.env`.

## Secrets necessários (já existem no repo, usados pelos outros painéis)
`FABRIC_REFRESH_TOKEN`, `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`.
> O refresh token (cliente Azure CLI) dura ~90 dias. Se expirar, renove com
> `node CS-Sucesso-do-cliente/get-fabric-token.js` e atualize o secret.

## Publicação
GitHub Pages serve em `https://vesti-mobi.github.io/dados/movimentacao_estoque/`.
