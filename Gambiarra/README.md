# Painel CS — todas as CS

Dashboard estático (HTML + JSON) seguindo o padrão do PainelCSGerencial.
Traz **todas as marcas** com `modulos LIKE '%vendas%'` (antes era restrito a
Elisa e Jennyfer). O filtro de CS é montado dinamicamente a partir dos dados.

## Regras que não são óbvias

- **1 linha por domínio.** `odbc_companies` traz 1 linha por CNPJ, então um
  domínio com filiais gerava N linhas com o *mesmo* GMV (Diamantes Lingerie
  aparecia 79x). `build_data.py` mantém só a matriz e agrega as filiais em
  `qtdFiliais`/`filiais`.
- **Primeiras 5 / 25 vendas**: primeiro mês-calendário em que a marca fez N
  vendas pagas *dentro do próprio mês*. A contagem zera na virada do mês
  enquanto não bater N; ao bater, grava a data exata e não zera mais.
- **Piso de dados = 1º mês de `mesesList`** (hoje jul/2025). Para marcas que
  entraram antes disso o marco das 5/25 não é "primeiras vendas" — é só "um mês
  em que fez N". O painel filtra essa coorte por padrão (checkbox nas abas).
- **Canal Vesti** = `partner_name` em {Vesti, Varejo Vesti}. Não entra no
  ranking de Parceiros.
- **Ativação/reativação = 4 origens** (13/08/2026). A aba lista a marca quando o
  ambiente é ativado ou volta a funcionar. Nenhum sinal sozinho cobria a lista das
  vendedoras, então são quatro regras complementares (`_eventos_ambiente` em
  `build_data.py`):

  | origem | o que é | pega o caso de |
  |---|---|---|
  | `criacao` | domínio criado (`odbc_domains.created_at`) | cliente que volta com **cadastro novo** (Surf Center abriu o domínio 2139982) |
  | `pagamento` | 45+ dias sem pagar e voltou (`LAG` sobre `iugu_invoices`) | inadimplência longa que retornou |
  | `retorno` | 1ª fatura paga >365d após a entrada | marca antiga que só começou a ser cobrada agora (Lunar Fitwear: 1.273 dias) |
  | `religamento` | `Ligado?=Sim` na planilha do n8n **sem fatura casada** | religamento pelo form manual (Surf Center antigo, 29/07/2026) |

  A origem `pagamento` substituiu `paid_at > due_date`: naquele critério 70% era
  atraso de 1 a 3 dias e o maior do banco todo era 30 dias, porque a Iugu cancela
  a fatura antes disso — inadimplência longa nunca aparecia.

  ⚠️ **Não usar a coluna `Ligado?=Sim` da planilha como religamento sem o filtro.**
  O ramo de desbloqueio do n8n dispara no webhook de *fatura paga* e chama `unblock`
  para todo mundo, bloqueada ou não: 710 dos 807 "Sim" (88%) têm a data exatamente
  igual à da última fatura paga. Cru, isso dava 530 "reativações" só em jul/2026;
  com o filtro sobram 72 religamentos reais.

  ⚠️ A guarda `GUARDA_PISO_DIAS` na origem `retorno` não é opcional: o espelho
  `iugu_invoices` só começa em 2025-01-01, então sem ela **toda** marca anterior a
  2025 apareceria como "1ª fatura tardia" (494 falsos positivos).

  Fora do alcance do dado: marca que pagou em dia e nunca foi desligada, mas que a
  vendedora classifica como reativação (caso Gabifit, maior atraso 40 dias). Isso é
  classificação comercial — só entraria ingerindo a planilha das vendedoras.

- **Log de ambiente**: planilha "Domínios Bloqueados Automação" (dona
  `diego@vesti.mobi`), alimentada pelo workflow n8n `8arVGfRb408xr4xH`. Ela guarda
  o **estado atual**, não histórico (append-or-update por domínio: 1.065 linhas,
  1.065 domínios distintos) — logo cada marca contribui com no máximo 1
  religamento, e religamentos antigos não são recuperáveis dessa fonte.
  A SA do pipeline (`829232163598-compute@developer.gserviceaccount.com`) precisa
  de **Leitor** na planilha; sem isso `fetch_ambiente.py` cai no snapshot
  `ambiente_bloqueios.csv` versionado no repo, que congela no dia da exportação.
- **Mensalidade**: `valor_plano` (price_cents da assinatura Iugu, o valor
  contratado) é o número certo, mas só existe para ~66 marcas. `valor_mensal`
  (última fatura paga, pode ser proporcional) é o fallback.

## Pipeline

```
fetch_elisa_bq.py  →  companies_elisa.json, gmv_elisa.json, cadastros_elisa.json,
                      vestipago_elisa.json, reativacao_elisa.json, links_elisa.json,
                      pagamentos_elisa.json
fetch_ambiente.py  →  ambiente_elisa.json   (lê a planilha do n8n; precisa de
                      pagamentos_elisa.json para separar religamento de "pagou")
build_data.py      →  dashboard_data.js     (consumido por index.html)
```

A ordem importa: `fetch_ambiente.py` depende de `pagamentos_elisa.json`, e
`build_data.py` depende dos dois.

### Rodar

```bash
py fetch_elisa_bq.py   # GOOGLE_APPLICATION_CREDENTIALS = SA key do BigQuery
py fetch_ambiente.py
py build_data.py
start index.html
```

`fabric_config.json` aponta para o mesmo Lakehouse VestiHouse do PainelCSGerencial.
`_fetch_fabric_base.py` é cópia do `fetch_fabric.py` (auth via az/refresh token).

## KPIs implementados

| KPI | Fonte | Status |
|---|---|---|
| GMV total (mês/semana) | MongoDB_Pedidos_Geral | ✅ |
| VestiPago Cartão / PIX | mesmo, split por payment_method | ✅ |
| Cadastro de produtos | ODBC_Products | ✅ |
| Primeiras 5 / 25 vendas | acumulado mensal de pedidos | ✅ |
| Sem VP ativo / Sem frete | proxy: pedidos nos últimos 30-60d | ⚠ proxy |
| Marcas travadas | 1º pedido cadastrado sem 1ª venda | ✅ |
| TOP 10 Starter / Parceiros | bucket atual por GMV | ✅ |
| Ativações e reativações | 4 origens: domínio criado, gap de 45+ dias, 1ª fatura tardia, religamento no n8n | ✅ |
| Link compartilhado / Clicks | sucessodocliente_products / _rankings | ✅ |
| Oportunidades de Upgrade | GMV dos 3 últimos meses fechados ≥ R$ 300k | ✅ |

## Canal

- **Starter (Interno)**: partner_name ∈ {Starter, Ve Vantagens, ProRoi, Up, Comfio, Glads, Tizzefy, Sete, Zoom, Renan}
- **Parceiros**: demais, exceto Atta/Onix nos rankings

## Alertas (linha vermelha na tabela)

- Cadastrou pedidos há >30d e ainda não vendeu
- <5 vendas após 60d da 1ª venda
- VP inativo / Frete inativo
- Nenhum produto cadastrado

## Pendências para confirmar com Laura

1. **Reativação** — definição: fatura Iugu paga após N dias inadimplente? Pedido após X dias sem pedido?
2. **VP ativo / Frete ativo** — hoje uso proxy (pedidos nos últimos 30-60d). Tem tabela em `mongodb_companies_logistics` ou `ODBC_Company_Method_Payments` que dá flag direta?
3. **Link compartilhado / Clicks** — `PedidosUTM_InserirClickdosprodutos`? Confirmar agregação por domínio.
4. **Cadastro de Produtos** — usar `ODBC_Products` com filtro de status/ativo?
