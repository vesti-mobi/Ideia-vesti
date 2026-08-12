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
- **Reativação = 45+ dias sem pagar.** Mede o intervalo entre uma fatura paga e
  a seguinte (`LAG` sobre `iugu_invoices` status=paid). *Não* é mais
  `paid_at > due_date`: naquele critério 70% era atraso de 1 a 3 dias e o maior
  do banco todo era 30 dias, porque a Iugu cancela a fatura antes disso — então
  inadimplência longa nunca aparecia. Deu 262 retornos de 198 marcas contra
  5.939 "reativações" do critério antigo.
- **Mensalidade**: `valor_plano` (price_cents da assinatura Iugu, o valor
  contratado) é o número certo, mas só existe para ~66 marcas. `valor_mensal`
  (última fatura paga, pode ser proporcional) é o fallback.

## Pipeline

```
fetch_elisa.py     →  companies_elisa.json, gmv_elisa.json,
                      cadastros_elisa.json, vestipago_elisa.json
build_data.py      →  dashboard_data.js  (consumido por index.html)
```

### Rodar

```bash
py fetch_elisa.py    # precisa az CLI logado (az login) ou FABRIC_REFRESH_TOKEN
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
| Reativações | iugu_invoices: fatura paga após o vencimento | ✅ |
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
