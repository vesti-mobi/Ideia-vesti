# Painel CS — Elisa & Jennyfer

Dashboard estático (HTML + JSON) seguindo o padrão do PainelCSGerencial.
Filtra apenas marcas dos CS **Elisa** e **Jennyfer** com `modulos LIKE '%vendas%'`.

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
| Reativações | — | ❌ pendente |
| Link compartilhado / Clicks | — | ❌ pendente |

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
