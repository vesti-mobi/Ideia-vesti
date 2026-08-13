# Painel de Clientes — CS

Painel estático de 8 abas, grão semanal (semana ISO do ano corrente).

```
index.html          o painel (layout + lógica, sem dependência externa)
publicar.js         sobe para vesti-mobi/dados/CS via Git Data API
dados.js            dados reais gerados pelo fetcher — window.PAINEL_DATA
fetch_dados.js      carga: BigQuery + HubSpot -> dados.js
painel-clientes.html  layout de referência original (não é usado em produção)
```

No ar: **https://vesti-mobi.github.io/dados/CS/** — senha `Mudar123`.
Local: `start index.html`. Sem `dados.js` o painel cai num mock com semente fixa
e o selo no topo mostra "Dados de exemplo".

⚠️ **A senha é uma tranca, não um cofre.** `vesti-mobi/dados` é um repositório
público: a checagem roda no navegador e `.../CS/dados.js` é baixável direto, sem
senha. Serve para não abrir o painel por acaso — não protege o conteúdo.

## Atualizar os dados

```bash
node fetch_dados.js      # ~2 min
```

Precisa de:
- **BigQuery** — service account em `GOOGLE_APPLICATION_CREDENTIALS`. O fallback é
  `C:\Users\Laura\Downloads\vesti-data-499015-7ea468dae45e.json`, a mesma chave que o
  PainelElisa usa. Só roda `SELECT`.
- **HubSpot** — `HUBSPOT_TOKEN`; por padrão lê de `../CS-Sucesso-do-cliente/.env`.

Sem HubSpot o painel carrega mesmo assim: Cross-sell, Upsell e Tarefas ficam vazias.

## De onde vem cada coluna

| Aba | Campo | Fonte |
|---|---|---|
| Todas | CS responsável | `odbc_domains.angel_id` → `odbc_angels.name` |
| Tabela geral | Integração | `odbc_domains.integration_id` → `odbc_integrations.name` |
| Tabela geral | Plano | item mais caro da última fatura Iugu paga (tirando desconto/Oráculo) |
| Tabela geral | Último pedido | `MAX(MongoDB_Pedidos_Geral.settings_createdAt)` |
| Tabela geral | Pedidos / Valor / Ticket | `MongoDB_Pedidos_Geral`, pagos, por semana |
| Tabela geral | Interchange | `payment_transaction_vestiPagoValue + antifraudValue` |
| Tabela geral | Mensalidade | linhas de **plano** das faturas Iugu pagas, casadas por CNPJ |
| Tabela geral | Outros (Iugu) | demais linhas da fatura: Oráculo, Filial, Assistente, Ativação |
| Tabela geral | Antecipação | `payment_transaction_antecipationValue` × fator Vesti |
| Tabela geral | Vencimento | próxima fatura Iugu `pending` com vencimento futuro |
| Cross-sell / Upsell | tudo | HubSpot, pipeline **Expand (Upgrades)** |
| Oráculo | atendimentos / % IA | `oraculo_Atendimentos` (`source` IA/HUMAN) |
| Oráculo | GMV | `oraculo_Pedidos.Tipo_Venda_Oraculo` |
| Tino | cliques e último acesso | `sucessodocliente_rankings` (`rankings_shared_links`, `rankings_created_at`) |
| Vesti Pago | valor / fee / antecipação | `MongoDB_Pedidos_Geral` com provider Vesti Pago |
| Vesti Pago | links | pedidos com `settings_source = 'Link de cobrança'` |
| Churn | data | derivado do Iugu (ver abaixo) |
| Tarefas | tudo | HubSpot `tasks` |

## Ressalvas que mudam a leitura do número

Estão também dentro do painel: clique no selo do topo direito.

**Já validados contra fonte independente (13/08):** interchange e antecipação foram
cruzados pedido a pedido com `vestipago_transaction_detail` — 28.559 pedidos, R$ 910.311
contra R$ 909.346 de fee, 0,97% de divergência (estornos). A base está correta.

1. **Último acesso na plataforma não tem fonte.** Não existe coluna de login/sessão
   de lojista em nenhuma tabela do `vestilake_BI` — procurei por `login`, `acess`,
   `last_*`, `signin`, `session`, `visit`, `seen`. `odbc_users.updated_at` não serve
   (muda a cada edição de cadastro e está parado desde 25/06). A coluna existe no
   layout e mostra `—` até alguém expor esse campo no espelho; o KPI de abandono usa
   **último pedido** no lugar. Já o **último acesso no Tino** existe e está na aba do
   Tino: é a data mais recente em que a marca compartilhou link.

2. **O fee da Vesti só existe para cartão.** Em PIX o campo vem nulo tanto em
   `MongoDB_Pedidos_Geral` quanto em `vestipago_transaction_detail`, e PIX é ~53%
   das transações Vesti Pago (108.950 transações / R$ 90,2M em 2026). Então
   "Interchange" e "Receita (fee)" cobrem só o cartão; **valor transacionado cobre
   os dois** e está quebrado em Cartão e PIX na aba.

3. **Mensalidade é só o plano.** A fatura do Iugu junta plano, Oráculo, Filial,
   Assistente e taxa de ativação no mesmo `total_cents`, e só **75,2%** é plano.
   Por isso a soma é feita linha a linha do item: o plano vai para "Mensalidade"
   (R$ 2,75M em 2026) e o resto para "Outros (Iugu)" (R$ 793k). Os dois entram na
   Receita total. Se a régua do time for "mensalidade = tudo que a marca paga por
   mês", é só somar as duas colunas.

4. **Antecipação é estimada.** `payment_transaction_antecipationValue` é o que o
   lojista pagou de antecipação, não o que a Vesti ganhou. O fetcher mede a fração
   da Vesti em `vestipago_transaction_detail`
   (`antecipationVestiFee / antecipationValue` = **18,83%**) e aplica sobre o valor
   cobrado. O fator é recalculado a cada carga e fica em `meta.fatorAntecipacaoVesti`.

5. **Links do Vesti Pago = só `'Link de cobrança'`.** Conferido no dado: os pedidos
   dessa origem que aparecem sem provider são exatamente os que ninguém pagou
   (8.562 pedidos, 0 pagos), ou seja, o link é do Vesti Pago de ponta a ponta.
   `'Link sem preço'` ficou de fora porque tem 4.226 pedidos **pagos por fora**
   (R$ 8,8M), e `'Link'` puro (573k pedidos) é o link de compartilhamento do
   vendedor, não de cobrança.

6. **Pedido com provider nulo não é Vesti Pago.** 162k pedidos pagos / R$ 352M de GMV
   em 2026 são vendas registradas na plataforma mas pagas por fora. Entram no GMV da
   tabela geral e ficam fora da aba Vesti Pago. É por isso que marcas grandes como
   Egoiste e JDL aparecem com receita R$ 0 — está certo, elas não usam o produto.

7. **Churn é derivado, não é um campo.** Regra: sem fatura Iugu paga há mais de 45
   dias **e** sem fatura futura em aberto; a data do churn é a última fatura paga +
   45 dias. Dá 48 cancelamentos em 2026. Quando as `iugu_subscriptions`
   estabilizarem, trocar por cancelamento de assinatura é mais direto.

8. **Cross-sell × upsell sai do nome do negócio.** A API não liberou escopo de
   `line_items` (403), então a classificação usa os padrões em `REGRAS_PRODUTO`.
   Regra definida na revisão de 13/08: **upsell = só upgrade de plano**; todo o
   resto (Filial, Multiloja, Oráculo, Integração, Assistente, Vesti Pago, Tino) é
   cross-sell. Exceções em `EXCECOES`: Kelly Rodrigues Store Fortaleza = Filial,
   Jay & Co e Landê Oficial = Upgrade. Fechado é só o estágio "Ganho (Expand)";
   "Em aberto" não conta como perdido.

9. **CS.** Marcas de anjos que saíram da carteira (`ANJOS_FORA`: Shirley Silva,
   Priscila Argolo) aparecem como "Sem CS". A aba Tarefas mostra só o time em
   `CS_TAREFAS` — Luana, Thamiris, Cristiane, Elisa, Gabriella, Alexia e Tatiane.
   O filtro de CS de cada aba é montado a partir das linhas dela, então nunca
   oferece um nome que não devolve nada.

10. **Teto de R$ 50.000 por pedido**, o mesmo filtro do CS-Sucesso e do PainelElisa,
   para os números baterem entre os painéis.

11. **Só 2026.** Todas as séries são semanas 1..atual do ano corrente, porque o painel
    inteiro é "semana do ano". Negócios e tarefas de anos anteriores ficam de fora
    (o pipeline Expand existe desde 2021; 101 dos 1.108 negócios têm data em 2026).

## Como o painel se comporta

- **Semanas**: dá para escolher semanas soltas, de meses diferentes. Cada semana é
  rotulada pela posição dentro do mês (1ª, 2ª...); clicar no nome do mês marca ou
  desmarca o mês inteiro; os atalhos põem as últimas 4/12/26 semanas ou o ano todo.
  O recorte vale de verdade em todas as abas — a tabela geral e as três abas de
  produto são somadas a partir das séries semanais, não de um total anual pronto.
- **Filtro de CS** existe na tabela geral, Oráculo, Tino, Vesti Pago e Tarefas, e
  recorta tabela *e* gráfico.
- Os gráficos obedecem só a cliente + CS. Filtros de linha (status do negócio,
  situação do churn) não mexem no histórico, de propósito.
- Exportar CSV exporta exatamente as linhas e colunas visíveis.
