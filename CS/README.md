# Painel de Clientes — CS

Painel estático de 9 abas, grão semanal (semana ISO do ano corrente).

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
- **Tino** — `TINO_USER` / `TINO_PASS`, a mesma credencial do painel
  `admin.tino.vesti.com.br`. **Sem valor padrão no código de propósito**: o
  `vesti-mobi/dados` é público.

Sem HubSpot o painel carrega mesmo assim: Cross-sell, Upsell, Reuniões e Tickets
ficam vazias. Sem a credencial do Tino, a aba do Tino e a coluna "Tino" da
tabela geral ficam vazias — o resto carrega igual.

### Automático, todo dia às 04:00 BRT

O workflow `.github/workflows/painel-clientes-cs.yml` no `vesti-mobi/dados` roda a
mesma carga (`node CS/fetch_dados.js`) às 07:00 UTC e commita o `CS/dados.js`.
Como o painel inteiro — todas as abas, tabelas e gráficos — lê desse único
arquivo, uma carga atualiza tudo. Usa os secrets `GCP_SA_KEY` e `HUBSPOT_TOKEN`,
que já existem no repositório, e aborta sem commitar se o `dados.js` sair com
menos de 1 MB (sinal de que alguma fonte falhou). Dá para rodar na mão pela aba
Actions ("Painel de Clientes CS" → Run workflow). A cópia local do arquivo é o
`atualizar-painel.yml` aqui na pasta.

⚠️ O `index.html` e o `README.md` **não** são publicados pelo workflow — mudança
de layout continua indo por `node publicar.js`.

## De onde vem cada coluna

| Aba | Campo | Fonte |
|---|---|---|
| Todas | CS responsável | `odbc_domains.angel_id` → `odbc_angels.name` |
| Tabela geral | Integração | `odbc_domains.integration_id` → `odbc_integrations.name` |
| Tabela geral | Data de cadastro | `odbc_domains.created_at` — entrada da marca na Vesti |
| Todas | Canal | `odbc_domains.partner_id` → `odbc_partners.name` |
| Tabela geral | Plano | item mais caro da última fatura Iugu paga (tirando desconto/Oráculo) |
| Tabela geral | Último pedido | `MAX(MongoDB_Pedidos_Geral.settings_createdAt)` |
| Tabela geral | Pedidos / Valor / Ticket | `MongoDB_Pedidos_Geral`, pagos, por semana |
| Tabela geral | Interchange | `vestipago_transaction_detail`: `mdrVestiValue + antifraudValue` — fee **já sem a taxa do banco** |
| Tabela geral | Mensalidade | linhas de **plano** das faturas Iugu pagas, casadas por CNPJ |
| Tabela geral | Outros (Iugu) | demais linhas da fatura: Oráculo, Filial, Assistente, Ativação |
| Tabela geral | Antecipação | `payment_transaction_antecipationValue` × fator Vesti |
| Tabela geral | Vencimento | próxima fatura Iugu `pending` com vencimento futuro |
| Cross-sell / Upsell | tudo | HubSpot, pipeline **Expand (Upgrades)** |
| Oráculo | atendimentos / % IA | `oraculo_Atendimentos` (`source` IA/HUMAN) |
| Oráculo | GMV | `oraculo_Pedidos.Tipo_Venda_Oraculo` |
| Tabela geral | Tino / Oráculo | Tino: marca presente na base do produto (API do Tino). Oráculo: tem atendimento em `oraculo_Atendimentos` |
| Tino | marcas, eventos, sessões, dias de acesso | **API do Tino** — `companies_chart`, `login_days`, `customer_kpis` |
| Vesti Pago | valor / fee / antecipação | `MongoDB_Pedidos_Geral` com provider Vesti Pago |
| Vesti Pago | links | pedidos com `settings_source = 'Link de cobrança'` |
| Churn | data | derivado do Iugu (ver abaixo) |
| Reuniões | reunião / data / responsável | HubSpot `meetings` (`hs_meeting_start_time` + owner) |
| Reuniões | Cliente | empresa associada à reunião no HubSpot |
| Reuniões | Resultado | negócio ganho (qualquer pipeline) creditado à última reunião daquela empresa antes do fechamento |
| Tickets | ticket / pipeline / estágio | HubSpot `tickets`, todos os pipelines |
| Tickets | Situação | estágio marcado como fechado (`metadata.ticketState`) ou `closed_date` preenchida |
| Tickets | Cliente e Canal | empresa associada ao ticket → marca do cadastro (nome, nome sem ruído ou CNPJ) |

## Ressalvas que mudam a leitura do número

Estão também dentro do painel: clique no selo do topo direito.

**Já validados contra fonte independente (13/08):** interchange e antecipação foram
cruzados pedido a pedido com `vestipago_transaction_detail` — 28.559 pedidos, R$ 910.311
contra R$ 909.346 de fee, 0,97% de divergência (estornos). A base está correta.

**Conferência da Alcance Loja Fábrica, julho/2026 (14/08).** Suspeita de que a
receita estivesse "puxando PIX". Não está — **PIX não entra em nenhuma linha de
receita**, porque `payment_transaction_vestiPagoValue` vem nulo em PIX (ressalva
2). Os R$ 44,6k de julho (semanas 27–31) se decompõem assim:

| Origem | Valor | De onde |
|---|---|---|
| Interchange | R$ 31.562 | cartão STARKBANK: R$ 22.211 de fee + R$ 9.352 de antifraude |
| Antecipação | R$ 11.405 | R$ 60.571 cobrados do lojista × 18,85% |
| Mensalidade + Outros | R$ 1.619 | fatura Iugu da semana 29 |
| **PIX** | **R$ 0** | R$ 41.849 transacionados, fee nulo na fonte |

O GMV da marca no mesmo mês é R$ 5,1M, mas R$ 3,95M disso são pedidos **pagos
por fora** (provider nulo, ressalva 6): aparecem em "Valor dos pedidos" e não
geram receita nenhuma. Se o número parecer alto, o candidato é a **antecipação**
(26% dos 44k) — é a única parcela estimada, por um fator médio da carteira e não
por medição pedido a pedido (ressalva 4).

0. **Data de cadastro = criação do domínio** (`odbc_domains.created_at`), ao lado
   de "Último acesso" na tabela geral, com o tempo de casa embaixo ("7a 4m").
   Está preenchida em 100% das 2.372 marcas, de 23/08/2016 até hoje, e nenhuma
   empresa em `odbc_companies` foi criada antes do domínio dela — então o
   domínio é mesmo a porta de entrada. Das 964 marcas com atividade em 2026, a
   safra maior é 2025 (170) e 2020 (139).

1. **Último acesso na plataforma não tem fonte.** Não existe coluna de login/sessão
   de lojista em nenhuma tabela do `vestilake_BI` — procurei por `login`, `acess`,
   `last_*`, `signin`, `session`, `visit`, `seen`. `odbc_users.updated_at` não serve
   (muda a cada edição de cadastro e está parado desde 25/06). A coluna existe no
   layout e mostra `—` até alguém expor esse campo no espelho; o KPI de abandono usa
   **último pedido** no lugar. Já o **último acesso no Tino** existe e está na aba do
   Tino: é o `last_login` da API do produto.

2. **O fee da Vesti só existe para cartão.** Em PIX o campo vem nulo tanto em
   `MongoDB_Pedidos_Geral` quanto em `vestipago_transaction_detail`, e PIX é ~53%
   das transações Vesti Pago (108.950 transações / R$ 90,2M em 2026). Então
   "Interchange" e "Receita (fee)" cobrem só o cartão; **valor transacionado cobre
   os dois** e está quebrado em Cartão e PIX na aba.

2b. **A fatura do Iugu acha a marca por CNPJ e, se falhar, pelo nome (18/08).**
   A fatura não traz domínio, então o casamento é por CNPJ — e em 2026 **925
   faturas pagas (R$ 564k, 13% do faturamento Iugu) estão num CNPJ que não existe
   no cadastro**. Foi o caso da Sawary: paga R$ 4.078/mês no CNPJ
   00.422.351/0001-90 e o cadastro dela tem 82.364.623/0001-08 — a receita sumia
   da linha dela (e o CNPJ do Iugu ainda aparece no cadastro de um terceiro,
   "Mary Elias"). Agora, quando o CNPJ não acha ninguém, a fatura procura pelo
   **nome do pagador**: o Iugu grava "Marca = RAZÃO SOCIAL LTDA", então tanto o
   apelido quanto a razão servem. Casamento **exato** depois de normalizar e só
   quando aponta para UMA marca. Isso resgatou 336 linhas (~R$ 154k). O mesmo
   resgate vale para **plano, vencimento e churn**, que saem da mesma fatura —
   sem ele, marca que paga em dia podia ser marcada como cancelada.
   Sobram R$ 410k sem dono, e a divisão é: **R$ 146k de contas que existem mas
   estão como "só compras"** (mesmo caso de [[DOMINIOS_EXTRA]], resolvível
   incluindo a marca) e **R$ 282k cujo nome não bate com domínio nenhum** — esses
   precisam de correção de cadastro, não de código.

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
   oferece um nome que não devolve nada. Cross-sell, Upsell e Churn passaram a
   mostrar a coluna **CS** junto com o filtro.

9b. **Interchange agora é líquido do banco (18/08).** O fee de cartão que o
   lojista paga (`vestiPagoValue`) não é receita inteira da Vesti: ele se divide
   em `mdrCardBrandValue`, que vai para o adquirente (Iugu, Starkbank, Pagarme),
   e `mdrVestiValue`, que fica com a Vesti. Em 2026 são R$ 953k de fee, dos quais
   **R$ 840k (88%) são do banco** — a coluna Interchange passou a ser
   `mdrVestiValue + antifraudValue` = **R$ 450k**, e a Receita total caiu junto.
   O antifraude continua inteiro: é cobrança da Vesti, não taxa de banco. Por
   isso o interchange saiu de `MongoDB_Pedidos_Geral` e passou a ser medido em
   `vestipago_transaction_detail`, a única tabela com a quebra do MDR (a
   diferença entre as duas bases é ~1%, já conferida em 13/08). A antecipação já
   era líquida, pelo fator `antecipationVestiFee/antecipationValue`.

9c. **O Tino não vem mais do BigQuery (18/08).** `sucessodocliente_rankings` só
   tinha os links compartilhados ("cliques") e não sabe quem *tem* o produto. A
   aba passou a ler a **API do Tino** (a mesma de `admin.tino.vesti.com.br`):
   `customer_kpis` dá o total de marcas — **92**, o número que o time usa —,
   `login_days` dá último acesso, dias de acesso e situação, e
   `companies_chart` dá **eventos** e sessões, uma chamada por semana. Evento é
   o que o Tino registra: login, troca de aba, filtro, abertura de produto,
   busca, export (23,8k eventos no ano). O casamento com o cadastro é por nome
   (a API só devolve o slug): **90 das 92** casaram; Lete Moda e Praia e Santho
   Pano ficam na tabela com "sem marca no cadastro".

9d. **Duas marcas entram no painel fora da regra de módulos (18/08).**
   A carteira é "domínio com módulo de vendas", filtro que impede o painel de
   encher de conta de lojista comprador. **Lete Moda Praia** (Summer House, CS
   Jennyfer) e **Santho Pano** (Donna Sami, CS Luana) estão no cadastro como
   *só compras*, mas têm CS, canal, pedidos e usam o Tino — foi por isso que a
   aba do Tino as mostrou como "sem marca no cadastro". Entram pela lista
   `DOMINIOS_EXTRA` no `fetch_dados.js`; se aparecer outra assim, é só
   acrescentar o domínio lá.

9e. **"Nunca acessaram" tem três números possíveis — o painel usa o do Tino.**
   No admin do Tino o card diz **12** e a tabela logo abaixo diz **16**; a
   primeira versão desta aba dizia **19**. Os três medem coisas diferentes:
   **12** = marcas sem nenhuma atividade registrada (é `total_brands` menos
   `company_list`, a régua do card); **16** = marcas com `login_days = 0`;
   **19** era 16 + 3 marcas que não têm linha em `login_days` — essas três
   (andressa_vesti, per_pochi, refugio_modas) TÊM atividade, então contá-las como
   "nunca acessou" estava errado. A diferença de 12 para 16 são quatro marcas
   (delia_modas, miss_manu, daline, stefani) que entram pelo **SSO da Vesti**: o
   evento é `sso_login_vesti` e não conta em `login_days`. Elas aparecem na
   coluna Situação como "entra sem login (SSO)".

9f. **A série do Tino não pode sair de `companies_chart` (18/08).** Essa rota
   devolve no máximo **20 linhas** — é o top 20 da tela do Tino, e o corte vale
   mesmo passando a lista de empresas no corpo (25 slugs explícitos voltam 20).
   Como a série era montada com uma chamada por semana, as semanas cheias (S29 em
   diante) vinham truncadas exatamente onde a CS mais olha: 205 pares
   semana-marca contra **311** reais. Agora é uma chamada de `timeline`
   (granularity=week) e uma de `metrics` por marca, 4 em paralelo.

10. **Teto de R$ 50.000 por pedido**, o mesmo filtro do CS-Sucesso e do PainelElisa,
   para os números baterem entre os painéis.

11. **Reuniões no lugar de Tarefas (18/08).** A aba de Tarefas saiu e entrou
    **Reuniões**, com a mesma leitura do painel
    [PlanilhasEPainelCS](https://vesti-mobi.github.io/dados/PlanilhasEPainelCS/):
    reunião realizada de um lado, negócio fechado do outro. A fonte é o objeto
    `meetings` do HubSpot (289 no ano, do time em `CS_TIME`), e o cliente vem da
    empresa associada à reunião. O **negócio ganho é creditado à última reunião
    daquela empresa antes do fechamento** — sem isso, uma empresa com cinco
    reuniões e um negócio viraria cinco negócios. Negócio ganho de qualquer
    pipeline conta (o time fecha filial, upgrade e Vesti Pago em pipelines
    diferentes); dos 370 ganhos em 2026, 59 caíram em alguma reunião — o resto
    fechou sem reunião registrada. Reunião com data futura aparece como
    **Agendada** quando a semana atual está na seleção.

12. **Canal = parceiro dono da conta.** `odbc_domains.partner_id` →
    `odbc_partners.name`: Vesti, Attasoft, Uemtel, Trial, Starter, Varejo Vesti,
    Treino, Onix, Glads, ProRoi, Tizeefy, Up Agency, Ve Vantagens. Quem está sem
    parceiro ou com `"N/A"` aparece como **Sem canal**. O filtro aceita mais de um
    canal ao mesmo tempo e nenhum marcado quer dizer todos. Atenção: `odbc_partners`
    vem com cada linha **duplicada** no espelho — o join agrupa antes, senão o
    cadastro dobraria de tamanho.

13. **Tickets: o cliente vem da empresa associada, não do assunto.** O ticket é
    ligado à marca do cadastro em três tentativas — nome igual, nome sem ruído
    (`chaveMarca()` tira "Ltda", "Modas", pontuação) e, por último, CNPJ
    (propriedades `cnpj` e `hs_tax_id` da empresa no HubSpot). Numa amostra de 121
    empresas isso levou o casamento de 54% para 76%. Ticket que não casa continua
    na lista, só fica **Sem canal** — sumir com ticket por causa de cadastro seria
    pior que mostrá-lo sem canal. A busca do HubSpot só pagina até 10.000
    resultados por consulta, então a carga de tickets é quebrada **mês a mês**
    (`buscarPorMes`); sem isso o resto sumiria em silêncio, sem erro.

14. **Só 2026.** Todas as séries são semanas 1..atual do ano corrente, porque o painel
    inteiro é "semana do ano". Negócios e tarefas de anos anteriores ficam de fora
    (o pipeline Expand existe desde 2021; 101 dos 1.108 negócios têm data em 2026).

## Como o painel se comporta

- **Semanas**: dá para escolher semanas soltas, de meses diferentes. Cada semana é
  rotulada pela posição dentro do mês (1ª, 2ª...); clicar no nome do mês marca ou
  desmarca o mês inteiro; os atalhos põem as últimas 4/12/26 semanas ou o ano todo.
  O recorte vale de verdade em todas as abas — a tabela geral e as três abas de
  produto são somadas a partir das séries semanais, não de um total anual pronto.
- **Filtro de CS** existe em **todas as abas** e é de seleção múltipla (nenhum
  marcado = todo o time), com atalhos "Todos" e "Inverter". Recorta tabela *e*
  gráfico. Em Cross-sell e Upsell o responsável é o dono do negócio no HubSpot,
  caindo no CS da carteira quando o negócio não tem dono; em Tarefas e Tickets é
  o dono do registro; nas demais, o CS da carteira da marca. O mesmo responsável
  gravado com nome curto na carteira e completo no registro ("Jennyfer Rabelo" ×
  "Jennyfer Rabelo dos Santos") é juntado num único filtro por `normalizarCs()`.
- **Filtro de Canal** é de seleção múltipla (nenhum marcado = todos) e vale na
  tabela geral, nas três abas de produto, no Churn e nos Tickets. Nas abas que
  agregam por cliente, o canal é buscado no cadastro pelo nome da marca.
- Os gráficos obedecem só a cliente + CS. Filtros de linha (status do negócio,
  situação do churn) não mexem no histórico, de propósito.
- Exportar CSV exporta exatamente as linhas e colunas visíveis.
- **"Hoje" é o dia de verdade do navegador.** Era uma data fixa (13/08/2026) de
  quando o painel foi escrito; com a carga diária, tudo que é relativo ("há X
  dias", tarefa atrasada, tempo de casa) tem que andar com o calendário.
