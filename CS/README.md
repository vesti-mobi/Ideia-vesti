# Painel de Clientes — CS

Painel estático de **10 abas**, grão de **dia**, filtrado por **data de início e fim**.

```
index.html          o painel (layout + lógica, sem dependência externa)
publicar.js         sobe para vesti-mobi/dados/CS via Git Data API
dados.js            dados reais gerados pelo fetcher — window.PAINEL_DATA
fetch_dados.js      carga: BigQuery + HubSpot + Tino -> dados.js
sincronizar_cs.js    roda ANTES do fetch: corrige angel_id no BigQuery comparando com a produção (Metabase)
carregar_tipo_empresa.js   leva a classificação Atacado × Varejo para o BigQuery
integracoes_snapshot.json  retrato de quem tem integração, da última carga
integracoes_novas.json     histórico das integrações detectadas como novas
varejo_manual.json         marcação FEITA NA MÃO de quais filiais são de varejo
painel-clientes.html  layout de referência original (não é usado em produção)
```

## O filtro é por data (mudou em 26/08/2026)

Era uma seleção de semanas ISO. A Laura levantou o problema: **semana ISO
atravessa a virada do mês** — a semana 31 de 2026 vai de 27/julho a 2/agosto —
então nenhum recorte semanal fechava um mês, e todo número mensal vinha com
sobra do mês vizinho. Agora o filtro é **De … Até**, com atalhos (este mês, mês
passado, últimos 30/90 dias, ano todo) e uma grade de meses fechados.

O que mudou por baixo:

- **Todas as séries passaram de `semana` para `data`** (`AAAA-MM-DD`), no fetcher
  e no painel. A carga vai de 1º de janeiro do **ano anterior** até hoje — o ano a
  mais existe só para a aba de Bonificação comparar mês contra o mesmo mês do ano
  passado; as séries que vão para o navegador continuam só com o ano corrente.
- **Os gráficos agrupam sozinhos**: até 45 dias de período, dia a dia; até 400,
  por semana; acima disso, por mês. O filtro continua exato — quem agrupa é só o
  desenho.
- **As séries vão empacotadas** em formato colunar (`{c:[colunas], dic:{}, r:[[…]]}`).
  Sem isso o `dados.js` iria de 6,5 MB para 37 MB, e ele é commitado todo dia.
  O painel desempacota no boot, em `desempacotar()`.
- **Correção de brinde:** a tabela geral e as abas de produto passaram a agregar
  **por domínio** e não por nome. 28 marcas da carteira são homônimas de outra
  (mesmo apelido, domínios diferentes) e as duas linhas recebiam a soma das duas —
  R$ 684 mil a mais de GMV só em julho de 2026. O domínio só passou a viajar na
  série quando o grão virou dia; antes não dava para corrigir.
- **Compatibilidade:** se o painel novo abrir com um `dados.js` ainda no formato
  de semana (a janela entre publicar o layout e a carga da madrugada), cada linha
  antiga vira a segunda-feira da semana dela e um aviso vai para o console. Os
  números ficam aproximados até a carga seguinte, em vez de zerados.

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
  PainelElisa usa. O `fetch_dados.js` em si só roda `SELECT` — quem escreve é o
  `sincronizar_cs.js` (ver abaixo), e só na coluna `angel_id`.
- **HubSpot** — `HUBSPOT_TOKEN`; por padrão lê de `../CS-Sucesso-do-cliente/.env`.
- **Tino** — `TINO_USER` / `TINO_PASS`, a mesma credencial do painel
  `admin.tino.vesti.com.br`. **Sem valor padrão no código de propósito**: o
  `vesti-mobi/dados` é público.
- **Metabase** (opcional) — `METABASE_URL` / `METABASE_API_KEY`, só para o
  `sincronizar_cs.js`. Sem eles esse passo é pulado e o `angel_id` fica do jeito
  que estava no espelho — o resto da carga roda igual.

Sem HubSpot o painel carrega mesmo assim: Cross-sell, Upsell, Reuniões e Tickets
ficam vazias (e, na Bonificação, a coluna de reuniões). Sem a credencial do Tino, a aba do Tino e a coluna "Tino" da
tabela geral ficam vazias — o resto carrega igual.

### CS responsável desatualizado no BigQuery (04/09/2026)

Descoberto com a Laura: trocar o CS responsável no admin da Vesti (campo
`angel_id` de `domains`) às vezes não atualiza o `updated_at` da linha, e a
réplica Vesti → BigQuery usa esse campo para saber o que sincronizar — a troca
fica **invisível pro espelho para sempre**, até algo mais tocar aquela linha.
Confirmado comparando o Postgres de produção (via Metabase) contra o
`vestilake_BI.odbc_domains`: 36 marcas da carteira ativa divergentes no dia da
descoberta, entre elas a MissManu (BigQuery dizia Tatiane Ayres, produção já
tinha Thamiris Ribeiro havia semanas).

`sincronizar_cs.js` roda **antes** do `fetch_dados.js` na carga automática:
compara `angel_id` de cada domínio da carteira ativa entre o Postgres de
produção (Metabase, banco "Vesti") e o BigQuery, e corrige no BigQuery só as
linhas que divergem — nunca a tabela inteira. Isso conserta o dado na fonte
que o painel lê, então beneficia qualquer outro painel da Vesti que também use
`odbc_domains`, não só este. Se o Metabase cair ou faltar credencial, o passo
avisa e segue sem corrigir nada — não trava a carga.

### Automático, todo dia às 04:00 BRT

O workflow `.github/workflows/painel-clientes-cs.yml` no `vesti-mobi/dados` roda
`node CS/sincronizar_cs.js` e depois `node CS/fetch_dados.js` às 07:00 UTC, e
commita o `CS/dados.js`. Como o painel inteiro — todas as abas, tabelas e
gráficos — lê desse único arquivo, uma carga atualiza tudo. Usa os secrets
`GCP_SA_KEY`, `HUBSPOT_TOKEN`, `METABASE_URL` e `METABASE_API_KEY`, que já
existem no repositório, e aborta sem commitar se o `dados.js` sair com menos de
1 MB (sinal de que alguma fonte falhou). Dá para rodar na mão pela aba Actions
("Painel de Clientes CS" → Run workflow). A cópia local do arquivo é o
`atualizar-painel.yml` aqui na pasta.

⚠️ O `index.html` e o `README.md` **não** são publicados pelo workflow — mudança
de layout continua indo por `node publicar.js`.

## De onde vem cada coluna

| Aba | Campo | Fonte |
|---|---|---|
| Todas | Domínio | `odbc_domains.ID` — o id da marca na Vesti, chave de busca no admin |
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
| Bonificação | Integrações ativas | Marcas do CS que venderam no mês E têm `odbc_domains.integration_owner = 'VESTI'`. Definição da medida `Integracao Ativa` do Power BI "GMV - Métricas 2025": integração que a Vesti mantém, por oposição à do parceiro/ERP (a passiva). É estoque, não fluxo: conta quem TEM integração e vendeu, não quem integrou no mês |
| Tabela geral | Tino / Oráculo / VestiPago | Tino: marca presente na base do produto (API do Tino). Oráculo: tem atendimento em `oraculo_Atendimentos`. VestiPago: tem conta de pagamento em `MongoDB_Payment_Companies` — é TER o produto, não usar; quem contratou e não transacionou aparece como Sim |
| Tino | marcas, eventos, sessões, dias de acesso | **API do Tino** — `companies_chart`, `login_days`, `customer_kpis` |
| VestiPago | valor / fee / antecipação | `MongoDB_Pedidos_Geral` com provider VestiPago |
| VestiPago | links | pedidos com `settings_source = 'Link de cobrança'` |
| Tino / VestiPago / Oráculo | Implantado em | Tino: `created_at` da marca na base do produto. VestiPago: `MongoDB_Payment_Companies.createdAt`. Oráculo: a menor entre `o-configurations.created_at` e o primeiro atendimento |
| Bonificação | tudo | agregado por CS e por mês, calculado no fetcher (ver abaixo) |
| Churn | data | derivado do Iugu (ver abaixo) |
| Reuniões | reunião / data / responsável | HubSpot `meetings` (`hs_meeting_start_time` + owner) |
| Reuniões | Cliente | empresa associada à reunião no HubSpot |
| Reuniões | Resultado | negócio ganho (qualquer pipeline) creditado à última reunião daquela empresa antes do fechamento |
| Tickets | ticket / pipeline / estágio | HubSpot `tickets`, todos os pipelines |
| Tickets | Situação | estágio marcado como fechado (`metadata.ticketState`) ou `closed_date` preenchida |
| Tickets | Cliente e Canal | empresa associada ao ticket → marca do cadastro (nome, nome sem ruído ou CNPJ) |

### Domínio nas abas

A coluna **Domínio** (`odbc_domains.ID`) aparece nas nove abas, ao lado do cliente.
Onde a linha nasce de um domínio — tabela geral, Oráculo, Tino, VestiPago, Churn —
o id vem direto e a cobertura é de 97% a 100%. Nas abas que vêm do HubSpot
(Cross-sell, Upsell, Reuniões, Tickets) a linha nasce com o **nome da empresa no
HubSpot**, então o id é resolvido pelo mesmo casamento em três tentativas usado
para o canal do ticket; sem casar fica `—`. Tickets fica em ~34% porque boa parte
deles é de empresa que não é marca da carteira — é a mesma limitação já
documentada na linha "Cliente e Canal" da tabela acima, não um defeito da coluna.

Inventar um id quando o nome não casa seria pior do que deixar em branco: a coluna
existe justamente para ser colada na busca do admin.

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
   das transações VestiPago (108.950 transações / R$ 90,2M em 2026). Então
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

5. **Links do VestiPago = só `'Link de cobrança'`.** Conferido no dado: os pedidos
   dessa origem que aparecem sem provider são exatamente os que ninguém pagou
   (8.562 pedidos, 0 pagos), ou seja, o link é do VestiPago de ponta a ponta.
   `'Link sem preço'` ficou de fora porque tem 4.226 pedidos **pagos por fora**
   (R$ 8,8M), e `'Link'` puro (573k pedidos) é o link de compartilhamento do
   vendedor, não de cobrança.

6. **Pedido com provider nulo não é VestiPago.** 162k pedidos pagos / R$ 352M de GMV
   em 2026 são vendas registradas na plataforma mas pagas por fora. Entram no GMV da
   tabela geral e ficam fora da aba VestiPago. É por isso que marcas grandes como
   Egoiste e JDL aparecem com receita R$ 0 — está certo, elas não usam o produto.

6b. **Data de implantação por produto (26/08/2026).** Cada aba de produto ganhou
   a coluna "Implantado em", com o tempo de casa embaixo; marca que entrou dentro
   do período selecionado aparece destacada, porque o número dela cobre menos
   dias que o das outras. As fontes estão na tabela acima. **A do Oráculo pede
   cuidado**: 699 dos 1.055 domínios têm a configuração criada em jan/2026, que é
   quando a tabela nasceu no espelho — para esses, a data é do espelho e não da
   venda. Quando o primeiro atendimento é anterior, ele é que vale, e a coluna diz
   de onde veio cada data.

6c. **Bonificação: os números e, embaixo, os pontos.** A aba traz as sete regras
   da planilha da Laura, uma coluna cada, por CS e por mês-calendário — mais
   "Integrações ativas", que entrou em 01/09/2026 vinda do Power BI. Três regras
   comparam com a **marca d'água** do CS (Tino +40 eventos, mensalidade,
   integrações ativas), duas com
   o mesmo mês do ano passado (VestiPago, GMV) e três são contagem do próprio mês
   (reuniões, varejos). Passar o mouse no número lista as marcas que
   entraram nele. As três de dinheiro (mensalidade, VestiPago, GMV) mostram a
   variação em **porcentagem**, não em reais (27/08/2026) — o valor em R$ da
   diferença fica no title da célula.

   A **segunda tabela** converte esses mesmos números em pontos (régua da Laura,
   revisada em 01/09/2026), sem medir nada de novo:

   | regra | pontos |
   |---|---|
   | Tino | 10 por cliente extra com 40+ eventos, acima da marca d'água |
   | Mensalidade | meta 0: 3 a cada 1% acima da marca d'água |
   | VestiPago | meta 40%: 2 a cada 1% acima dela, vs. o mesmo mês do ano passado |
   | Reuniões | 1 cada |
   | Integrações ativas | 10 por integração acima da marca d'água |
   | GMV | meta 10%: 2 a cada 1% acima dela, vs. o mesmo mês do ano passado |
   | Varejos | 10 cada |

   **Meta é piso, e não paga nada por si.** Bater exatamente 40% no VestiPago é
   zero; 41% são 2 pontos. Só ponto fechado conta: 41,9% continua sendo 1 de
   excedente, não 2.

   **"Novas integrações" foi SUBSTITUÍDA por "Integrações ativas" em 01/09/2026.**
   A antiga media o fluxo — integração nova fechada no mês — e dava 0 ou 1: foram
   7 no time todo em 2026, e nunca duas no mesmo mês para o mesmo CS. A nova mede
   o estoque e herdou a vaga de pontos, agora valendo 10 por integração acima da
   marca d'água. O retrato diário (`integracoes_snapshot.json`) **continua sendo
   gravado** mesmo sem a coluna que ele alimentava — é a única coisa que registra
   quando uma integração começa, e um dia não fotografado não volta.

   A **marca d'água** (31/08/2026) é o maior número que aquele CS já registrou em
   um mês, contando só os meses **anteriores** ao escolhido — incluir o próprio mês
   faria a diferença ser sempre zero ou negativa e ninguém bateria a própria marca.
   Antes, Tino e mensalidade comparavam com o mês anterior; na prática o ponto
   ficou mais caro, porque só pontua quem faz o melhor mês da própria história. O
   primeiro mês da série não tem recorde atrás e compara contra zero, igual ao que
   já acontecia com um "mês anterior" inexistente.

   Duas decisões que a régua não dizia: **queda não tira ponto** (mês que não bate
   a marca d'água, ou pior que o ano passado, dá zero, não negativo) e, nas
   porcentagens, **só bloco fechado conta** — 3,9% no
   VestiPago é 1 ponto, não 2. A mensalidade é a única que arredonda para cima,
   porque foi o que ela pediu. Quem não tem base no ano anterior (marca que não
   existia, ou CS que assumiu a carteira depois) faz zero ponto nas regras de %:
   não dá para chamar de crescimento o que não tem de onde crescer. O title de
   cada célula mostra a conta que gerou o ponto.

   Três coisas para ler junto com a tabela:
   - **A carteira é a de hoje.** A marca é creditada ao CS que a atende agora,
     porque o cadastro não guarda o histórico de quem atendia antes. Quem assumiu
     carteira no meio do caminho aparece com base baixa em 2025 e um "crescimento"
     que é troca de responsável.
   - **Varejo novo = filial nova de marca já existente, e de VAREJO.** São duas
     perguntas, e cada uma tem sua fonte:
     - *é filial?* `odbc_companies.parent_id` nunca vem preenchido no espelho
       (zero linhas em 20 meses), então filial é a 2ª empresa em diante do mesmo
       domínio, por ordem de criação — a mesma régua do PainelElisa. Filial com
       "teste" no nome fica de fora.
     - *é de varejo?* **Não existe no espelho `odbc_*`**: `lojista` vem `false` em
       todas as filiais, `market` vem nulo, as 18 tags são de segmento de moda
       (jeans, fitness, praia) e o canal "Varejo Vesti" está zerado desde
       jul/2025. A marcação real é a coluna **"Tipo _Atacado | Varejo_"** do
       Relatório Confecções (Fabric, `dbo.Confeccao2025_Query1`), a mesma que o
       CS-Sucesso usa. Ela foi trazida para o BigQuery em **`confeccao_tipo_empresa`**
       por `carregar_tipo_empresa.js`.

     Isso muda o número: contando qualquer filial dava 3 a 13 por mês; contando só
     as de varejo dá **1 a 4**. Cerca de cinco em cada seis filiais novas são de
     atacado.

     ⚠️ **A classificação tem data de corte.** A carga atual é de **30/03/2026**,
     porque veio do CSV do CS-Sucesso: em 26/08/2026 o warehouse do Fabric não
     respondia nem a um `SELECT 1` ("Couldn't complete the operation due to a
     system update", em 6 tentativas com reconexão). Filial criada depois disso
     fica sem tipo, **não entra na conta** e aparece na célula como
     "+N sem classificação" — para ninguém ler zero como "não abriu nenhuma".
     Quando o Fabric voltar, `node carregar_tipo_empresa.js --fabric` atualiza a
     tabela e a aba melhora sozinha, sem mexer em mais nada.

     ✍️ **Dá para marcar na mão, e é o que vale.** Como a classificação automática
     está congelada, a aba deixa dizer filial por filial se é de varejo: na coluna
     **"Varejos novos"**, clicar no número abre a lista daquele CS naquele mês, com
     três botões por filial — *Varejo*, *Atacado* e *Automático* (seguir o cadastro).
     Também dá para apontar uma **marca inteira**, para o varejo que abre em domínio
     próprio ("Nicoboco Varejo") e que a régua de "2ª empresa do mesmo domínio" nunca
     enxerga como filial.

     **Um clique em "Salvar para todo mundo" e acabou.** A marcação vai para
     `CS/varejo_manual.json` no repositório e passa a valer para quem abrir o painel,
     sem esperar a carga da madrugada.

     Como uma página estática não commita nada sozinha, quem grava é a **API do
     stark-admin** (`https://vesti-contas.vercel.app/api/varejo`), que guarda o
     `GH_TOKEN` no servidor — mesma ideia do `overlays.js` do relatoriocs2:
     - `GET /api/varejo` é **público**: o painel lê no boot e aplica por cima do
       `dados.js`, então a marcação de uma CS aparece para as outras no mesmo dia;
     - `POST /api/varejo` pede `Authorization: Bearer <senha do painel>` (env
       **`CS_SENHA`** na Vercel, hoje igual à senha da tela). Manda só o que mudou;
       o servidor aplica em cima do arquivo mais novo com retry, então duas CS
       marcando ao mesmo tempo não se apagam. Cada salvamento é **1 commit**.
     - ⚠️ a senha de escrita é a mesma que está no código da página. Quem abre o
       painel pode marcar varejo — é o mesmo nível de acesso do painel em si. Para
       separar, basta trocar `CS_SENHA` na Vercel e pedir a senha na tela.

     Enquanto não foi salvo, fica **só no navegador** (`localStorage`, chave
     `painelcs:varejo-manual`) e aparece com um **ponto dourado** — inclusive se a
     API estiver fora. O botão **"Baixar arquivo"** é o plano B: gera o
     `varejo_manual.json` para publicar à mão (`node publicar.js --varejo --sem-dados`).

     O `fetch_dados.js` lê esse mesmo arquivo em cada carga, então os números
     agregados do `dados.js` também nascem já com a marcação.

     Precedência: **marcação no navegador > `varejo_manual.json` > cadastro**. O
     `publicar.js` se recusa a subir um `varejo_manual.json` local que tenha MENOS
     marcações do que o que já está no repositório — seria apagar o trabalho de
     outra pessoa; nesse caso ele avisa e publica o resto.
   - **Integração nova ainda é o que o HubSpot registrou.** O cadastro guarda quem
     TEM integração, não desde quando — não há histórico para reconstruir. A partir
     desta versão a carga fotografa a carteira todo dia (`integracoes_snapshot.json`)
     e acumula o que mudou (`integracoes_novas.json`), então o número melhora
     sozinho a cada dia que passa. Os dois arquivos são commitados pelo workflow:
     sem isso o retrato nasceria vazio a cada execução e nada seria detectado.

7. **Churn é derivado, não é um campo.** Regra: sem fatura Iugu paga há mais de 45
   dias **e** sem fatura futura em aberto; a data do churn é a última fatura paga +
   45 dias. Dá 48 cancelamentos em 2026. Quando as `iugu_subscriptions`
   estabilizarem, trocar por cancelamento de assinatura é mais direto.

8. **Cross-sell × upsell sai do nome do negócio.** A API não liberou escopo de
   `line_items` (403), então a classificação usa os padrões em `REGRAS_PRODUTO`.
   Regra definida na revisão de 13/08: **upsell = só upgrade de plano**; todo o
   resto (Filial, Multiloja, Oráculo, Integração, Assistente, VestiPago, Tino) é
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
    pipeline conta (o time fecha filial, upgrade e VestiPago em pipelines
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
