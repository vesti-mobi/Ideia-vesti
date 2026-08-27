# Tamanho da marca — seguidores e loja física (via Apify)

`tamanho_marca.py` alimenta a aba **📐 Tamanho da marca** do painel: quão grande a
marca é *fora* da Vesti, pra achar quem cresceu mas continua num plano antigo e barato.

```bash
py tamanho_marca.py --limit 3      # piloto barato
py tamanho_marca.py                # tudo (pergunta antes de gastar)
py tamanho_marca.py --so-instagram # pula o Maps (bem mais barato)
py tamanho_marca.py --so-json      # só regera o JSON, sem chamar API
```

Lê as abas `PRO` e `Light  Starter ` de `Mensalidade até 400 .xlsx`, grava na aba
`Histórico` da mesma planilha (append: cada coleta é um bloco novo) e em
`tamanho_marca.json`, que o `build_data.py` cruza com a base **por CNPJ** (nome só
como queda — a planilha chama de "Chik Bijux" o que a base chama de "Lusby Salas suyo").

**Porte** combina os dois sinais: `Muito grande` (500k+ seguidores ou 5+ lojas),
`Grande` (100k+ ou 3+), `Media` (20k+ ou 1+), `Pequena`, `Indefinido`.
A fila da aba ordena por porte alto × mensalidade baixa.

### Custo (plano FREE = US$ 5/mês)

Medido em 13/08/2026: Instagram **US$ 0,0026/perfil**, Maps **US$ 0,005/estabelecimento**.
As 87 marcas dão ~US$ 1,10–2,40. Chamadas são **em lote** (uma run por bloco, não por
marca) — é o que cabe no plano gratuito. `MAX_LOCAIS_POR_BUSCA` limita o gasto do Maps.

### ⚠️ O número de lojas é indício, não certeza

O Maps devolve qualquer estabelecimento que case com o texto da busca. No piloto,
"Gringa" trouxe **Coxinha Da Gringa** e **Gringa Burger**. `_lugar_e_da_marca()` filtra
por categoria (só varejo/moda) **e** por nome, mas ainda passa loja de roupa homônima
("MAFRA GRINGA"). Por isso a coluna *O que o Maps achou* mostra os nomes encontrados —
confira antes de ligar para a marca.

### ⚠️ Token do Apify

Vem de `APIFY_API_TOKEN` ou do arquivo `.apify_token` (no `.gitignore`).
**O token está em texto puro em `gemini-code-*.txt` dentro do projeto** — esse padrão
também entrou no `.gitignore`, mas convém **rotacionar a chave** em
apify.com → Settings → Integrations, já que ela circulou em prompt.

---

# Planilha de upgrade — loja física e Instagram (fluxo antigo, BrasilAPI)

Fluxo em dois passos, a partir de `Mensalidade até 400 .xlsx`.

## Passo 1 — preparar

```bash
py upgrade_planilha.py preparar
```

Consolida as **3 abas** (`CS`, `Vesti`, `Light Starter`) numa aba só, tira as
marcas repetidas entre abas e já preenche o que dá pra tirar da base da Vesti,
cruzando pelo nome da marca:

| Vem de graça da base | Observação |
|---|---|
| CNPJ | 133 das 176 marcas casaram automaticamente |
| Razão social | do `odbc_companies` |
| CS, canal, mensalidade | do painel |
| **Filiais na Vesti** | nº de CNPJs extras no mesmo domínio — é o sinal mais forte de loja física |
| GMV 3 meses | últimos 3 meses fechados |

Gera `upgrade_marcas.xlsx`. As duas colunas **laranja** são suas:

- **CNPJ** — 43 marcas não casaram pelo nome (a coluna "Marca na base?" mostra
  `NAO ENCONTRADA`). Preencher só essas.
- **Instagram (@)** — nenhuma vem preenchida, a base da Vesti não guarda isso.

## Passo 2 — enriquecer

```bash
py upgrade_planilha.py enriquecer
```

Para cada CNPJ consulta a **BrasilAPI** (dados da Receita Federal, de graça e sem
chave) e preenche CNAE principal e descrição, porte, matriz/filial, situação
cadastral, município e UF. Com isso calcula:

- **Provável loja física** — `provavel SIM` / `talvez` / `provavel NÃO` / `indefinido`
- **Motivo loja física** — sempre explica de onde veio a conclusão

O veredito soma sinais, não é certeza:

| Sinal | Peso |
|---|---|
| Tem filial cadastrada na Vesti | +2 |
| CNAE principal de varejo em loja (4781, 4782, 4713…) | +2 |
| CNAE secundário de varejo em loja | +1 |
| CNPJ é FILIAL de um grupo | +1 |
| CNAE principal de venda por internet/catálogo (4791-4793) | −2 |

3 pontos ou mais → `provavel SIM`. Sempre confira o motivo antes de ligar
para a marca — uma confecção (CNAE 1412) pode ter loja e não aparecer no CNAE.

As respostas ficam em `.cache_cnpj.json`; rodar de novo não repete consulta.

### Seguidores do Instagram

**Não existe caminho gratuito e confiável.** O Instagram bloqueia requisição
anônima, então o script só preenche a coluna se houver uma chave de API:

```bash
set INSTAGRAM_API_KEY=sua_chave_rapidapi
py upgrade_planilha.py enriquecer
```

Serviço testado: `instagram-scraper-api2` no RapidAPI (tem plano gratuito com
cota baixa; 176 perfis custam poucos dólares no plano pago). Dá pra trocar o
provedor pela variável `INSTAGRAM_API_HOST` — o script lê `follower_count`,
`followers` ou `edge_followed_by.count` da resposta.

Sem chave, a coluna fica vazia e é preenchida à mão.
