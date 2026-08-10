# Catálogo IA — Métricas de uso

Painel separado, no padrão dos outros (senha na entrada, HTML estático no GitHub Pages).

- **URL:** https://vesti-mobi.github.io/dados/CatalogoIA-Metricas/
- **Senha:** `Mudar123` (mesma dos demais painéis)

## O que mostra

- KPIs: produtos publicados, descrições geradas, marcas que usaram × liberadas, ativas, paradas, contas.
- **Linha do tempo** (12 semanas): produtos publicados por semana + crescimento da base de marcas.
- **Marcas ativas** por semana × marcas que deram algum sinal de vida.
- Tabela por marca: produtos publicados, últimos 7 dias, descrições geradas, envios, último acesso e situação.

## De onde vêm os dados

Não é BigQuery nem planilha. A telemetria do Catálogo IA é escrita pelo próprio app no
**Redis** (Upstash, o mesmo que guarda contas, arquétipos e o estilo aprendido).
O `fetch_metricas.js` lê esse Redis e congela um retrato em `dados.js` — assim o painel é
estático e o Redis não fica exposto na web.

## Atualizar

```bash
cd CatalogoIA-Metricas
node fetch_metricas.js        # reescreve dados.js
node fetch_metricas.js --ver  # só imprime, não escreve
```

Depois é só publicar o `dados.js` no repo. O script lê as credenciais de fora do repo:

| Variável | Onde está | Para quê |
|---|---|---|
| `REDIS_URL` | `ProjetoCatalogo/versao_auto/.env.local` | a telemetria |
| `VESTI_BRANDS` | `ProjetoCatalogo/.env` | só o **nome** de cada marca — nenhuma APIKEY entra no `dados.js` |

## Regras (as mesmas do `/admin` do app)

- **Ativa** = publicou 2 ou mais produtos nos últimos 7 dias.
- **Parada** = passou de 7 dias sem nenhum toque.
- **Produto conta uma vez**: reenviar o mesmo produto até aprovar o texto continua sendo
  1 produto publicado (o conjunto é por `integration_id`). Cada geração, porém, soma em
  "descrições geradas" — por isso descrições ≥ produtos.
- **O ≈ no total**: a telemetria só existe desde **24/07/2026**. Para o que veio antes, o
  número sai do aprendizado de estilo, que guarda o id de cada produto aprovado desde o
  início do projeto. É um piso histórico, subestimado de propósito — pode haver mais.
- **"Testou, não publicou"**: marca que gerou descrição e não enviou nenhuma para a Vesti.
  Não é o mesmo que "nunca usou", e é o caso que mais pede uma conversa.

## Automatizar (não feito)

Dá para rodar sozinho num workflow do GitHub Actions, como o PainelElisa. Precisaria do
`REDIS_URL` como secret do repo — decisão da Laura, porque hoje essa credencial não está lá.
