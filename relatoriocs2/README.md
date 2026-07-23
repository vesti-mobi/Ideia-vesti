# Relatório CS 2 — Passagem de Bastão / Tino / Upsell

Painel estilo planilha, 3 abas, hospedado no GitHub Pages do repo `dados`
(`https://vesti-mobi.github.io/dados/relatoriocs2/`). Senha: `Mudar123`.

## Abas
1. **Passagem de Bastão** — aba `PRO` da planilha "Implementação > CS Passagem de bastão".
   Traz todas as colunas + a **cor** de cada linha:
   - 🟩 verde = ativa · 🟥 vermelho = cancelada · ⬜ sem cor = sem reunião (contatar).
   - **Alerta ⏰** recalculado ao vivo: marca sem reunião alerta sempre; marca ativa
     alerta a cada 45 dias contados a partir da data da coluna **25+** (fallback: Entrada);
     cancelada nunca alerta.
   - Observação editável e cor da linha editável (salvo compartilhado — ver abaixo).
2. **Acessos Tino** — API `allblue-tinindo-tracking` (`/login_days`). Mostra dias sem
   acessar (= hoje − último acesso). **Alerta**: > 15 dias sem acessar, ou nunca acessou.
3. **Ranking Upsell** — abas `BUSTO` / `LUANA` / `THA` do `ranking_crescimento_upsell.xlsx`,
   unidas com filtro por CS. Observação + cor editáveis.

## Arquivos
- `index.html` / `styles.css` / `app.js` — front (estático).
- `dashboard_data.js` — dados gerados (`window.CS2_DATA`). **Único arquivo que o
  workflow reescreve.**
- `fetch_data.py` — fetcher. Lê as 2 planilhas + a API do Tino e gera `dashboard_data.js`.

## Atualização automática (workflow `relatoriocs2.yml`)
Roda diariamente. Secrets no repo `vesti-mobi/dados`:
- `RELCS2_TINO_USER` / `RELCS2_TINO_PASS` — credenciais da API do Tino.
- `RELCS2_GOOGLE_SA_JSON` — JSON de uma **service account** Google com acesso de leitura
  às 2 planilhas (compartilhar as planilhas com o e-mail da service account).

Sem `RELCS2_GOOGLE_SA_JSON`, o fetcher **preserva** o último snapshot das abas 1 e 3 e
atualiza só a aba 2 (Tino). A contagem de dias e os alertas de 45 dias são recalculados
ao vivo no navegador, então continuam corretos mesmo entre atualizações.

## Edições compartilhadas (observação + cor)
O front salva as edições em `window.CS2_API` (`/api/overlays`). Enquanto `CS2_API`
estiver vazio, as edições ficam só no `localStorage` do navegador. Ao apontar `CS2_API`
para o backend compartilhado, todas as CSs passam a ver as mesmas notas/cores.
