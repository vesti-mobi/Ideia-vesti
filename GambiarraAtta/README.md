# Inadimplentes · Atta

Link dedicado da aba Inadimplentes filtrada no canal **Atta**, para publicar no Vercel.

## O que é

Uma página só, com um iframe para o Gambiarra publicado no GitHub Pages:

    https://vesti-mobi.github.io/dados/Gambiarra/?only=inadimplentes&canal=Atta

Não há cópia de código nem de dados. O modo travado (`aplicaModoTravado` em
`../Gambiarra/app.js`) faz o resto:

| Parâmetro | Efeito |
|---|---|
| `only=inadimplentes` | abre só essa aba e esconde a navegação |
| `canal=Atta` | trava o canal, troca as caixas por um rótulo fixo e esconde o seletor de empresa |

O filtro de **Situação** (Em alerta / Bloqueada / Cancelada), o de **CS** e o de
**Atraso mínimo** continuam funcionando — foi o que a Laura pediu.

Como os dados vêm do GitHub Pages, o link acompanha sozinho o
`painel-elisa.yml`, que roda todo dia às 08:00 BRT.

## ⚠️ Antes de mandar para fora da Vesti

**O travamento é de vista, não de dado.** O `dashboard_data.js` carregado dentro
do iframe traz as ~1.300 marcas do painel inteiro. Quem abrir o devtools vê
todas — inclusive CS, CNPJ e faturamento de marcas de outros canais.

Para um link realmente isolado é preciso gerar um `dashboard_data` só com o canal
Atta e servi-lo daqui, em vez do iframe. É uma mudança pequena no
`build_data.py`, mas ainda não foi feita.

Use este link internamente enquanto isso.

## Deploy

    vercel --cwd GambiarraAtta
    vercel --cwd GambiarraAtta --prod
