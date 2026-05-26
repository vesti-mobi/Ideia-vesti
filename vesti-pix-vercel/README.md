# Vesti — Pix Automático (Vercel)

App público de geração de Pix recorrente (jornada 3 Iugu) hospedado na Vercel.
Substitui o Streamlit `vesti-pix-automatico/` para casos cliente-facing.

## Como o cliente usa

Você manda um link tipo:

```
https://vesti-pix.vercel.app/?parceiro=starter&plano=mensal_49_90
```

- `parceiro` → nome curto da subconta Iugu (ex: `starter`, `uemtel`, `alcance`).
- `plano` → o `identifier` do plano cadastrado na Iugu.

O cliente abre, vê o plano (nome/valor/frequência puxados da Iugu), preenche nome/email/CPF e gera o Pix.

## Deploy na Vercel

1. **Linkar o projeto** apontando pra esta pasta `vesti-pix-vercel/` do repo `vesti-mobi/dados` (Root Directory na config do Vercel).
2. **Env vars** (Project Settings → Environment Variables):
   - `IUGU_TOKEN_STARTER` = token Iugu da subconta Starter
   - `IUGU_TOKEN_UEMTEL` = token da Uemtel
   - `IUGU_TOKEN_ALCANCE` = token da Alcance
   - (... uma por subconta. Formato: `IUGU_TOKEN_<NOME>` em caixa alta)
3. Cada push em `main` re-deploya automaticamente.

## Adicionando um novo parceiro

1. Criar env var `IUGU_TOKEN_<NOME>` no Vercel com o token da subconta.
2. Mandar link com `?parceiro=<nome em minúsculas>`.

## Adicionando/editando um plano

Edita direto no painel da Iugu — não precisa mexer no código nem deployar.
Use o `identifier` do plano na query string.

## Endpoints

- `GET /api/plano?parceiro=X&plano=Y` → `{nome, valor_cents, frequencia, ...}`
- `POST /api/gerar-pix` body `{parceiro, plano, nome, email, cpf}` → `{qrcode, qrcode_text, invoice_id, ...}`

## Listar planos cadastrados na Iugu

Usar `iugupixautomatico/listar_planos.py` local pra ver os `identifier` disponíveis em cada subconta.
