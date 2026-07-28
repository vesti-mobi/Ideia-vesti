#!/usr/bin/env python3
"""Preenche nomeFantasia vazio no dados.js (CP) usando o invoices.js (CR).

Por que existe: o fetch_data.py (CP) resolve o nome da marca lendo o
invoices.js que JA esta no repo (_company_meta_from_invoices). No workflow o
CP roda ANTES do fetch_invoices.py, entao uma marca cujo primeiro pedido
Starkbank e recente ainda nao esta no invoices.js do commit anterior e sai com
nomeFantasia="" — o painel entao mostra o companyId cru (index.html faz
`nomeFantasia || companyId`). Caso real: Sisal Jeans (1329765) em 28/07/2026.

Este passo roda DEPOIS do fetch_invoices.py e reaplica os nomes que ja estao
disponiveis. Nao inventa nome: se o companyId tambem nao estiver no
invoices.js, deixa como esta (o proximo run resolve).

Idempotente: se nao houver nada pra corrigir, nao reescreve o arquivo.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
DADOS_JS = BASE / "dados.js"
INVOICES_JS = BASE / "invoices.js"


def _load_js(path: Path) -> dict:
    """Le um arquivo `window.X = {...};` e devolve o objeto JSON."""
    txt = path.read_text(encoding="utf-8")
    return json.JSONDecoder().raw_decode(txt, txt.index("{"))[0]


def _meta_from_invoices(inv: dict) -> dict[str, dict]:
    """companyId -> {nome, ws}. Primeira ocorrencia com nome nao-vazio vence."""
    meta: dict[str, dict] = {}
    for f in inv.get("faturas", []):
        cid = f.get("companyId")
        nome = (f.get("nomeFantasia") or "").strip()
        if not cid or not nome or cid in meta:
            continue
        meta[cid] = {"nome": nome, "ws": str(f.get("workspaceId") or "").strip()}
    return meta


def main() -> None:
    if not DADOS_JS.exists() or not INVOICES_JS.exists():
        print("[patch] dados.js ou invoices.js ausente — nada a fazer",
              file=sys.stderr)
        return

    dados = _load_js(DADOS_JS)
    meta = _meta_from_invoices(_load_js(INVOICES_JS))
    print(f"[patch] {len(meta)} marcas mapeadas via invoices.js")

    corrigidos: dict[str, int] = {}
    faltando: set[str] = set()
    for bloco in ("pedidos", "pagamentos"):
        for r in dados.get(bloco, []):
            if (r.get("nomeFantasia") or "").strip():
                continue
            m = meta.get(r.get("companyId") or "")
            if not m:
                faltando.add(r.get("companyId") or "")
                continue
            r["nomeFantasia"] = m["nome"]
            # workspaceId vem do mesmo lookup e fica vazio pelo mesmo motivo
            if not str(r.get("workspaceId") or "").strip() and m["ws"]:
                r["workspaceId"] = m["ws"]
            corrigidos[m["nome"]] = corrigidos.get(m["nome"], 0) + 1

    if faltando:
        print(f"[patch] ainda sem nome (nao estao no invoices.js): "
              f"{sorted(faltando)}", file=sys.stderr)

    if not corrigidos:
        print("[patch] nenhum nome vazio pra corrigir")
        return

    DADOS_JS.write_text(
        "window.DADOS = " + json.dumps(dados, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    total = sum(corrigidos.values())
    print(f"[patch] {total} registros corrigidos em {len(corrigidos)} marca(s): "
          + ", ".join(f"{k} ({v})" for k, v in sorted(corrigidos.items())))


if __name__ == "__main__":
    main()
