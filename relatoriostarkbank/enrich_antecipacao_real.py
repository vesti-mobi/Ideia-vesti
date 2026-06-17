# -*- coding: utf-8 -*-
"""Reconcilia o `dados.js` (Mongo) com o `invoices.js` (API Stark) para que o CP
mostre o valor *real* a ser antecipado.

Bug original
------------
O Mongo carimba todas as parcelas antecipadas de um pedido com o mesmo
`payDate` (= data prevista para repasse) mesmo que o Starkbank antecipe
apenas ~80% das parcelas. Resultado: o CP infla o valor a pagar hoje.

Regra do Stark
--------------
Quando o Stark antecipa uma parcela ela ganha:
  * tag `contract-chunk/<id>` em `tags`
  * `due` re-escrito para D+1 (data da antecipação)
Parcelas NÃO antecipadas ficam com `due == nominalDue` e sem essa tag — vão
liquidar normalmente no mes do fluxo.

O que este script faz
---------------------
Para cada pagamento Mongo com `isAntecipacao=true`:
  1. Localiza a installment correspondente no Stark pelo
     `(orderId, ordinal_da_parcela_por_nominalDue_asc)`.
  2. Se a installment NÃO tem `contract-chunk` (não foi de fato antecipada):
     - `payDate`        := `nominalDue` (BRT, 10 chars) — joga pro fluxo normal
     - `isAntecipacao`  := False                       — sai do filtro Antec.
     - `antecipacaoConfirmada` := False                — marca explícita
  3. Se tem chunk: mantém payDate e marca `antecipacaoConfirmada=True`.

Pagamentos sem orderId ou sem match na API são deixados intactos e contados
no relatório no final.

Reescreve `dados.js` no lugar.
"""

import io
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
DADOS_JS = ROOT / "dados.js"
INVOICES_JS = ROOT / "invoices.js"


def load_window(path: Path, varname: str):
    src = path.read_text(encoding="utf-8").strip()
    m = re.match(rf"window\.{varname}\s*=\s*(.*);\s*$", src, re.DOTALL)
    if not m:
        raise SystemExit(f"[ERRO] formato inesperado em {path.name}")
    return json.loads(m.group(1))


def brt_date(iso: str) -> str:
    """ISO UTC -> 'YYYY-MM-DD' BRT (UTC-3). Aceita string vazia."""
    s = str(iso or "")
    if not s:
        return ""
    try:
        return (datetime.fromisoformat(s.replace("Z", "+00:00")) - timedelta(hours=3)).date().isoformat()
    except Exception:
        return s[:10]


def build_stark_index(inv: dict) -> dict:
    """orderId -> [ {ordinal,nominalDue(BRT),due(BRT),status,hasChunk}, ... ]
    Ordinal é 1-based, ordenado por nominalDue asc (e id desempate).
    """
    idx: dict[str, list[dict]] = {}
    for f in inv.get("faturas", []):
        p = f.get("purchase") or {}
        order_id = None
        for t in (p.get("tags") or []):
            if isinstance(t, str) and t.startswith("order_"):
                order_id = t[6:]
                break
        if not order_id:
            order_id = f.get("orderId")
        if not order_id:
            continue
        insts = list(p.get("installments") or [])
        insts.sort(key=lambda i: ((i.get("nominalDue") or i.get("due") or ""), str(i.get("id") or "")))
        rows = []
        for n, i in enumerate(insts, start=1):
            tags = i.get("tags") or []
            has_chunk = any(isinstance(t, str) and t.startswith("contract-chunk/") for t in tags)
            rows.append({
                "ordinal": n,
                "nominalDue": brt_date(i.get("nominalDue") or i.get("due")),
                "due": brt_date(i.get("due")),
                "status": i.get("status") or "",
                "hasChunk": has_chunk,
            })
        idx[order_id] = rows
    return idx


def main() -> int:
    dados = load_window(DADOS_JS, "DADOS")
    inv = load_window(INVOICES_JS, "INVOICES")
    stark = build_stark_index(inv)
    pags = dados.get("pagamentos") or []

    stats = defaultdict(int)
    by_marca = defaultdict(lambda: {"moved": 0, "kept": 0, "moved_gross": 0.0})

    for p in pags:
        if not p.get("isAntecipacao"):
            continue
        stats["antec_total"] += 1
        oid = p.get("orderId") or ""
        inst = p.get("installment") or 0
        rows = stark.get(oid)
        if not rows:
            stats["sem_match_purchase"] += 1
            continue
        if inst <= 0 or inst > len(rows):
            stats["sem_match_installment"] += 1
            continue
        row = rows[inst - 1]
        if row["hasChunk"]:
            p["antecipacaoConfirmada"] = True
            stats["antec_confirmada"] += 1
            by_marca[p.get("nomeFantasia", "")]["kept"] += 1
        else:
            # não foi antecipada — joga pro fluxo
            old = p.get("payDate") or ""
            new_pd = row["nominalDue"] or row["due"] or old
            p["payDate"] = new_pd
            p["isAntecipacao"] = False
            p["antecipacaoConfirmada"] = False
            stats["antec_movida_p_fluxo"] += 1
            by_marca[p.get("nomeFantasia", "")]["moved"] += 1
            by_marca[p.get("nomeFantasia", "")]["moved_gross"] += float(p.get("grossValue") or 0)

    # rewrite dados.js
    out = "window.DADOS = " + json.dumps(dados, ensure_ascii=False, separators=(",", ":")) + ";\n"
    DADOS_JS.write_text(out, encoding="utf-8")

    print("=== enrich_antecipacao_real ===")
    for k, v in stats.items():
        print(f"  {k:<28} {v}")
    print()
    print("Top marcas com parcelas movidas p/ fluxo:")
    items = sorted(by_marca.items(), key=lambda kv: -kv[1]["moved"])
    for marca, v in items[:15]:
        if v["moved"] == 0:
            continue
        print(f"  {marca:<40} moved={v['moved']:>4}  kept={v['kept']:>4}  gross_movido=R$ {v['moved_gross']:>12,.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
