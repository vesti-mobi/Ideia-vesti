"""
Clientes Vesti (todos) que atingiram a marca de 80+ pedidos num mesmo mes
em 2026. Cada empresa aparece 1x APENAS no primeiro mes (em 2026) que
bateu a marca — se bate em jan e fev, so conta jan.

Adicionalmente traz:
  - dataBateu: data/hora do 80o pedido do mes qualificado
  - totalPedidos: total historico de pedidos da empresa (lifetime, todos
    os anos no lakehouse)

Output: top80_data.json

Formato (consumido pelo template.html via merge_data -> TOP80_DATA):
{
    "geradoEm": "2026-04-17T...",
    "threshold": 80,
    "linhas": [
        {
            "dominioId": "...",
            "marca": "...",
            "cs": "...",
            "canal": "...",
            "cnpj": "...",
            "mes": "2026-02",                  # primeiro mes 80+
            "dataBateu": "2026-02-18",         # dia do 80o pedido
            "qtTotal": 120,                    # pedidos no mes qualificado
            "qtPix": 30, "qtCartao": 15,
            "valTotal": 45123.45, "valPix": 15000, "valCartao": 7500,
            "totalPedidos": 4523                # historico da empresa
        }
    ],
    "mesesList": ["2026-01", "2026-02", ...],
    "csList": [...],
    "resumo": {"nEmpresas": 100, ...}
}
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from fetch_fabric import connect, load_config

ROOT = Path(__file__).parent
COMPANIES_JSON = ROOT / "companies_data.json"
OUT_JSON = ROOT / "top80_data.json"

THRESHOLD = 80
START_DATE = "2026-01-01"
END_DATE = "2027-01-01"

SQL_TOP80 = f"""
WITH orders_2026 AS (
    SELECT domainId, settings_createdAt_TIMESTAMP, summary_total, payment_method
    FROM dbo.MongoDB_Pedidos_Geral
    WHERE domainId IS NOT NULL
      AND TRY_CAST(domainId AS BIGINT) IS NOT NULL
      AND summary_total IS NOT NULL AND summary_total > 0 AND summary_total < 50000
      AND settings_createdAt_TIMESTAMP >= '{START_DATE}'
      AND settings_createdAt_TIMESTAMP < '{END_DATE}'
),
monthly AS (
    SELECT domainId,
           FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM') AS mes,
           COUNT(*) AS qt_total,
           SUM(CASE WHEN payment_method = 'PIX' THEN 1 ELSE 0 END) AS qt_pix,
           SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN 1 ELSE 0 END) AS qt_cartao,
           SUM(summary_total) AS val_total,
           SUM(CASE WHEN payment_method = 'PIX' THEN summary_total ELSE 0 END) AS val_pix,
           SUM(CASE WHEN payment_method = 'CREDIT_CARD' THEN summary_total ELSE 0 END) AS val_cartao
    FROM orders_2026
    GROUP BY domainId, FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM')
    HAVING COUNT(*) >= {THRESHOLD}
),
first_month AS (
    SELECT domainId, MIN(mes) AS first_mes
    FROM monthly
    GROUP BY domainId
),
ranked AS (
    SELECT domainId,
           FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM') AS mes,
           settings_createdAt_TIMESTAMP AS data_bateu,
           ROW_NUMBER() OVER (
               PARTITION BY domainId, FORMAT(settings_createdAt_TIMESTAMP, 'yyyy-MM')
               ORDER BY settings_createdAt_TIMESTAMP ASC
           ) AS rn
    FROM orders_2026
),
order_80th AS (
    SELECT domainId, mes, data_bateu
    FROM ranked
    WHERE rn = {THRESHOLD}
),
lifetime AS (
    SELECT domainId, COUNT(*) AS total_lifetime
    FROM dbo.MongoDB_Pedidos_Geral
    WHERE domainId IS NOT NULL
      AND TRY_CAST(domainId AS BIGINT) IS NOT NULL
      AND summary_total IS NOT NULL AND summary_total > 0 AND summary_total < 50000
      AND domainId IN (SELECT domainId FROM first_month)
    GROUP BY domainId
)
SELECT m.domainId, m.mes,
       m.qt_total, m.qt_pix, m.qt_cartao,
       m.val_total, m.val_pix, m.val_cartao,
       o.data_bateu,
       l.total_lifetime
FROM monthly m
JOIN first_month fm ON fm.domainId = m.domainId AND fm.first_mes = m.mes
LEFT JOIN order_80th o ON o.domainId = m.domainId AND o.mes = m.mes
LEFT JOIN lifetime   l ON l.domainId = m.domainId
ORDER BY m.mes DESC, m.qt_total DESC
"""


def load_companies() -> dict[str, dict]:
    if not COMPANIES_JSON.exists():
        print(f"ERRO: {COMPANIES_JSON} nao existe. Rode fetch_fabric.py antes.", file=sys.stderr)
        sys.exit(1)
    data = json.loads(COMPANIES_JSON.read_text(encoding="utf-8"))
    by_dom: dict[str, dict] = {}
    for c in data:
        did = str(c.get("domain_id") or "")
        if not did:
            continue
        if c.get("isMatriz"):
            by_dom[did] = c
        elif did not in by_dom:
            by_dom[did] = c
    return by_dom


def fetch_rows(conn) -> list[dict]:
    print(f"[fabric] rodando query (1o mes com {THRESHOLD}+ pedidos em 2026, "
          f"com dataBateu + totalPedidos)")
    cur = conn.cursor()
    cur.execute(SQL_TOP80)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(rows)} empresas no primeiro mes qualificado")
    return rows


def build(rows: list[dict], companies: dict[str, dict]) -> dict:
    linhas: list[dict] = []
    sem_match = 0
    meses_set: set[str] = set()
    cs_set: set[str] = set()

    for r in rows:
        dom = str(r.get("domainId") or "").strip()
        if not dom:
            continue
        try:
            dom = str(int(dom))
        except (TypeError, ValueError):
            pass
        c = companies.get(dom)
        if c is None:
            sem_match += 1
            continue
        mes = r.get("mes") or ""
        if not mes:
            continue
        cs = c.get("anjo") or ""
        data_bateu = r.get("data_bateu")
        data_bateu_str = ""
        if data_bateu is not None:
            if hasattr(data_bateu, "isoformat"):
                data_bateu_str = data_bateu.isoformat()
            else:
                data_bateu_str = str(data_bateu)
        linhas.append({
            "dominioId": dom,
            "marca": c.get("nome_fantasia") or c.get("name") or "",
            "cs": cs,
            "canal": c.get("canal") or "",
            "cnpj": c.get("cnpj") or "",
            "mes": mes,
            "dataBateu": data_bateu_str[:19] if data_bateu_str else "",
            "qtTotal": int(r.get("qt_total") or 0),
            "qtPix": int(r.get("qt_pix") or 0),
            "qtCartao": int(r.get("qt_cartao") or 0),
            "valTotal": round(float(r.get("val_total") or 0), 2),
            "valPix": round(float(r.get("val_pix") or 0), 2),
            "valCartao": round(float(r.get("val_cartao") or 0), 2),
            "totalPedidos": int(r.get("total_lifetime") or 0),
        })
        meses_set.add(mes)
        if cs:
            cs_set.add(cs)

    linhas.sort(key=lambda r: (r["mes"], -r["qtTotal"]), reverse=True)
    meses_list = sorted(meses_set)
    cs_list = sorted(cs_set, key=lambda s: s.lower())

    print(f"[build] {len(linhas)} empresas (1o mes 80+), sem match em companies: {sem_match}")
    total_valor = sum(l["valTotal"] for l in linhas)
    print(f"[build] GMV no 1o mes qualificado: R$ {total_valor:,.2f}")
    print(f"[build] Meses distintos de primeira conquista: {meses_list}")

    return {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "threshold": THRESHOLD,
        "linhas": linhas,
        "mesesList": meses_list,
        "csList": cs_list,
        "resumo": {
            "nEmpresas": len(linhas),
            "totalValor": round(total_valor, 2),
            "totalPix": round(sum(l["valPix"] for l in linhas), 2),
            "totalCartao": round(sum(l["valCartao"] for l in linhas), 2),
            "totalPedidos": sum(l["qtTotal"] for l in linhas),
            "totalPedidosLifetime": sum(l["totalPedidos"] for l in linhas),
        },
    }


def main() -> None:
    cfg = load_config()
    companies = load_companies()
    print(f"[companies] {len(companies)} dominios carregados")
    with connect(cfg) as conn:
        rows = fetch_rows(conn)
    data = build(rows, companies)
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {OUT_JSON.name}")


if __name__ == "__main__":
    main()
