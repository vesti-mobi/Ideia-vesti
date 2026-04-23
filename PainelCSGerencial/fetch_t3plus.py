"""
T3+ = receita de mensalidade (iugu_invoices pagas) dos clientes cujo Partner
NAO seja Starter (nem Trial/Treino). Logica espelha o painel CS no Fabric
(Vestilake > Paineis > Painel CS / aba Faturamento).

Agrega por mes (created_at_iso_TIMESTAMP) e por empresa.
Saida: t3plus_data.json

Rodar:
    py fetch_t3plus.py
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from fetch_fabric import connect, load_config

ROOT = Path(__file__).parent
OUT_JSON = ROOT / "t3plus_data.json"

# Partners excluidos do T3+:
#   Starter = cliente starter (objetivo da feature)
#   Trial / Treino = ja eram excluidos na base ativa
STARTER_ID = "c2cda592-cd9f-4380-96df-316a51bfc6fb"
TRIAL_ID = "25fec57c-620c-4ecd-ae7d-cd4fee27b158"
TREINO_ID = "ff66c2f1-1f9f-456c-9308-028e48c89582"

SQL = f"""
WITH t3_domains AS (
    SELECT d.id, d.name, d.partner_id, d.angel_id
    FROM dbo.ODBC_Domains d
    WHERE d.modulos LIKE '%vendas%'
      AND LOWER(d.name) NOT LIKE '%teste%'
      AND (d.partner_id IS NULL OR d.partner_id NOT IN (
          '{STARTER_ID}', '{TRIAL_ID}', '{TREINO_ID}'
      ))
),
ranked_companies AS (
    SELECT
        c.domain_id,
        c.company_name,
        c.social_name,
        ROW_NUMBER() OVER (PARTITION BY c.domain_id ORDER BY c.created_at ASC) AS rn
    FROM dbo.ODBC_Companies c
    WHERE c.domain_id IN (SELECT id FROM t3_domains)
)
SELECT
    d.id                         AS domain_id,
    d.name                       AS domain_name,
    rc.company_name              AS company_name,
    rc.social_name               AS social_name,
    p.name                       AS partner_name,
    a.name                       AS angel_name,
    inv.id                       AS invoice_id,
    inv.total_paid_cents         AS total_paid_cents,
    inv.total_cents              AS total_cents,
    inv.status                   AS status,
    inv.created_at_iso_TIMESTAMP AS invoice_date
FROM t3_domains d
JOIN ranked_companies rc       ON rc.domain_id = d.id AND rc.rn = 1
LEFT JOIN dbo.ODBC_Partners p  ON p.id = d.partner_id
LEFT JOIN dbo.ODBC_Angels   a  ON a.id = d.angel_id
JOIN dbo.silver_companiesativos_iugu sc ON sc.domain_id = d.id
JOIN (
    SELECT DISTINCT id, customer_id, total_cents, total_paid_cents,
           status, created_at_iso_TIMESTAMP
    FROM dbo.iugu_invoices
    WHERE status = 'paid'
) inv ON inv.customer_id = sc.Customer_ID_Iugu
WHERE inv.created_at_iso_TIMESTAMP >= '2025-01-01'
"""


def fetch_rows(conn) -> list[dict]:
    print("[fabric] rodando query T3+ (invoices pagas de dominios nao-Starter)")
    cur = conn.cursor()
    cur.execute(SQL)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(rows)} invoices T3+ retornados")
    return rows


def _cents_to_reais(c) -> float:
    if c is None:
        return 0.0
    try:
        return float(c) / 100.0
    except (TypeError, ValueError):
        return 0.0


def _ym(dt) -> str:
    if dt is None:
        return ""
    if hasattr(dt, "isoformat"):
        s = dt.isoformat()
    else:
        s = str(dt)
    return s[:7]  # YYYY-MM


def build(rows: list[dict]) -> dict:
    # Agregados
    por_mes: dict[str, float] = defaultdict(float)
    por_mes_qtd: dict[str, int] = defaultdict(int)
    por_mes_dominios: dict[str, set] = defaultdict(set)
    empresas: dict[str, dict] = {}  # domain_id -> {nome, partner, ..., por_mes}

    for r in rows:
        dom = str(r.get("domain_id") or "").strip()
        if not dom:
            continue
        val = _cents_to_reais(r.get("total_paid_cents") or r.get("total_cents"))
        if val <= 0:
            continue
        mes = _ym(r.get("invoice_date"))
        if not mes:
            continue
        por_mes[mes] += val
        por_mes_qtd[mes] += 1
        por_mes_dominios[mes].add(dom)

        emp = empresas.setdefault(dom, {
            "domainId": dom,
            "empresa": r.get("company_name") or r.get("social_name") or r.get("domain_name") or "",
            "canal": r.get("partner_name") or "",
            "cs": r.get("angel_name") or "",
            "total": 0.0,
            "nInvoices": 0,
            "porMes": defaultdict(float),
        })
        emp["total"] += val
        emp["nInvoices"] += 1
        emp["porMes"][mes] += val

    meses = sorted(por_mes.keys())
    serie = [
        {
            "mes": m,
            "receita": round(por_mes[m], 2),
            "nInvoices": por_mes_qtd[m],
            "nEmpresas": len(por_mes_dominios[m]),
        }
        for m in meses
    ]

    empresas_list = []
    for dom, e in empresas.items():
        empresas_list.append({
            "domainId": e["domainId"],
            "empresa": e["empresa"],
            "canal": e["canal"],
            "cs": e["cs"],
            "total": round(e["total"], 2),
            "nInvoices": e["nInvoices"],
            "porMes": {m: round(v, 2) for m, v in sorted(e["porMes"].items())},
        })
    empresas_list.sort(key=lambda x: x["total"], reverse=True)

    total_geral = round(sum(por_mes.values()), 2)
    ultimo = serie[-1] if serie else None
    anterior = serie[-2] if len(serie) >= 2 else None

    print(f"[build] {len(empresas_list)} empresas T3+ | {len(rows)} invoices | total R$ {total_geral:,.2f}")
    if ultimo:
        print(f"[build] ultimo mes ({ultimo['mes']}): R$ {ultimo['receita']:,.2f} ({ultimo['nEmpresas']} empresas)")

    return {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "serie": serie,
        "empresas": empresas_list,
        "resumo": {
            "totalGeral": total_geral,
            "nEmpresas": len(empresas_list),
            "nInvoices": len(rows),
            "mesAtual": ultimo["mes"] if ultimo else "",
            "receitaMesAtual": ultimo["receita"] if ultimo else 0.0,
            "receitaMesAnterior": anterior["receita"] if anterior else 0.0,
        },
    }


def main() -> None:
    cfg = load_config()
    with connect(cfg) as conn:
        rows = fetch_rows(conn)
    data = build(rows)
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {OUT_JSON.name}")


if __name__ == "__main__":
    main()
