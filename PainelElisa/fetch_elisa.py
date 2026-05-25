"""
PainelElisa - coleta consolidada do Fabric.

Filtra apenas marcas dos CS Elisa e Jennyfer e gera 4 JSONs:
  - companies_elisa.json   : empresas ativas (matriz+filial), com canal, plano, valor mensal
  - gmv_elisa.json         : GMV mensal e semanal por marca, separa PIX/Cartao
  - cadastros_elisa.json   : qtd produtos cadastrados + 1a venda + qtd vendas por mes
  - vestipago_elisa.json   : VP cartao/pix por marca/periodo + flag tem_vp_ativo + tem_frete_ativo

Reuso de fabric_config.json e auth (az CLI / refresh token) ja existente no
_fetch_fabric_base.py (copia local do fetch_fabric.py do PainelCSGerencial).

Rodar:  py fetch_elisa.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from _fetch_fabric_base import connect, load_config

ROOT = Path(__file__).parent
OUT_COMPANIES = ROOT / "companies_elisa.json"
OUT_GMV       = ROOT / "gmv_elisa.json"
OUT_CADASTROS = ROOT / "cadastros_elisa.json"
OUT_VP        = ROOT / "vestipago_elisa.json"

# CS alvo do painel
CS_ALVO = ("Elisa", "Jennyfer")

# Parceiros agrupados como "Starter Interno"
STARTER_INTERNO = {
    "starter", "ve vantagens", "proroi", "up", "comfio",
    "glads", "tizzefy", "sete", "zoom", "renan",
}

# Parceiros excluidos (atta / onix) na visao "Parceiros"
PARCEIROS_EXCLUIDOS = {"attasoft", "atta", "onix"}

FILTRO_DOMINIOS = """
    d.modulos LIKE '%vendas%'
    AND (d.partner_id IS NULL OR d.partner_id NOT IN (
        'ff66c2f1-1f9f-456c-9308-028e48c89582',
        '25fec57c-620c-4ecd-ae7d-cd4fee27b158'
    ))
    AND LOWER(d.name) NOT LIKE '%teste%'
"""

# -----------------------------------------------------------------------------
# 1) EMPRESAS
# -----------------------------------------------------------------------------
SQL_EMPRESAS = f"""
WITH active_domains AS (
    SELECT d.id, d.name, d.angel_id, d.integration_id, d.partner_id
    FROM dbo.ODBC_Domains d
    WHERE {FILTRO_DOMINIOS}
),
elisa_domains AS (
    SELECT ad.*
    FROM active_domains ad
    JOIN dbo.ODBC_Angels a ON a.id = ad.angel_id
    WHERE a.name IN ('Elisa Marques', 'Jennyfer Rabelo')
),
ranked_companies AS (
    SELECT
        c.domain_id, c.tax_document, c.social_name, c.company_name, c.created_at,
        ROW_NUMBER() OVER (PARTITION BY c.domain_id ORDER BY c.created_at ASC) AS rn
    FROM dbo.ODBC_Companies c
    WHERE c.domain_id IN (SELECT id FROM elisa_domains)
)
SELECT
    d.id                 AS domain_id,
    d.name               AS domain_name,
    rc.tax_document      AS cnpj,
    rc.social_name       AS razao_social,
    rc.company_name      AS company_name,
    rc.rn                AS row_num,
    rc.created_at        AS company_created_at,
    a.name               AS angel_name,
    i.name               AS integration_name,
    p.name               AS partner_name,
    sub.plan_name        AS plano,
    inv_last.total_cents AS last_invoice_cents
FROM elisa_domains d
JOIN ranked_companies rc          ON rc.domain_id = d.id
LEFT JOIN dbo.ODBC_Angels a       ON a.id = d.angel_id
LEFT JOIN dbo.ODBC_Integrations i ON i.id = d.integration_id
LEFT JOIN dbo.ODBC_Partners p     ON p.id = d.partner_id
OUTER APPLY (
    SELECT TOP 1 s.plan_name
    FROM dbo.silver_companiesativos_iugu sc
    JOIN dbo.iugu_subscriptions s ON s.customer_id = sc.Customer_ID_Iugu
    WHERE sc.domain_id = d.id
    ORDER BY
        CASE WHEN LOWER(s.active)='true' AND LOWER(s.suspended)='false' THEN 0 ELSE 1 END,
        s.updated_at DESC
) sub
OUTER APPLY (
    SELECT TOP 1 inv.total_cents
    FROM dbo.silver_companiesativos_iugu sc2
    JOIN (SELECT DISTINCT id, customer_id, total_cents, status, created_at_iso_TIMESTAMP
          FROM dbo.iugu_invoices) inv ON inv.customer_id = sc2.Customer_ID_Iugu
    WHERE sc2.domain_id = d.id AND inv.status = 'paid'
    ORDER BY inv.created_at_iso_TIMESTAMP DESC
) inv_last
"""

def _classify_canal(partner_name: str) -> tuple[str, bool]:
    """Retorna (canal_visivel, eh_starter_interno)."""
    p = (partner_name or "").strip().lower()
    if p in STARTER_INTERNO or p == "":
        # vazio = direto Vesti tambem entra como starter? Manter como "Parceiros" se nao bater.
        if p == "":
            return ("Parceiros", False)
        return ("Starter", True)
    return ("Parceiros", False)


def build_empresas(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        rn = int(r.get("row_num") or 1)
        is_filial = rn > 1
        domain_name = r.get("domain_name") or ""
        company_name = r.get("company_name") or ""
        partner = r.get("partner_name") or ""
        canal, is_starter_interno = _classify_canal(partner)

        cents = r.get("last_invoice_cents")
        try:
            valor_mensal = float(cents) / 100.0 if cents not in (None, "") else 0.0
        except (TypeError, ValueError):
            valor_mensal = 0.0

        if is_filial:
            name = company_name or f"{domain_name} - Filial {rn}"
            matriz_name = domain_name
        else:
            name = domain_name or company_name
            matriz_name = ""

        out.append({
            "domain_id": str(r.get("domain_id") or ""),
            "name": name,
            "nome_fantasia": company_name or domain_name,
            "cnpj": r.get("cnpj") or "",
            "razao_social": r.get("razao_social") or "",
            "cs": r.get("angel_name") or "",
            "canal": canal,
            "partner_raw": partner,
            "starter_interno": is_starter_interno,
            "integracao": r.get("integration_name") or "",
            "plano": r.get("plano") or "",
            "valor_mensal": round(valor_mensal, 2),
            "is_filial": is_filial,
            "isMatriz": not is_filial,
            "matriz_name": matriz_name,
            "status": "Ativa",
        })
    return out


# -----------------------------------------------------------------------------
# 2) GMV mensal + semanal
# -----------------------------------------------------------------------------
SQL_GMV = """
SELECT
    p.domainId AS domain_id,
    CAST(p.settings_createdAt_TIMESTAMP AS DATE) AS dia,
    SUM(CASE WHEN p.payment_method='PIX' THEN p.summary_total ELSE 0 END)        AS val_pix,
    SUM(CASE WHEN p.payment_method='CREDIT_CARD' THEN p.summary_total ELSE 0 END) AS val_cartao,
    SUM(p.summary_total) AS val_total,
    SUM(CASE WHEN p.payment_method='PIX' THEN 1 ELSE 0 END)        AS qt_pix,
    SUM(CASE WHEN p.payment_method='CREDIT_CARD' THEN 1 ELSE 0 END) AS qt_cartao,
    COUNT(*) AS qt_total
FROM dbo.MongoDB_Pedidos_Geral p
WHERE p.summary_total IS NOT NULL AND p.summary_total > 0 AND p.summary_total < 50000
  AND p.settings_createdAt_TIMESTAMP >= '2025-01-01'
GROUP BY p.domainId, CAST(p.settings_createdAt_TIMESTAMP AS DATE)
"""


def _semana_iso(d) -> str:
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


def build_gmv(rows: list[dict], empresas_by_dom: dict[str, dict]) -> dict:
    """Agrega por (domain, mes) e (domain, semana). Filtra apenas empresas Elisa/Jenny."""
    by_emp: dict[str, dict] = {}
    for r in rows:
        dom = str(r.get("domain_id") or "").strip()
        try:
            dom = str(int(dom))
        except (TypeError, ValueError):
            pass
        if dom not in empresas_by_dom:
            continue
        dia = r.get("dia")
        if not dia:
            continue
        mes = dia.strftime("%Y-%m")
        sem = _semana_iso(dia)
        emp = by_emp.setdefault(dom, {"mensal": {}, "semanal": {}, "primeiraVenda": None, "qtdVendasMes": {}})
        for chave, periodo in (("mensal", mes), ("semanal", sem)):
            bucket = emp[chave].setdefault(periodo, {
                "valPix": 0.0, "valCartao": 0.0, "valTotal": 0.0,
                "qtPix": 0,   "qtCartao": 0,   "qtTotal": 0,
            })
            bucket["valPix"]    += float(r.get("val_pix") or 0)
            bucket["valCartao"] += float(r.get("val_cartao") or 0)
            bucket["valTotal"]  += float(r.get("val_total") or 0)
            bucket["qtPix"]     += int(r.get("qt_pix") or 0)
            bucket["qtCartao"]  += int(r.get("qt_cartao") or 0)
            bucket["qtTotal"]   += int(r.get("qt_total") or 0)
        # primeira venda
        dia_iso = dia.isoformat()
        if not emp["primeiraVenda"] or dia_iso < emp["primeiraVenda"]:
            emp["primeiraVenda"] = dia_iso
        emp["qtdVendasMes"][mes] = emp["qtdVendasMes"].get(mes, 0) + int(r.get("qt_total") or 0)

    return {"geradoEm": datetime.now(timezone.utc).isoformat(), "empresas": by_emp}


# -----------------------------------------------------------------------------
# 3) Cadastro de produtos + 1o cadastro de pedido
# -----------------------------------------------------------------------------
SQL_PRODUTOS = """
SELECT pr.domain_id,
       FORMAT(pr.created_at, 'yyyy-MM') AS mes,
       COUNT(*) AS qt_produtos,
       MIN(pr.created_at) AS primeiro_cadastro
FROM dbo.ODBC_Products pr
WHERE pr.created_at IS NOT NULL
GROUP BY pr.domain_id, FORMAT(pr.created_at, 'yyyy-MM')
"""

SQL_PRIMEIRO_PEDIDO = """
SELECT p.domainId AS domain_id, MIN(p.settings_createdAt_TIMESTAMP) AS primeiro_pedido_cadastrado
FROM dbo.MongoDB_Pedidos_Geral p
GROUP BY p.domainId
"""


def build_cadastros(prods: list[dict], primeiros: list[dict],
                    empresas_by_dom: dict[str, dict]) -> dict:
    out: dict[str, dict] = {}
    # prods agora vem agregado por (domain, mes); somar total + por mes
    for r in prods:
        dom = str(r.get("domain_id") or "").strip()
        if dom not in empresas_by_dom:
            continue
        mes = r.get("mes") or ""
        qt = int(r.get("qt_produtos") or 0)
        pc = r.get("primeiro_cadastro")
        slot = out.setdefault(dom, {"qtProdutos": 0, "produtosPorMes": {}, "primeiroCadastroProduto": ""})
        slot["qtProdutos"] += qt
        if mes:
            slot["produtosPorMes"][mes] = slot["produtosPorMes"].get(mes, 0) + qt
        if pc:
            iso = pc.isoformat() if hasattr(pc, "isoformat") else str(pc)
            if not slot["primeiroCadastroProduto"] or iso < slot["primeiroCadastroProduto"]:
                slot["primeiroCadastroProduto"] = iso
    # primeiro mes + qtd no 1o mes
    for dom, slot in out.items():
        meses = sorted(slot["produtosPorMes"].keys())
        if meses:
            slot["primeiroMes"] = meses[0]
            slot["qtProdutos1oMes"] = slot["produtosPorMes"][meses[0]]
        else:
            slot["primeiroMes"] = ""
            slot["qtProdutos1oMes"] = 0
    for r in primeiros:
        dom = str(r.get("domain_id") or "").strip()
        try: dom = str(int(dom))
        except (TypeError, ValueError): pass
        if dom not in empresas_by_dom:
            continue
        pp = r.get("primeiro_pedido_cadastrado")
        out.setdefault(dom, {})["primeiroPedidoCadastrado"] = pp.isoformat() if hasattr(pp, "isoformat") else (str(pp) if pp else "")
    return out


# -----------------------------------------------------------------------------
# 4) VestiPago (status ativo + valores por periodo)
# Reaproveita GMV ja agregado; flag de ativo: tem >=1 pedido VP nos ultimos 30 dias
# -----------------------------------------------------------------------------
def build_vp(empresas_by_dom: dict[str, dict], gmv: dict) -> dict:
    hoje = datetime.now(timezone.utc).date()
    mes_atual = hoje.strftime("%Y-%m")
    mes_anterior_y, mes_anterior_m = (hoje.year, hoje.month - 1) if hoje.month > 1 else (hoje.year - 1, 12)
    mes_anterior = f"{mes_anterior_y:04d}-{mes_anterior_m:02d}"
    janela = {mes_atual, mes_anterior}

    vp: dict[str, dict] = {}
    for dom, emp in empresas_by_dom.items():
        bucket_gmv = gmv["empresas"].get(dom, {}).get("mensal", {})
        tem_vp = any(
            (bucket_gmv.get(m, {}).get("qtPix", 0) + bucket_gmv.get(m, {}).get("qtCartao", 0)) > 0
            for m in janela
        )
        # frete: assumimos ativo se tem pedidos no periodo (proxy). Substituir por
        # query real em mongodb_companies_logistics quando schema confirmado.
        tem_frete = any(bucket_gmv.get(m, {}).get("qtTotal", 0) > 0 for m in janela)
        vp[dom] = {"temVPAtivo": tem_vp, "temFreteAtivo": tem_frete}
    return vp


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def run_query(conn, sql: str, label: str) -> list[dict]:
    print(f"[fabric] {label} ...", flush=True)
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {label}: {len(rows)} linhas", flush=True)
    return rows


def main() -> None:
    cfg = load_config()
    with connect(cfg) as conn:
        emp_rows  = run_query(conn, SQL_EMPRESAS, "empresas Elisa/Jenny")
        gmv_rows  = run_query(conn, SQL_GMV, "GMV diario")
        prod_rows = run_query(conn, SQL_PRODUTOS, "qtd produtos por dominio")
        pp_rows   = run_query(conn, SQL_PRIMEIRO_PEDIDO, "primeiro pedido cadastrado")

    empresas = build_empresas(emp_rows)
    OUT_COMPANIES.write_text(json.dumps(empresas, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT_COMPANIES.name} ({len(empresas)} linhas)")

    empresas_by_dom = {e["domain_id"]: e for e in empresas if e["isMatriz"]}
    # filiais somam no dominio da matriz tambem
    for e in empresas:
        empresas_by_dom.setdefault(e["domain_id"], e)

    gmv = build_gmv(gmv_rows, empresas_by_dom)
    OUT_GMV.write_text(json.dumps(gmv, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {OUT_GMV.name} ({len(gmv['empresas'])} dominios)")

    cad = build_cadastros(prod_rows, pp_rows, empresas_by_dom)
    OUT_CADASTROS.write_text(json.dumps(cad, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT_CADASTROS.name} ({len(cad)} dominios)")

    vp = build_vp(empresas_by_dom, gmv)
    OUT_VP.write_text(json.dumps(vp, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT_VP.name} ({len(vp)} dominios)")
    print("[ok] coleta concluida. Rode build_data.py em seguida.")


if __name__ == "__main__":
    main()
