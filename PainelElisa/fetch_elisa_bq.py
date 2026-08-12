"""
PainelElisa - coleta consolidada do BigQuery (migracao Fabric -> BQ).

Substitui fetch_elisa.py (pyodbc/Fabric) puxando de vesti-data-499015.vestilake_BI.
Gera os mesmos JSONs, com as MESMAS build_* (copiadas de fetch_elisa.py p/ nao
depender de pyodbc no runner). Decisoes da migracao em _MIGRACAO_BQ_STATUS.md:
  - BQ e a fonte da verdade (opcao B). Piso de dados = jun/2025 (sem backfill).
  - Cliques + Reativacao usam o valor CORRETO do BQ (o antigo Fabric inflava:
    reativacao ~11x por iugu_invoices explodido; cliques o BQ e' maior por cobrir
    mais snapshots diarios da rankings, SUM diario e' a agregacao certa).
  - Produtos: BLOQUEADO ate ingerir `odbc_products` no BQ. `odbc_product_details`
    e' grao de variante (1-47x/loja), nao serve. Enquanto nao existir, qtProdutos
    fica vazio (guarda _table_exists) e o resto do painel atualiza normal.

Rodar:  py fetch_elisa_bq.py
Credencial: GOOGLE_APPLICATION_CREDENTIALS (SA key). Fallback = caminho local.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

ROOT = Path(__file__).parent
OUT_COMPANIES = ROOT / "companies_elisa.json"
OUT_GMV       = ROOT / "gmv_elisa.json"
OUT_CADASTROS = ROOT / "cadastros_elisa.json"
OUT_VP        = ROOT / "vestipago_elisa.json"
OUT_REATIV    = ROOT / "reativacao_elisa.json"
OUT_LINKS     = ROOT / "links_elisa.json"

PROJECT = "vesti-data-499015"
DATASET = "vestilake_BI"
DS = f"`{PROJECT}.{DATASET}`"

# Piso de dados da migracao (sem backfill anterior).
PISO = "2025-06-01"

# Fallback local da SA key se GOOGLE_APPLICATION_CREDENTIALS nao vier setado.
_SA_FALLBACK = r"C:\Users\Laura\Downloads\vesti-data-499015-7ea468dae45e.json"

# Parceiros agrupados como "Starter Interno"
STARTER_INTERNO = {
    "starter", "ve vantagens", "proroi", "up", "comfio",
    "glads", "tizzefy", "sete", "zoom", "renan",
}

# -----------------------------------------------------------------------------
# 1) EMPRESAS (dedup partners e' OBRIGATORIO: odbc_partners vem 2x no BQ)
# -----------------------------------------------------------------------------
SQL_EMPRESAS = f"""
WITH partners AS (
  SELECT id, name FROM (
    SELECT id, name, ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rn
    FROM `{PROJECT}.{DATASET}.odbc_partners`) WHERE rn = 1),
active_domains AS (
  SELECT d.ID id, d.name, d.angel_id, d.integration_id, d.partner_id, d.modulos, d.created_at
  FROM `{PROJECT}.{DATASET}.odbc_domains` d
  WHERE LOWER(d.modulos) LIKE '%vendas%'
    AND (d.partner_id IS NULL OR d.partner_id NOT IN (
      'ff66c2f1-1f9f-456c-9308-028e48c89582', '25fec57c-620c-4ecd-ae7d-cd4fee27b158'))
    AND LOWER(d.name) NOT LIKE '%teste%'),
atta_domains AS (
  SELECT d.ID id, d.name, d.angel_id, d.integration_id, d.partner_id, d.modulos, d.created_at
  FROM `{PROJECT}.{DATASET}.odbc_domains` d
  JOIN partners p ON p.id = d.partner_id
  WHERE LOWER(d.modulos) LIKE '%vendas%' AND LOWER(d.name) NOT LIKE '%teste%'
    AND LOWER(p.name) IN ('atta', 'attasoft')),
elisa_domains AS (
  -- TODAS as CS (antes era restrito a Elisa Marques / Jennyfer Rabelo).
  -- LEFT JOIN em angels p/ nao perder dominio sem CS atribuida.
  SELECT ad.id, ad.name, ad.angel_id, ad.integration_id, ad.partner_id, ad.modulos,
         ad.created_at domain_created_at
  FROM active_domains ad
  UNION DISTINCT
  SELECT atd.id, atd.name, atd.angel_id, atd.integration_id, atd.partner_id, atd.modulos,
         atd.created_at
  FROM atta_domains atd),
ranked_companies AS (
  SELECT c.domain_id, c.tax_document, c.social_name, c.company_name, c.created_at,
    ROW_NUMBER() OVER(PARTITION BY c.domain_id ORDER BY c.created_at ASC) rn
  FROM `{PROJECT}.{DATASET}.odbc_companies` c
  WHERE c.domain_id IN (SELECT id FROM elisa_domains)),
sub_best AS (SELECT domain_id, plan_name, price_cents FROM (
    SELECT sc.domain_id, s.plan_name, SAFE_CAST(s.price_cents AS INT64) price_cents,
      ROW_NUMBER() OVER(PARTITION BY sc.domain_id ORDER BY
      CASE WHEN LOWER(s.active)='true' AND LOWER(s.suspended)='false' THEN 0 ELSE 1 END,
      s.updated_at DESC) rn
    FROM `{PROJECT}.{DATASET}.silver_companiesativos_iugu` sc
    JOIN `{PROJECT}.{DATASET}.iugu_subscriptions` s ON s.customer_id = sc.Customer_ID_Iugu) WHERE rn=1),
inv_best AS (SELECT domain_id, total_cents FROM (
    SELECT sc.domain_id, inv.total_cents,
      ROW_NUMBER() OVER(PARTITION BY sc.domain_id ORDER BY inv.created_at_iso DESC) rn
    FROM `{PROJECT}.{DATASET}.silver_companiesativos_iugu` sc
    JOIN (SELECT DISTINCT id, customer_id, total_cents, status, created_at_iso
          FROM `{PROJECT}.{DATASET}.iugu_invoices` WHERE status='paid') inv
      ON inv.customer_id = sc.Customer_ID_Iugu) WHERE rn=1)
SELECT d.id domain_id, d.name domain_name, d.domain_created_at,
  rc.tax_document cnpj, rc.social_name razao_social, rc.company_name,
  rc.rn row_num, rc.created_at company_created_at, a.name angel_name,
  i.name integration_name, p.name partner_name, sub_best.plan_name plano,
  sub_best.price_cents plano_price_cents,
  inv_best.total_cents last_invoice_cents, d.modulos
FROM elisa_domains d
JOIN ranked_companies rc ON rc.domain_id = d.id
LEFT JOIN `{PROJECT}.{DATASET}.odbc_angels` a ON a.id = d.angel_id
LEFT JOIN `{PROJECT}.{DATASET}.odbc_integrations` i ON i.id = SAFE_CAST(d.integration_id AS INT64)
LEFT JOIN partners p ON p.id = d.partner_id
LEFT JOIN sub_best ON sub_best.domain_id = d.id
LEFT JOIN inv_best ON inv_best.domain_id = d.id
"""

# -----------------------------------------------------------------------------
# 2) GMV diario (agregacao mensal/semanal em Python, igual build_gmv)
# -----------------------------------------------------------------------------
SQL_GMV = f"""
SELECT domainId domain_id, DATE(CAST(settings_createdAt AS TIMESTAMP)) dia,
  SUM(CASE WHEN payment_method='PIX' THEN CAST(summary_total AS FLOAT64) ELSE 0 END) val_pix,
  SUM(CASE WHEN payment_method='CREDIT_CARD' THEN CAST(summary_total AS FLOAT64) ELSE 0 END) val_cartao,
  SUM(CAST(summary_total AS FLOAT64)) val_total,
  SUM(CASE WHEN payment_method='PIX' THEN 1 ELSE 0 END) qt_pix,
  SUM(CASE WHEN payment_method='CREDIT_CARD' THEN 1 ELSE 0 END) qt_cartao,
  COUNT(*) qt_total,
  SUM(CASE WHEN payment_paidAt IS NOT NULL AND payment_paidAt<>'' THEN 1 ELSE 0 END) qt_paid
FROM `{PROJECT}.{DATASET}.MongoDB_Pedidos_Geral`
WHERE SAFE_CAST(summary_total AS FLOAT64) > 0 AND SAFE_CAST(summary_total AS FLOAT64) < 50000
  AND CAST(settings_createdAt AS TIMESTAMP) >= '{PISO}'
GROUP BY domainId, DATE(CAST(settings_createdAt AS TIMESTAMP))
"""

# -----------------------------------------------------------------------------
# 3a) PRODUTOS (BLOQUEADO): so roda se `odbc_products` existir no BQ.
#     odbc_product_details NAO serve (grao de variante). Ver _MIGRACAO_BQ_STATUS.md.
# -----------------------------------------------------------------------------
SQL_PRODUTOS = f"""
SELECT domain_id, FORMAT_DATETIME('%Y-%m', CAST(created_at AS DATETIME)) mes,
  COUNT(*) qt_produtos, MIN(CAST(created_at AS DATETIME)) primeiro_cadastro
FROM `{PROJECT}.{DATASET}.odbc_products`
WHERE created_at IS NOT NULL AND CAST(created_at AS DATETIME) >= '{PISO}'
GROUP BY 1, 2
"""

# 3b) PRIMEIRO PEDIDO (MongoDB_Pedidos_Geral existe no BQ)
SQL_PRIMEIRO_PEDIDO = f"""
SELECT domainId domain_id, MIN(CAST(settings_createdAt AS TIMESTAMP)) primeiro_pedido_cadastrado
FROM `{PROJECT}.{DATASET}.MongoDB_Pedidos_Geral`
GROUP BY domainId
"""

# -----------------------------------------------------------------------------
# 4) LINKS + CLIQUES (rankings e' snapshot diario -> SUM diario e' correto)
# -----------------------------------------------------------------------------
SQL_LINKS = f"""
WITH links AS (
  SELECT u.DomainId domain_id,
         FORMAT_TIMESTAMP('%Y-%m', p.product_sent_lists_created_at) mes,
         COUNT(DISTINCT p.product_sent_lists_id) links
  FROM `{PROJECT}.{DATASET}.sucessodocliente_products` p
  JOIN `{PROJECT}.{DATASET}.sucessodocliente_cadastrouser` u ON u.UserId = p.USERS_ID
  WHERE p.product_sent_lists_created_at >= '{PISO}'
    AND p.product_sent_lists_created_at < '2100-01-01'
  GROUP BY 1, 2),
cliques AS (
  SELECT u.DomainId domain_id,
         FORMAT_TIMESTAMP('%Y-%m', r.rankings_created_at) mes,
         SUM(SAFE_CAST(r.rankings_shared_links AS INT64)) cliques
  FROM `{PROJECT}.{DATASET}.sucessodocliente_rankings` r
  JOIN `{PROJECT}.{DATASET}.sucessodocliente_cadastrouser` u ON u.UserId = r.USERS_ID
  WHERE r.rankings_created_at >= '{PISO}'
  GROUP BY 1, 2)
SELECT COALESCE(l.domain_id, c.domain_id) domain_id,
       COALESCE(l.mes, c.mes) mes,
       COALESCE(l.links, 0) links,
       COALESCE(c.cliques, 0) cliques
FROM links l
FULL OUTER JOIN cliques c ON c.domain_id = l.domain_id AND c.mes = l.mes
WHERE COALESCE(l.links, 0) + COALESCE(c.cliques, 0) > 0
"""

# -----------------------------------------------------------------------------
# 5) REATIVACAO = a marca ficou um ciclo inteiro sem pagar e voltou.
#
#    NAO usa mais `paid_at > due_date` (fatura paga em atraso). Aquele criterio
#    media so' "pagou com atraso": 70% eram de 1 a 3 dias e o maior atraso do
#    banco inteiro era 30 dias -- a Iugu cancela a fatura antes disso (2.691
#    canceladas), entao inadimplencia longa NUNCA aparecia ali.
#
#    O criterio certo e' o INTERVALO entre uma fatura paga e a seguinte do mesmo
#    customer. Gap > 45 dias = pulou pelo menos um ciclo. Decidido com a Laura
#    em 12/08/2026.
#
#    DISTINCT id continua obrigatorio: iugu_invoices vem explodido no BQ.
# -----------------------------------------------------------------------------
GAP_MIN_DIAS = 46

SQL_REATIVACAO = f"""
WITH pagas AS (
  SELECT DISTINCT id, customer_id, DATE(SUBSTR(paid_at, 1, 10)) paid_dt
  FROM `{PROJECT}.{DATASET}.iugu_invoices`
  WHERE status='paid' AND paid_at IS NOT NULL AND paid_at NOT IN ('None','')),
com_gap AS (
  SELECT customer_id, paid_dt,
         LAG(paid_dt) OVER(PARTITION BY customer_id ORDER BY paid_dt) pag_anterior
  FROM pagas)
SELECT sc.domain_id domain_id,
  FORMAT_DATE('%Y-%m', g.paid_dt) mes_pago,
  g.pag_anterior ultimo_pagamento,
  g.paid_dt data_volta,
  DATE_DIFF(g.paid_dt, g.pag_anterior, DAY) dias_sem_pagar
FROM com_gap g
JOIN `{PROJECT}.{DATASET}.silver_companiesativos_iugu` sc ON sc.Customer_ID_Iugu = g.customer_id
WHERE g.pag_anterior IS NOT NULL
  AND DATE_DIFF(g.paid_dt, g.pag_anterior, DAY) >= {GAP_MIN_DIAS}
"""


# =============================================================================
# build_* (copiadas de fetch_elisa.py -- puras, sem dependencia de pyodbc)
# =============================================================================
# Parceiros que sao a propria Vesti (canal proprio, nao e' parceiro externo)
VESTI_PROPRIO = {"vesti", "varejo vesti"}


def _classify_canal(partner_name: str) -> tuple[str, bool]:
    """Retorna (canal, starter_interno). O front recalcula o canal a partir de
    partner_raw (canalDe), entao aqui basta manter Starter/Vesti/Parceiros."""
    p = (partner_name or "").strip().lower()
    if not p or p == "n/a":
        return ("Parceiros", False)
    if p in VESTI_PROPRIO:
        return ("Vesti", False)
    # match tolerante: a base tem "Tizeefy" e "Up Agency" alem dos nomes canonicos
    if any(p == s or p.startswith(s + " ") for s in STARTER_INTERNO) or p in ("tizeefy",):
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

        def _reais(v):
            try:
                return round(float(v) / 100.0, 2) if v not in (None, "") else 0.0
            except (TypeError, ValueError):
                return 0.0

        # valor_mensal = ultima fatura PAGA (pode ser proporcional/parcial).
        # valor_plano  = price_cents da assinatura Iugu = mensalidade contratada.
        # Para decidir upgrade o valor_plano e' o numero certo; mantemos os dois.
        valor_mensal = _reais(r.get("last_invoice_cents"))
        valor_plano = _reais(r.get("plano_price_cents"))

        if is_filial:
            name = company_name or f"{domain_name} - Filial {rn}"
            matriz_name = domain_name
        else:
            name = domain_name or company_name
            matriz_name = ""

        dca = r.get("domain_created_at")
        data_entrada = dca.isoformat() if hasattr(dca, "isoformat") else (str(dca) if dca else "")
        out.append({
            "domain_id": str(r.get("domain_id") or ""),
            "name": name,
            "dataEntrada": data_entrada,
            "nome_fantasia": company_name or domain_name,
            "cnpj": r.get("cnpj") or "",
            "razao_social": r.get("razao_social") or "",
            "cs": r.get("angel_name") or "",
            "canal": canal,
            "partner_raw": partner,
            "starter_interno": is_starter_interno,
            "integracao": r.get("integration_name") or "",
            "plano": r.get("plano") or "",
            "valor_mensal": valor_mensal,
            "valor_plano": valor_plano,
            "modulos": (r.get("modulos") or ""),
            "is_filial": is_filial,
            "isMatriz": not is_filial,
            "matriz_name": matriz_name,
            "status": "Ativa",
        })
    return out


def _semana_iso(d) -> str:
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


def build_gmv(rows: list[dict], empresas_by_dom: dict[str, dict]) -> dict:
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
        emp = by_emp.setdefault(dom, {"mensal": {}, "semanal": {}, "primeiraVenda": None,
                                       "primeiraVendaPaga": None,
                                       "qtdVendasMes": {}, "qtdVendasPagasMes": {},
                                       "vendasPagasPorDia": {}})
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
        dia_iso = dia.isoformat()
        if not emp["primeiraVenda"] or dia_iso < emp["primeiraVenda"]:
            emp["primeiraVenda"] = dia_iso
        emp["qtdVendasMes"][mes] = emp["qtdVendasMes"].get(mes, 0) + int(r.get("qt_total") or 0)
        qt_paid = int(r.get("qt_paid") or 0)
        if qt_paid:
            emp["qtdVendasPagasMes"][mes] = emp["qtdVendasPagasMes"].get(mes, 0) + qt_paid
            # grao diario: build_data.py usa p/ achar a DATA exata das 5a / 25a venda
            emp["vendasPagasPorDia"][dia_iso] = emp["vendasPagasPorDia"].get(dia_iso, 0) + qt_paid
            if not emp["primeiraVendaPaga"] or dia_iso < emp["primeiraVendaPaga"]:
                emp["primeiraVendaPaga"] = dia_iso

    return {"geradoEm": datetime.now(timezone.utc).isoformat(), "empresas": by_emp}


def build_cadastros(prods: list[dict], primeiros: list[dict],
                    empresas_by_dom: dict[str, dict]) -> dict:
    out: dict[str, dict] = {}
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


def build_vp(empresas_by_dom: dict[str, dict], gmv: dict) -> dict:
    hoje = datetime.now(timezone.utc).date()
    mes_atual = hoje.strftime("%Y-%m")
    mes_anterior_y, mes_anterior_m = (hoje.year, hoje.month - 1) if hoje.month > 1 else (hoje.year - 1, 12)
    mes_anterior = f"{mes_anterior_y:04d}-{mes_anterior_m:02d}"
    janela = {mes_atual, mes_anterior}

    vp: dict[str, dict] = {}
    for dom, emp in empresas_by_dom.items():
        bucket_gmv = gmv["empresas"].get(dom, {}).get("mensal", {})
        tem_pix = any(bucket_gmv.get(m, {}).get("qtPix", 0) > 0 for m in janela)
        tem_cartao = any(bucket_gmv.get(m, {}).get("qtCartao", 0) > 0 for m in janela)
        tem_vp = tem_pix or tem_cartao
        tem_frete = any(bucket_gmv.get(m, {}).get("qtTotal", 0) > 0 for m in janela)
        vp[dom] = {
            "temVPAtivo": tem_vp,
            "temPixAtivo": tem_pix,
            "temCartaoAtivo": tem_cartao,
            "temFreteAtivo": tem_frete,
        }
    return vp


def build_links(rows: list[dict], empresas_by_dom: dict[str, dict]) -> dict:
    out: dict[str, dict] = {}
    for r in rows:
        dom = str(r.get("domain_id") or "").strip()
        try: dom = str(int(dom))
        except (TypeError, ValueError): pass
        if dom not in empresas_by_dom:
            continue
        links = int(r.get("links") or 0)
        cliq  = int(r.get("cliques") or 0)
        mes   = r.get("mes") or ""
        slot = out.setdefault(dom, {"linksCompartilhados": 0, "cliquesTotal": 0,
                                     "cliquesPorMes": {}, "linksPorMes": {},
                                     "influenciadores": []})
        slot["linksCompartilhados"] += links
        slot["cliquesTotal"]        += cliq
        if mes:
            if links: slot["linksPorMes"][mes]   = slot["linksPorMes"].get(mes, 0) + links
            if cliq:  slot["cliquesPorMes"][mes] = slot["cliquesPorMes"].get(mes, 0) + cliq
    return out


def build_reativacao(rows: list[dict], empresas_by_dom: dict[str, dict]) -> dict:
    """Um evento por RETORNO: a marca passou `dias` sem pagar e voltou em `voltou`."""
    def _iso(v):
        return v.isoformat() if hasattr(v, "isoformat") else (str(v)[:10] if v else "")

    out: dict[str, dict] = {}
    for r in rows:
        dom = str(r.get("domain_id") or "").strip()
        if dom not in empresas_by_dom:
            continue
        mes = r.get("mes_pago") or ""
        dias = int(r.get("dias_sem_pagar") or 0)
        slot = out.setdefault(dom, {"reativacoesPorMes": {}, "diasAusenteTotal": 0,
                                     "totalReativ": 0, "eventos": []})
        if mes:
            slot["reativacoesPorMes"][mes] = slot["reativacoesPorMes"].get(mes, 0) + 1
        slot["diasAusenteTotal"] += dias
        slot["totalReativ"] += 1
        slot["eventos"].append({"mes": mes, "ultimoPag": _iso(r.get("ultimo_pagamento")),
                                "voltou": _iso(r.get("data_volta")), "dias": dias})
    for slot in out.values():
        slot["eventos"].sort(key=lambda ev: ev["voltou"], reverse=True)
        slot["maiorAusencia"] = max((ev["dias"] for ev in slot["eventos"]), default=0)
        slot["ultimaVolta"] = slot["eventos"][0]["voltou"] if slot["eventos"] else ""
    return out


# =============================================================================
# BigQuery
# =============================================================================
def run_query(client: bigquery.Client, sql: str, label: str) -> list[dict]:
    print(f"[bq] {label} ...", flush=True)
    rows = [dict(r) for r in client.query(sql).result()]
    print(f"[bq] {label}: {len(rows)} linhas", flush=True)
    return rows


def _table_exists(client: bigquery.Client, name: str) -> bool:
    try:
        client.get_table(f"{PROJECT}.{DATASET}.{name}")
        return True
    except NotFound:
        return False


# Campos de Produtos (paridade de schema com o painel antigo).
_PRODUTOS_KEYS = ("qtProdutos", "produtosPorMes", "primeiroCadastroProduto",
                  "primeiroMes", "qtProdutos1oMes")


def _garante_campos_produtos(cad: dict) -> None:
    """Garante que todo dominio tenha os 5 campos de Produtos (mesmos do antigo)."""
    for slot in cad.values():
        slot.setdefault("qtProdutos", 0)
        slot.setdefault("produtosPorMes", {})
        slot.setdefault("primeiroCadastroProduto", "")
        slot.setdefault("primeiroMes", "")
        slot.setdefault("qtProdutos1oMes", 0)


def _aplicar_produtos_fallback(cad: dict, empresas_by_dom: dict) -> None:
    """Enquanto `odbc_products` nao existe no BQ, preserva os valores de Produtos
    do cadastros_elisa.json anterior (evita zerar o painel). Some sozinho quando a
    tabela for ingerida. Le o arquivo ANTES de sobrescrever."""
    if not OUT_CADASTROS.exists():
        return
    try:
        prev = json.loads(OUT_CADASTROS.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        print("[bq] AVISO: cadastros_elisa.json anterior ilegivel; Produtos ficara vazio.",
              file=sys.stderr, flush=True)
        return
    carregados = 0
    for dom, pv in prev.items():
        if not isinstance(pv, dict) or dom not in empresas_by_dom:
            continue
        anterior = {k: pv[k] for k in _PRODUTOS_KEYS if k in pv}
        if not anterior:
            continue
        cad.setdefault(dom, {}).update(anterior)
        carregados += 1
    print(f"[bq] Produtos: fallback do arquivo anterior aplicado a {carregados} dominios "
          f"(ate ingerir odbc_products).", flush=True)


def coletar_do_bq(client: bigquery.Client):
    emp_rows    = run_query(client, SQL_EMPRESAS, "empresas (todas as CS)")
    gmv_rows    = run_query(client, SQL_GMV, "GMV diario")
    pp_rows     = run_query(client, SQL_PRIMEIRO_PEDIDO, "primeiro pedido cadastrado")
    reativ_rows = run_query(client, SQL_REATIVACAO, "reativacoes (45+ dias sem pagar)")
    links_rows  = run_query(client, SQL_LINKS, "links/cliques compartilhados")

    # Produtos: so se `odbc_products` estiver ingerido (odbc_product_details NAO serve).
    if _table_exists(client, "odbc_products"):
        prod_rows = run_query(client, SQL_PRODUTOS, "qtd produtos por dominio")
    else:
        prod_rows = []
        print("[bq] AVISO: tabela `odbc_products` nao existe no BQ -> Produtos fica "
              "VAZIO (qtProdutos). Ingerir dbo.ODBC_Products e rodar de novo. "
              "Ver _MIGRACAO_BQ_STATUS.md.", file=sys.stderr, flush=True)

    return emp_rows, gmv_rows, prod_rows, pp_rows, reativ_rows, links_rows


def main() -> None:
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists(_SA_FALLBACK):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _SA_FALLBACK
    client = bigquery.Client(project=PROJECT)

    prod_disponivel = _table_exists(client, "odbc_products")
    emp_rows, gmv_rows, prod_rows, pp_rows, reativ_rows, links_rows = coletar_do_bq(client)

    empresas = build_empresas(emp_rows)
    OUT_COMPANIES.write_text(json.dumps(empresas, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT_COMPANIES.name} ({len(empresas)} linhas)")

    empresas_by_dom = {e["domain_id"]: e for e in empresas if e["isMatriz"]}
    for e in empresas:
        empresas_by_dom.setdefault(e["domain_id"], e)

    gmv = build_gmv(gmv_rows, empresas_by_dom)
    OUT_GMV.write_text(json.dumps(gmv, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {OUT_GMV.name} ({len(gmv['empresas'])} dominios)")

    cad = build_cadastros(prod_rows, pp_rows, empresas_by_dom)
    if not prod_disponivel:
        _aplicar_produtos_fallback(cad, empresas_by_dom)  # preserva Produtos do arquivo anterior
    _garante_campos_produtos(cad)                          # paridade de schema c/ painel antigo
    OUT_CADASTROS.write_text(json.dumps(cad, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT_CADASTROS.name} ({len(cad)} dominios)")

    vp = build_vp(empresas_by_dom, gmv)
    OUT_VP.write_text(json.dumps(vp, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT_VP.name} ({len(vp)} dominios)")

    reativ = build_reativacao(reativ_rows, empresas_by_dom)
    OUT_REATIV.write_text(json.dumps(reativ, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT_REATIV.name} ({len(reativ)} dominios com reativacoes)")

    links = build_links(links_rows, empresas_by_dom)
    OUT_LINKS.write_text(json.dumps(links, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT_LINKS.name} ({len(links)} dominios com links)")
    print("[ok] coleta BQ concluida. Rode build_data.py em seguida.")


if __name__ == "__main__":
    main()
