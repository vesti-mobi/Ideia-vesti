"""
Relatorio StarkBank VestiPago — Recebiveis (CP).

Fonte: API Vesti ao vivo `order/v1/report/orders-paylinks`, desnormalizando
payment.receivables (1 linha por parcela) — mesma logica do PBI "Pagamento
Stark". Substituiu o espelho do Fabric (dbo.mongodb_pedidos_recebiveis), que
materializava so ~1 de N parcelas/pedido e deixava o CP em ~46% do real.
Gera dados.js consumido pelo index.html desta pasta.

Filtros: isClosed + payment.isPaid + CREDIT_CARD + STARKBANK, janela de
~6 meses por payment.paidAt. Todas as marcas (sem filtro de companyId).
Antecipacao x Fluxo: parcela com antecipationValue > 0 = antecipada.

Auth: VESTI_ORDER_TOKEN (Bearer JWT de servico, sem expiracao).
As funcoes Fabric (connect/fetch_rows/SQL) ficam abaixo so como legado.
"""

import io
import json
import os
import struct
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import pyodbc  # usado so pelas funcoes Fabric legadas (fora do caminho principal)
except ImportError:
    pyodbc = None

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
OUT_JS = ROOT / "dados.js"
COMPANIES_JSON = ROOT.parent / "PainelCSGerencial" / "companies_data.json"

SQL_SERVER = "7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com"
SQL_DATABASE = "VestiHouse"
DRIVER = "{ODBC Driver 18 for SQL Server}"
SQL_COPT_SS_ACCESS_TOKEN = 1256


# ---------- auth ----------

def _refresh_token_access() -> str | None:
    refresh = os.environ.get("FABRIC_REFRESH_TOKEN", "").strip()
    tenant = os.environ.get("FABRIC_TENANT_ID", "").strip()
    client = os.environ.get("FABRIC_CLIENT_ID", "").strip() or "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
    if not refresh or not tenant:
        return None
    body = urllib.parse.urlencode({
        "client_id": client,
        "scope": "https://database.windows.net/.default offline_access",
        "grant_type": "refresh_token",
        "refresh_token": refresh,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[auth] refresh token flow falhou: {e}", file=sys.stderr)
        return None
    new_refresh = data.get("refresh_token")
    if new_refresh:
        try:
            (ROOT / ".new_refresh_token").write_text(new_refresh, encoding="utf-8")
        except Exception:
            pass
    return data.get("access_token")


def _az_token_struct() -> bytes | None:
    is_windows = sys.platform.startswith("win")
    try:
        out = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", "https://database.windows.net/",
             "--query", "accessToken", "-o", "tsv"],
            capture_output=True, text=True, check=True, shell=is_windows,
        )
        token = out.stdout.strip()
        if not token:
            return None
        enc = token.encode("utf-16-le")
        return struct.pack("=i", len(enc)) + enc
    except Exception:
        return None


def connect():
    base = (
        f"Driver={DRIVER};"
        f"Server={SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    ts = _az_token_struct()
    if ts:
        print("[auth] usando access token do az CLI")
        return pyodbc.connect(base, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: ts})
    raw = _refresh_token_access()
    if raw:
        print("[auth] usando FABRIC_REFRESH_TOKEN")
        enc = raw.encode("utf-16-le")
        return pyodbc.connect(base, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: struct.pack("=i", len(enc)) + enc})
    print("[auth] nenhum metodo disponivel (sem az e sem FABRIC_REFRESH_TOKEN)", file=sys.stderr)
    sys.exit(1)


# ---------- query ----------

SQL = """
-- Fonte principal: mongodb_pedidos_recebiveis (1 linha por parcela do recebivel)
-- Enriquecemos com MongoDB_Pedidos_Geral (orderNumber, customer_name, data do
-- pedido, summary_total — campos nivel-pedido que faltam na tabela de
-- recebiveis). Pedidos STARKBANK que existem so em Pedidos_Geral (ainda nao
-- copiados pra recebiveis) sao incluidos via UNION pra nao perder nada.
WITH rec AS (
    SELECT
        r._id                                    AS order_id,
        r.companyId                              AS company_id,
        r.domainId                               AS domain_id,
        r.payment_method                         AS payment_method,
        r.payment_transaction_provider           AS provider,
        r.payment_isPaid                         AS is_paid,
        -- Mongo armazena os timestamps em UTC naive. Confirmado comparando
        -- com Starkbank API: pedido 80149 UTC 01:42 vs Mongo 01:42 (~10s de
        -- diferenca apenas). Converte pra BRT antes de truncar ao dia.
        DATEADD(HOUR, -3, TRY_CAST(r.payment_paidAt AS DATETIME2))             AS paid_at,
        r.payment_transaction_installments       AS installments_total,
        r.payment_transaction_netValue           AS tx_net_value,
        r.payment_receivables_receivableId       AS rec_id,
        r.payment_receivables_installment        AS rec_installment,
        DATEADD(HOUR, -3, TRY_CAST(r.payment_receivables_dueAt AS DATETIME2))  AS rec_due_at,
        DATEADD(HOUR, -3, TRY_CAST(r.payment_receivables_paidAt AS DATETIME2)) AS rec_paid_at,
        r.payment_receivables_status             AS rec_status,
        r.payment_receivables_netValue           AS rec_net_value,
        r.payment_receivables_grossValue         AS rec_gross_value,
        r.payment_receivables_vestiPagoValue     AS rec_vp_value,
        r.payment_receivables_antifraudValue     AS rec_antifraud_value,
        r.payment_receivables_antecipationValue  AS rec_antecipation_value,
        r.payment_receivables_advanced           AS rec_advanced,
        r.payment_receivables_invoiceUrl         AS rec_invoice_url,
        r.payment_receivables_transactionId      AS rec_transaction_id
    FROM dbo.mongodb_pedidos_recebiveis r
    WHERE r.payment_transaction_provider = 'STARKBANK'
),
pedidos AS (
    SELECT
        _id                             AS order_id,
        MAX(orderNumber)                AS order_number,
        MAX(customer_name)              AS customer_name,
        MAX(customer_doc)               AS customer_doc,
        -- settings_createdAt_TIMESTAMP vem em UTC; converte pra BRT (-3h)
        -- antes de truncar pra DATE — senão pedidos feitos depois das 21h BRT
        -- aparecem no dia seguinte.
        MAX(CONVERT(DATE, DATEADD(HOUR, -3, TRY_CAST(settings_createdAt_TIMESTAMP AS DATETIME2)))) AS order_date,
        MAX(summary_total)              AS summary_total
    FROM dbo.MongoDB_Pedidos_Geral
    WHERE payment_transaction_provider = 'STARKBANK'
    GROUP BY _id
),
only_pedidos AS (
    -- Pedidos que existem em Pedidos_Geral mas nao em recebiveis ainda:
    -- monta 1 linha "parcela 1" a partir dos campos de receivable que
    -- Pedidos_Geral traz (installment 1 normalmente)
    SELECT
        p._id                                    AS order_id,
        p.companyId                              AS company_id,
        p.domainId                               AS domain_id,
        p.payment_method                         AS payment_method,
        p.payment_transaction_provider           AS provider,
        p.payment_isPaid                         AS is_paid,
        DATEADD(HOUR, -3, TRY_CAST(p.payment_paidAt AS DATETIME2))             AS paid_at,
        p.payment_transaction_installments       AS installments_total,
        p.payment_transaction_netValue           AS tx_net_value,
        p.payment_receivables_receivableId       AS rec_id,
        p.payment_receivables_installment        AS rec_installment,
        DATEADD(HOUR, -3, TRY_CAST(p.payment_receivables_dueAt AS DATETIME2))  AS rec_due_at,
        DATEADD(HOUR, -3, TRY_CAST(p.payment_receivables_paidAt AS DATETIME2)) AS rec_paid_at,
        p.payment_receivables_status             AS rec_status,
        p.payment_receivables_netValue           AS rec_net_value,
        p.payment_receivables_grossValue         AS rec_gross_value,
        p.payment_receivables_vestiPagoValue     AS rec_vp_value,
        p.payment_receivables_antifraudValue     AS rec_antifraud_value,
        p.payment_receivables_antecipationValue  AS rec_antecipation_value,
        p.payment_receivables_advanced           AS rec_advanced,
        p.payment_receivables_invoiceUrl         AS rec_invoice_url,
        p.payment_receivables_transactionId      AS rec_transaction_id
    FROM dbo.MongoDB_Pedidos_Geral p
    WHERE p.payment_transaction_provider = 'STARKBANK'
      AND p._id NOT IN (SELECT DISTINCT order_id FROM rec)
)
SELECT
    u.order_id,
    p.order_number,
    u.company_id, u.domain_id,
    p.customer_name, p.customer_doc,
    p.order_date,
    u.payment_method, u.provider,
    u.is_paid, u.paid_at,
    u.installments_total, u.tx_net_value,
    p.summary_total,
    u.rec_id, u.rec_installment, u.rec_due_at, u.rec_paid_at,
    u.rec_status, u.rec_net_value, u.rec_gross_value, u.rec_vp_value,
    u.rec_antifraud_value, u.rec_antecipation_value, u.rec_advanced,
    u.rec_invoice_url, u.rec_transaction_id,
    c.paymentSettings_provider                         AS company_provider,
    c.name                                             AS company_name,
    c.paymentSettings_customAntecipationFees_isEnabled AS antec_fee_enabled,
    c.paymentSettings_customAntecipationFees_d1        AS antec_d1,
    c.paymentSettings_starkbank_workspaceId            AS workspace_id
FROM (
    SELECT * FROM rec
    UNION ALL
    SELECT * FROM only_pedidos
) u
LEFT JOIN pedidos p ON p.order_id = u.order_id
LEFT JOIN dbo.mongodb_companies c ON c.companyId = u.company_id
ORDER BY p.order_date DESC, u.order_id, u.rec_installment
"""


def _to_float(v, default: float = 0.0) -> float:
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _iso_or_empty(v) -> str:
    if v is None:
        return ""
    if hasattr(v, "isoformat"):
        s = v.isoformat()
    else:
        s = str(v)
    # Aceita strings tipo "2026-05-18T03:00:00Z" e datas simples "2026-04-16"
    return s[:19] if len(s) >= 19 else s


def fetch_rows(conn) -> list[dict]:
    print("[fabric] rodando query STARKBANK")
    cur = conn.cursor()
    cur.execute(SQL)
    cols = [d[0] for d in cur.description]
    raw = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(raw)} linhas brutas (uma por parcela)")
    return raw


def _load_company_map() -> dict[str, str]:
    """domain_id -> nome_fantasia (prioriza matriz). Dominios ausentes sao
    considerados teste/inativos e serao filtrados."""
    if not COMPANIES_JSON.exists():
        print(f"[companies] {COMPANIES_JSON} nao existe — sem filtro de teste", file=sys.stderr)
        return {}
    try:
        data = json.loads(COMPANIES_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[companies] falha lendo companies_data.json: {e}", file=sys.stderr)
        return {}
    mp: dict[str, str] = {}
    for c in data:
        did = str(c.get("domain_id") or "").strip()
        if not did:
            continue
        nf = (c.get("nome_fantasia") or c.get("name") or "").strip()
        if c.get("isMatriz") or did not in mp:
            mp[did] = nf or mp.get(did, "")
    print(f"[companies] {len(mp)} dominios ativos carregados")
    return mp


def build(raw: list[dict]) -> dict:
    """Agrupa por orderId; cada pedido e uma dict com `parcelas` aninhadas."""
    company_map = _load_company_map()
    by_order: dict[str, dict] = {}
    for r in raw:
        oid = r.get("order_id") or ""
        if not oid:
            continue
        # filtra parcelas com netValue 0, exceto em empresas com antecipacao
        # habilitada (ex: Andressa-Teste, cujos pedidos de teste vem zerados
        # mas precisamos exibir pra validar o fluxo de antecipacao)
        _nv = float(r.get("rec_net_value") or 0)
        _has_antec = (float(r.get("antec_d1") or 0) > 0) or bool(r.get("antec_fee_enabled"))
        if _nv == 0 and not _has_antec:
            continue
        parcela = {
            "recId": r.get("rec_id") or "",
            "installment": int(r.get("rec_installment") or 0),
            "dueAt": _iso_or_empty(r.get("rec_due_at")),
            "paidAt": _iso_or_empty(r.get("rec_paid_at")),
            "status": r.get("rec_status") or "",
            "netValue": float(r.get("rec_net_value") or 0),
            "grossValue": float(r.get("rec_gross_value") or 0),
            "vpValue": float(r.get("rec_vp_value") or 0),
            "antifraudValue": float(r.get("rec_antifraud_value") or 0),
            "antecipationValue": _to_float(r.get("rec_antecipation_value")),
            "advanced": bool(r.get("rec_advanced")) if r.get("rec_advanced") is not None else None,
            "invoiceUrl": r.get("rec_invoice_url") or "",
            "transactionId": r.get("rec_transaction_id") or "",
        }
        ped = by_order.get(oid)
        if ped is None:
            did = str(r.get("domain_id") or "").strip()
            # Empresas com antecipacao habilitada (d1>0) passam mesmo se o
            # dominio nao estiver no companies_data.json (ex: "Andressa - Teste"
            # em dominio de teste, usado pra validar o fluxo de antecipacao).
            has_antec = (float(r.get("antec_d1") or 0) > 0) or bool(r.get("antec_fee_enabled"))
            if company_map and did not in company_map and not has_antec:
                continue
            ped = {
                "orderId": oid,
                "orderNumber": r.get("order_number"),
                "companyId": r.get("company_id") or "",
                "domainId": did,
                "nomeFantasia": company_map.get(did, "") or (r.get("company_name") or ""),
                "customerName": r.get("customer_name") or "",
                "customerDoc": r.get("customer_doc") or "",
                "orderDate": _iso_or_empty(r.get("order_date")),
                "paymentMethod": r.get("payment_method") or "",
                "provider": r.get("provider") or "",
                "isPaid": bool(r.get("is_paid")) if r.get("is_paid") is not None else None,
                "paidAt": _iso_or_empty(r.get("paid_at")),
                "installmentsTotal": int(r.get("installments_total") or 0),
                "txNetValue": float(r.get("tx_net_value") or 0),
                "summaryTotal": float(r.get("summary_total") or 0),
                "companyProvider": r.get("company_provider") or "",
                "antecipacaoEnabled": (float(r.get("antec_d1") or 0) > 0) or bool(r.get("antec_fee_enabled")),
                "antecipacaoD1": float(r.get("antec_d1") or 0),
                "workspaceId": str(r.get("workspace_id") or "").strip(),
                "parcelas": [],
            }
            by_order[oid] = ped
        ped["parcelas"].append(parcela)

    # Pos-processa cada pedido — stats das parcelas
    pedidos: list[dict] = []
    for ped in by_order.values():
        if not ped["parcelas"]:
            continue
        # 2a verificacao: se qualquer parcela tem antecipationValue > 0,
        # considera o pedido como antecipacao (mesmo que a flag da empresa
        # esteja desligada — ex: Kelly Rodrigues com pedidos antigos cuja
        # flag so foi ligada depois).
        if not ped.get("antecipacaoEnabled"):
            if any(float(p.get("antecipationValue") or 0) > 0 for p in ped["parcelas"]):
                ped["antecipacaoEnabled"] = True
        parcelas = sorted(ped["parcelas"], key=lambda p: p["installment"])
        ped["parcelas"] = parcelas
        ped["nParcelas"] = len(parcelas)
        due_dates = [p["dueAt"] for p in parcelas if p["dueAt"]]
        ped["firstDueAt"] = min(due_dates) if due_dates else ""
        ped["lastDueAt"] = max(due_dates) if due_dates else ""
        # Proxima parcela = menor dueAt entre as nao pagas
        unpaid = [p for p in parcelas if not p["paidAt"]]
        ped["nextDueAt"] = min([p["dueAt"] for p in unpaid if p["dueAt"]], default="")
        ped["nPagas"] = sum(1 for p in parcelas if p["paidAt"])
        ped["nPendentes"] = sum(1 for p in parcelas if not p["paidAt"])
        ped["totalNet"] = round(sum(p["netValue"] for p in parcelas), 2)
        ped["totalGross"] = round(sum(p["grossValue"] for p in parcelas), 2)
        ped["totalVp"] = round(sum(p["vpValue"] for p in parcelas), 2)
        ped["allPaid"] = ped["nPendentes"] == 0 and ped["nPagas"] > 0
        pedidos.append(ped)

    return _assemble(pedidos)


def _assemble(pedidos: list[dict]) -> dict:
    """Gera pagamentos/resumo/listas a partir de uma lista de pedidos ja montada.
    Separado de build() pra poder rodar sobre pedidos MESCLADOS (merge do CP
    dos ultimos dias no dados.js existente) sem refazer a agregacao inteira."""
    pedidos.sort(key=lambda p: p.get("orderDate") or "", reverse=True)

    methods = sorted({p["paymentMethod"] for p in pedidos if p["paymentMethod"]})
    statuses = sorted({pc["status"] for p in pedidos for pc in p["parcelas"] if pc["status"]})
    companies = sorted({p["companyId"] for p in pedidos if p["companyId"]})

    # --- Lista flat de pagamentos (1 por parcela) pro financeiro ---
    # payDate = dueAt (dia em que StarkBank liquida a parcela). Se a
    # parcela ja foi paga, usa paidAt real. Fallback: orderDate+1 pra
    # antec quando dueAt vem vazio.
    from datetime import date as _date, timedelta as _td
    def _parse_day(s: str):
        try:
            return _date.fromisoformat(s[:10]) if s else None
        except Exception:
            return None

    def _next_business_day(d):
        """Avanca enquanto cair em sab/dom (weekday 5 ou 6)."""
        while d.weekday() >= 5:
            d = d + _td(days=1)
        return d

    pagamentos: list[dict] = []
    for p in pedidos:
        is_antec = bool(p.get("antecipacaoEnabled"))
        order_d = _parse_day(p.get("orderDate") or "")
        customer_paid_at = (p.get("paidAt") or "")[:10]
        cpa_d = _parse_day(customer_paid_at)
        for pc in p["parcelas"]:
            paid = (pc.get("paidAt") or "")[:10]
            due = (pc.get("dueAt") or "")[:10]
            if is_antec:
                # Antecipacao: o dueAt da parcela ja e a data em que a StarkBank
                # liquida pra marca (D+1 com o corte noturno DELA). Manda nele.
                # A heuristica "proximo dia util de (pagamento do cliente + 1)"
                # so entra quando a API nao trouxe dueAt — ela erra em venda
                # tarde da noite (vira o dia em UTC) e desloca o valor de dia.
                if paid:
                    pay_date = paid
                elif due:
                    pay_date = due
                else:
                    base = cpa_d or order_d
                    pay_date = (
                        _next_business_day(base + _td(days=1)).isoformat()
                        if base else ""
                    )
            elif paid:
                pay_date = paid
            elif due:
                pay_date = due
            else:
                pay_date = ""
            pagamentos.append({
                "payDate": pay_date,
                "isAntecipacao": is_antec,
                "isPaid": bool(paid),
                "companyId": p["companyId"],
                "nomeFantasia": p.get("nomeFantasia", ""),
                "domainId": p.get("domainId", ""),
                "orderId": p["orderId"],
                "orderNumber": p.get("orderNumber"),
                "customerName": p.get("customerName", ""),
                "orderDate": p.get("orderDate", ""),
                "customerPaidAt": customer_paid_at,
                "installment": pc.get("installment", 0),
                "installmentsTotal": p.get("installmentsTotal", 0),
                "dueAt": due,
                "paidAt": paid,
                "grossValue": round(pc.get("grossValue") or 0, 2),
                "vpValue": round(pc.get("vpValue") or 0, 2),
                "antifraudValue": round(pc.get("antifraudValue") or 0, 2),
                "antecipationValue": round(pc.get("antecipationValue") or 0, 2),
                "netValue": round(pc.get("netValue") or 0, 2),
                "status": pc.get("status", ""),
            })
    pagamentos.sort(key=lambda x: (x["payDate"] or "", x["nomeFantasia"] or "", x.get("orderNumber") or 0))

    total_net = sum(p["totalNet"] for p in pedidos)
    total_gross = sum(p["totalGross"] for p in pedidos)
    total_vp = sum(p["totalVp"] for p in pedidos)
    total_parcelas = sum(p["nParcelas"] for p in pedidos)
    total_pagas = sum(p["nPagas"] for p in pedidos)
    total_pendentes = sum(p["nPendentes"] for p in pedidos)

    return {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "pedidos": pedidos,
        "paymentMethods": methods,
        "statuses": statuses,
        "companies": companies,
        "pagamentos": pagamentos,
        "resumo": {
            "nPedidos": len(pedidos),
            "nParcelas": total_parcelas,
            "totalNet": round(total_net, 2),
            "totalGross": round(total_gross, 2),
            "totalVpValue": round(total_vp, 2),
            "nPagas": total_pagas,
            "nPendentes": total_pendentes,
        },
    }


# ---------- fonte: API Vesti (orders-paylinks) ----------
# O CP vem da API ao vivo (payment.receivables completo), NAO do espelho Fabric
# (que materializava so ~1 de N parcelas/pedido -> CP ficava em ~46% do real).
# Mesma logica do PBI "Pagamento Stark" (orders_receivables_custom.py).

API_URL = "https://apivesti.vesti.mobi/order/v1/report/orders-paylinks"
WINDOW_DAYS = int(os.environ.get("CP_WINDOW_DAYS", "190"))  # ~6 meses
PAGE_LIMIT = int(os.environ.get("CP_PAGE_LIMIT", "500"))  # com fatiamento mensal o offset e' raso; 500/pagina ~poucas paginas/mes
PAGE_DELAY = float(os.environ.get("CP_PAGE_DELAY", "1"))   # seg de pausa entre requisicoes (gentil com o banco de producao)
INVOICES_JS = ROOT / "invoices.js"


def _order_token() -> str:
    t = os.environ.get("VESTI_ORDER_TOKEN", "").strip()
    if not t:
        print("ERRO: defina VESTI_ORDER_TOKEN (JWT da API de pedidos Vesti)",
              file=sys.stderr)
        sys.exit(1)
    return t


def _brt(s) -> str:
    """ISO UTC ('...Z') -> string BRT (-3h) 'YYYY-MM-DDTHH:MM:SS'; vazio se nulo.
    Mantem a mesma convencao do antigo SQL (DATEADD(HOUR,-3,...))."""
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return (dt - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return str(s)[:19]


def _company_meta_from_invoices() -> dict:
    """companyId -> {nome, ws, domain} a partir do invoices.js (CR), ja presente
    no repo. Evita depender de Fabric/companies_data.json so pro nome da marca."""
    meta: dict[str, dict] = {}
    if not INVOICES_JS.exists():
        print("[api] invoices.js ausente — nomes de marca podem ficar vazios",
              file=sys.stderr)
        return meta
    try:
        txt = INVOICES_JS.read_text(encoding="utf-8").strip()
        txt = txt[txt.index("=") + 1:].rstrip().rstrip(";")
        inv = json.loads(txt)
        for f in inv.get("faturas", []):
            cid = f.get("companyId")
            if not cid or cid in meta:
                continue
            meta[cid] = {
                "nome": f.get("nomeFantasia") or "",
                "ws": str(f.get("workspaceId") or "").strip(),
                "domain": str(f.get("domainId") or "").strip(),
            }
    except Exception as e:
        print(f"[api] falha lendo invoices.js: {e}", file=sys.stderr)
    print(f"[api] {len(meta)} marcas mapeadas via invoices.js")
    return meta


def _get_json(url: str, headers: dict, attempts: int = 4) -> dict:
    """GET com retry/backoff por requisicao (cobre IncompleteRead/URLError/5xx),
    pra um soluco numa pagina nao obrigar a refazer a paginacao inteira."""
    delay = 5
    for a in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers),
                                        timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            if a == attempts:
                raise
            print(f"[api]   retry {a}/{attempts} ({type(e).__name__}: {e})",
                  file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 60)


def _month_windows(start_dt: datetime, end_dt: datetime) -> list[tuple[datetime, datetime]]:
    """Fatia [start_dt, end_dt] em janelas de mes-calendario (UTC). Cada janela
    tem poucos pedidos -> poucas paginas -> offset raso -> a API nao da 500."""
    wins: list[tuple[datetime, datetime]] = []
    cur = start_dt
    while cur <= end_dt:
        if cur.month == 12:
            nxt = datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            nxt = datetime(cur.year, cur.month + 1, 1, tzinfo=timezone.utc)
        # lte = inicio do mes seguinte (janelas ENCOSTAM); o dedup por _id remove
        # a duplicata da fronteira. Assim nenhum pedido no ultimo instante do mes
        # (com milissegundos) escapa entre 23:59:59 e 00:00:00.
        w_end = min(nxt, end_dt)
        wins.append((cur, w_end))
        cur = nxt
    return wins


def _fetch_window(base: dict, gte: str, lte: str, headers: dict, label: str) -> tuple[list[dict], int]:
    """Pagina UMA janela (mes) com paidAt em [gte, lte]. Offset sempre raso."""
    pbase = dict(base)
    pbase["filter[payment.paidAt][$gte]"] = gte
    pbase["filter[payment.paidAt][$lte]"] = lte
    out: list[dict] = []
    total_pages = None
    total_docs = None
    page = 1
    while True:
        params = dict(pbase)
        params["page"] = page
        d = _get_json(API_URL + "?" + urllib.parse.urlencode(params), headers)
        items = d.get("data", []) if isinstance(d, dict) else (d or [])
        out.extend(items)
        if total_pages is None and isinstance(d, dict):
            total_pages = int(d.get("totalPages") or 1)
            total_docs = d.get("totalDocs")
        print(f"[api]   {label} pagina {page}/{total_pages}: +{len(items)} "
              f"(acum {len(out)}/{total_docs})")
        # hasNextPage da API e bugado; paginamos por totalPages.
        if not items or (total_pages and page >= total_pages):
            break
        page += 1
        if PAGE_DELAY > 0:
            time.sleep(PAGE_DELAY)  # gentil com o banco de producao
    if total_docs and len(out) < total_docs:
        print(f"[api]   {label} AVISO: coletados {len(out)} < totalDocs {total_docs}",
              file=sys.stderr)
    return out, int(total_docs or 0)


def fetch_orders() -> list[dict]:
    token = _order_token()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=WINDOW_DAYS)
    headers = {"accept": "application/json", "Authorization": "Bearer " + token}
    base = {
        "filter[isClosed]": "true",
        "filter[payment.isPaid]": "true",
        "filter[payment.method]": "CREDIT_CARD",
        "filter[payment.transaction.provider]": "STARKBANK",
        "select": "orderNumber,companyId,domainId,customer,payment",
        "limit": PAGE_LIMIT,
    }
    windows = _month_windows(start, now)
    print(f"[api] pedidos pagos STARKBANK, paidAt em [{start:%Y-%m-%d} .. {now:%Y-%m-%d}] "
          f"— fatiado em {len(windows)} janelas mensais (evita offset fundo/500)")
    by_id: dict[str, dict] = {}
    sum_docs = 0
    for (ws, we) in windows:
        gte = ws.strftime("%Y-%m-%dT%H:%M:%S")
        lte = we.strftime("%Y-%m-%dT%H:%M:%S")
        label = f"{ws:%Y-%m}"
        items, tdocs = _fetch_window(base, gte, lte, headers, label)
        sum_docs += tdocs
        n_new = 0
        for o in items:
            key = o.get("_id") or o.get("id") or f"{o.get('orderNumber')}|{o.get('companyId')}"
            if key not in by_id:
                by_id[key] = o
                n_new += 1
        print(f"[api] janela {label}: {len(items)} itens ({n_new} novos, dedup) — totalDocs {tdocs}")
    all_orders = list(by_id.values())
    print(f"[api] {len(all_orders)} pedidos no total ({len(windows)} janelas; "
          f"soma totalDocs das janelas={sum_docs})")
    return all_orders


def rows_from_orders(orders: list[dict]) -> list[dict]:
    """Desnormaliza payment.receivables no formato de linha que build() espera
    (mesmas chaves rec_*/order_* do antigo SQL do Fabric)."""
    meta = _company_meta_from_invoices()
    rows: list[dict] = []
    for o in orders:
        pay = o.get("payment") or {}
        tx = pay.get("transaction") or {}
        cust = o.get("customer") or {}
        cid = o.get("companyId") or ""
        m = meta.get(cid, {})
        paid_brt = _brt(pay.get("paidAt"))
        order_date = paid_brt[:10]  # API nao traz createdAt; usa a data do pagamento
        domain = str(o.get("domainId") or m.get("domain") or "").strip()
        for rc in (pay.get("receivables") or []):
            rows.append({
                "order_id": o.get("_id") or "",
                "order_number": o.get("orderNumber"),
                "company_id": cid,
                "domain_id": domain,
                "customer_name": cust.get("name") or "",
                "customer_doc": cust.get("doc") or "",
                "order_date": order_date,
                "payment_method": pay.get("method") or "",
                "provider": tx.get("provider") or "STARKBANK",
                "is_paid": pay.get("isPaid"),
                "paid_at": paid_brt,
                "installments_total": tx.get("installments") or 0,
                "tx_net_value": tx.get("netValue") or 0,
                "summary_total": tx.get("value") or 0,
                "rec_id": rc.get("receivableId") or "",
                "rec_installment": rc.get("installment") or 0,
                "rec_due_at": _brt(rc.get("dueAt")),
                "rec_paid_at": _brt(rc.get("paidAt")),
                "rec_status": rc.get("status") or "",
                "rec_net_value": rc.get("netValue") or 0,
                "rec_gross_value": rc.get("grossValue") or 0,
                "rec_vp_value": rc.get("vestiPagoValue") or 0,
                "rec_antifraud_value": rc.get("antifraudValue") or 0,
                "rec_antecipation_value": rc.get("antecipationValue"),
                "rec_advanced": rc.get("advanced"),
                "rec_invoice_url": rc.get("invoiceUrl") or "",
                "rec_transaction_id": rc.get("transactionId") or "",
                "company_provider": tx.get("provider") or "STARKBANK",
                "company_name": m.get("nome") or "",
                "antec_fee_enabled": None,
                "antec_d1": 0,
                "workspace_id": m.get("ws") or "",
            })
    print(f"[api] {len(rows)} linhas (uma por parcela)")
    return rows


def _fetch_with_retry(max_attempts: int = 4) -> list[dict]:
    """Busca os pedidos na API Vesti e desnormaliza em linhas-parcela. Retry
    com backoff em erros transitorios de rede/HTTP."""
    delay = 15
    for attempt in range(1, max_attempts + 1):
        try:
            return rows_from_orders(fetch_orders())
        except Exception as e:
            if attempt == max_attempts:
                print(f"[api] falha definitiva apos {attempt} tentativas: {e}",
                      file=sys.stderr)
                raise
            print(f"[api] erro '{e}' (tentativa {attempt}/{max_attempts}); "
                  f"aguardando {delay}s...", file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 120)
    raise RuntimeError("unreachable")


def _read_existing_data():
    """Le o dados.js atual (window.DADOS = {...};) pra permitir merge parcial."""
    if not OUT_JS.exists():
        return None
    txt = OUT_JS.read_text(encoding="utf-8").strip()
    prefix = "window.DADOS = "
    if txt.startswith(prefix):
        txt = txt[len(prefix):]
    if txt.endswith(";"):
        txt = txt[:-1]
    try:
        return json.loads(txt)
    except Exception as e:
        print(f"[merge] falha ao ler dados.js existente: {e}", file=sys.stderr)
        return None


def _load_cancelled_ids() -> set:
    """orderIds cancelados (cancelados_valores.js) — pra nao re-inserir cancelados."""
    f = ROOT / "cancelados_valores.js"
    if not f.exists():
        return set()
    try:
        t = f.read_text(encoding="utf-8").strip()
        t = t[t.index("=") + 1:].rstrip().rstrip(";")
        return set(json.loads(t).get("valores", {}).keys())
    except Exception as e:
        print(f"[persist] falha lendo cancelados_valores.js: {e}", file=sys.stderr)
        return set()


def _ids_nao_pagos(candidatos: list[dict]) -> set:
    """orderIds que a API declara HOJE como NAO PAGOS entre os candidatos a
    persistir (pagamento recusado ou estornado depois da venda).

    Por que existe: um pedido cujo pagamento virou `isPaid=false` /
    `consolidatedPaymentStatus=REFUSED` sai do retorno de fetch_orders() — nao por
    drop da API, e sim porque a coleta filtra `payment.isPaid=true`. Sem esta
    checagem a persistencia segurava esse pedido para sempre e o "a pagar" do
    fluxo ficava inflado. Caso que motivou (17/08/2026): Nova Versao 8054/8055/
    8056/8171 e Maria Chica 5951 — R$ 2.236,02 de gross futuro fantasma, R$ 332,48
    deles caindo em "hoje".

    Como: 1 varredura por marca com `filter[payment.isPaid]=false` (projecao
    enxuta, so isPaid + consolidatedPaymentStatus) e intersecao pelos _id. NAO da
    para achar esses pedidos por janela de paidAt — o paidAt deles virou null.

    Pedido que NAO aparecer aqui continua persistido: aquele e o drop da API, que e
    exatamente o caso que a persistencia existe para cobrir. Falha de token ou de
    rede tambem nao descarta ninguem (fail open) — melhor inflar um pouco do que
    apagar cauda legitima.
    """
    token = os.environ.get("VESTI_ORDER_TOKEN", "").strip()
    if not token:
        print("[persist] sem VESTI_ORDER_TOKEN — nao consigo checar pagamento "
              "recusado; persistindo todos os candidatos", file=sys.stderr)
        return set()
    headers = {"accept": "application/json", "Authorization": "Bearer " + token}
    por_marca: dict[str, dict[str, dict]] = {}
    for p in candidatos:
        oid = p.get("orderId")
        cid = p.get("companyId") or ""
        if oid:
            por_marca.setdefault(cid, {})[oid] = p

    recusados: set = set()
    for cid, ids in por_marca.items():
        base = {
            "filter[companyId]": cid,
            "filter[payment.isPaid]": "false",
            # projecao enxuta: 1000 pedidos em ~160KB no lugar de ~1,3MB
            "select": "orderNumber,companyId,payment.isPaid,"
                      "payment.consolidatedPaymentStatus",
            "limit": PAGE_LIMIT,
        }
        try:
            page = 1
            total_pages = None
            while True:
                params = dict(base)
                params["page"] = page
                d = _get_json(API_URL + "?" + urllib.parse.urlencode(params), headers)
                items = d.get("data", []) if isinstance(d, dict) else (d or [])
                if total_pages is None:
                    total_pages = int(d.get("totalPages") or 1) if isinstance(d, dict) else 1
                for o in items:
                    oid = o.get("_id") or o.get("id") or ""
                    p = ids.get(oid)
                    if not p:
                        continue
                    pay = o.get("payment") or {}
                    recusados.add(oid)
                    print(f"[persist]   descartado: {p.get('nomeFantasia')} pedido "
                          f"{p.get('orderNumber')} — API diz isPaid="
                          f"{pay.get('isPaid')} / {pay.get('consolidatedPaymentStatus')}")
                if not items or (total_pages and page >= total_pages):
                    break
                page += 1
                if PAGE_DELAY > 0:
                    time.sleep(PAGE_DELAY)
        except Exception as e:
            print(f"[persist] checagem de pagamento falhou para {cid} "
                  f"({type(e).__name__}: {e}) — mantendo os candidatos dessa marca",
                  file=sys.stderr)
    return recusados


def _persist_fluxo_orders(fresh: dict) -> dict:
    """Preserva pedidos de FLUXO (nao-antecipados) ja vistos que a API deixou de
    devolver neste run, pra a cauda 'a pagar' nao desaparecer quando a API dropa
    pedidos (comportamento observado: o conjunto de um mes encolhe sem que os
    pedidos tenham sido cancelados).

    Regras:
      - o pedido FRESCO sempre vence (so re-inserimos pedidos AUSENTES do fresco),
        entao nada e atualizado errado;
      - so re-insere FLUXO (nao-antecipado) — a aba Antecipacao segue o retrato
        fresco da API, sem alteracao;
      - nao re-insere cancelados;
      - nao re-insere pedido cujo PAGAMENTO a API declara nao pago (recusado/
        estornado) — ver _ids_nao_pagos();
      - PODA: so mantem o pedido persistido enquanto ele tiver ao menos uma parcela
        vencendo hoje ou no futuro (BRT); quando todas venceram, ele sai sozinho.
    """
    existing = _read_existing_data()
    if not (existing and existing.get("pedidos")):
        return fresh  # 1a geracao / dados.js ilegivel -> nada a preservar

    fresh_ids = {p.get("orderId") for p in fresh.get("pedidos", []) if p.get("orderId")}
    canc_ids = _load_cancelled_ids()
    brt_today = (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d")

    candidatos = []
    for p in existing["pedidos"]:
        oid = p.get("orderId")
        if not oid or oid in fresh_ids:        # presente no fresco -> fresco vence
            continue
        if p.get("antecipacaoEnabled"):        # antecipacao NAO e persistida
            continue
        if oid in canc_ids:                    # cancelado -> nao re-insere
            continue
        dues = [(pc.get("dueAt") or "")[:10] for pc in p.get("parcelas", [])]
        if not any(d and d >= brt_today for d in dues):   # todas venceram -> poda
            continue
        candidatos.append(p)

    if not candidatos:
        return fresh

    # Pagamento recusado/estornado nao e divida: esse pedido sai do retorno da API
    # por causa do filtro isPaid=true, e sem esta checagem a persistencia o
    # segurava para sempre (fantasma no "a pagar").
    recusados = _ids_nao_pagos(candidatos)
    readd = []
    for p in candidatos:
        if p.get("orderId") in recusados:
            continue
        p["_persistido"] = True                # marcador (transparencia/debug)
        readd.append(p)
    if recusados:
        print(f"[persist] {len(recusados)} pedido(s) NAO persistido(s): a API diz "
              f"que o pagamento nao esta pago (recusado/estornado)")

    if not readd:
        return fresh

    merged = list(fresh["pedidos"]) + readd
    print(f"[persist] {len(readd)} pedido(s) de fluxo preservados "
          f"(sumiram da API mas ainda tem parcela a vencer)")
    return _assemble(merged)


def main() -> None:
    raw = _fetch_with_retry()
    fresh = build(raw)

    # CP_MERGE=1 + janela curta (CP_WINDOW_DAYS=2): atualiza SO os pedidos dos
    # ultimos dias e faz overlay por orderId no dados.js existente, mantendo todo
    # o historico intacto. Serve pra fugir do 500 da paginacao funda (a janela
    # curta tem poucas paginas -> offset raso -> nao estoura), atualizando pelo
    # menos ontem+hoje sem depender do fetch completo de 190 dias.
    if os.environ.get("CP_MERGE") == "1":
        existing = _read_existing_data()
        if not (existing and existing.get("pedidos")):
            print("[merge] ERRO: CP_MERGE pedido mas dados.js existente esta "
                  "inexistente/ilegivel — abortando pra NAO sobrescrever o "
                  "historico com so a janela curta.", file=sys.stderr)
            sys.exit(1)
        by_id = {p.get("orderId"): p for p in existing["pedidos"] if p.get("orderId")}
        n_before = len(by_id)
        for p in fresh.get("pedidos", []):
            oid = p.get("orderId")
            if oid:
                by_id[oid] = p  # fresco vence; pedidos antigos ficam intactos
        data = _assemble(list(by_id.values()))
        print(f"[merge] {len(fresh.get('pedidos', []))} pedidos frescos (janela "
              f"{WINDOW_DAYS}d) aplicados; {n_before} -> {len(by_id)} pedidos no total")
    else:
        # Preserva a cauda de FLUXO que a API dropar (antecipacao segue fresca).
        data = _persist_fluxo_orders(fresh)

    OUT_JS.write_text(
        "window.DADOS = " + json.dumps(data, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    size_kb = OUT_JS.stat().st_size / 1024
    print(f"[write] {OUT_JS.name} ({data['resumo']['nPedidos']} pedidos, "
          f"{data['resumo']['nParcelas']} parcelas, {size_kb:.1f}KB)")


if __name__ == "__main__":
    main()
