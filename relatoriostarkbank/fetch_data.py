"""
Relatorio StarkBank VestiPago — Recebiveis.

Puxa pedidos com payment_transaction_provider = 'STARKBANK' da tabela
dbo.MongoDB_Pedidos_Geral no lakehouse VestiHouse (Fabric). Gera dados.js
consumido pelo index.html desta pasta.

Auth:
- local: az CLI (az login na conta com acesso ao Fabric)
- CI:    FABRIC_REFRESH_TOKEN + FABRIC_TENANT_ID (+ FABRIC_CLIENT_ID opcional)
         — trocado por access token no fluxo refresh_token OAuth2
"""

import io
import json
import os
import struct
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    import pyodbc
except ImportError:
    print("ERRO: pyodbc nao instalado. Rode: py -m pip install pyodbc", file=sys.stderr)
    sys.exit(1)

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
        r.payment_paidAt                         AS paid_at,
        r.payment_transaction_installments       AS installments_total,
        r.payment_transaction_netValue           AS tx_net_value,
        r.payment_receivables__id                AS rec_id,
        r.payment_receivables_installment        AS rec_installment,
        r.payment_receivables_dueAt              AS rec_due_at,
        r.payment_receivables_paidAt             AS rec_paid_at,
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
        p.payment_paidAt                         AS paid_at,
        p.payment_transaction_installments       AS installments_total,
        p.payment_transaction_netValue           AS tx_net_value,
        p.payment_receivables__id                AS rec_id,
        p.payment_receivables_installment        AS rec_installment,
        p.payment_receivables_dueAt              AS rec_due_at,
        p.payment_receivables_paidAt             AS rec_paid_at,
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


from datetime import timezone as _tz, timedelta as _td2, datetime as _dt2
_BRT = _tz(_td2(hours=-3))


def _iso_or_empty(v) -> str:
    """Retorna timestamp ISO truncado em segundos, convertido UTC -> BRT.

    Fabric armazena os paid_at/due_at como VARCHAR ISO em UTC (ex:
    '2026-04-24T02:30:00Z'). Sem conversao, o truncamento [:10] joga pagamentos
    feitos a noite BRT pro dia seguinte. Esta funcao detecta datetime-like
    (objeto ou string com 'T') e converte. Strings date-only ('2026-04-24')
    passam intactas.
    """
    if v is None:
        return ""
    dt = None
    # objeto datetime/date
    if hasattr(v, "isoformat"):
        try:
            if getattr(v, "tzinfo", None) is None and hasattr(v, "hour"):
                dt = v.replace(tzinfo=_tz.utc)
            else:
                dt = v
            dt = dt.astimezone(_BRT) if hasattr(dt, "hour") else dt
        except Exception:
            dt = v
        s = dt.isoformat()
    else:
        s = str(v)
        # string ISO com timezone (ex: "2026-04-24T02:30:00Z"): parse + converte
        if "T" in s and len(s) >= 11:
            try:
                parsed = _dt2.fromisoformat(s.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=_tz.utc)
                s = parsed.astimezone(_BRT).isoformat()
            except Exception:
                pass
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

    pagamentos: list[dict] = []
    for p in pedidos:
        is_antec = bool(p.get("antecipacaoEnabled"))
        order_d = _parse_day(p.get("orderDate") or "")
        customer_paid_at = (p.get("paidAt") or "")[:10]
        for pc in p["parcelas"]:
            paid = (pc.get("paidAt") or "")[:10]
            due = (pc.get("dueAt") or "")[:10]
            if paid:
                pay_date = paid
            elif due:
                pay_date = due
            elif is_antec and order_d:
                pay_date = (order_d + _td(days=1)).isoformat()
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


def main() -> None:
    with connect() as conn:
        raw = fetch_rows(conn)
    data = build(raw)
    OUT_JS.write_text(
        "window.DADOS = " + json.dumps(data, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    size_kb = OUT_JS.stat().st_size / 1024
    print(f"[write] {OUT_JS.name} ({data['resumo']['nPedidos']} pedidos, "
          f"{data['resumo']['nParcelas']} parcelas, {size_kb:.1f}KB)")


if __name__ == "__main__":
    main()
