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
        MAX(settings_createdAt_TIMESTAMP) AS order_date,
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
    u.rec_invoice_url, u.rec_transaction_id
FROM (
    SELECT * FROM rec
    UNION ALL
    SELECT * FROM only_pedidos
) u
LEFT JOIN pedidos p ON p.order_id = u.order_id
ORDER BY p.order_date DESC, u.order_id, u.rec_installment
"""


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
    print(f"[fabric] {len(raw)} linhas STARKBANK")

    rows: list[dict] = []
    for r in raw:
        rows.append({
            "orderId": r.get("order_id") or "",
            "orderNumber": r.get("order_number"),
            "companyId": r.get("company_id") or "",
            "domainId": r.get("domain_id") or "",
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
            "recId": r.get("rec_id") or "",
            "recInstallment": int(r.get("rec_installment") or 0),
            "recDueAt": _iso_or_empty(r.get("rec_due_at")),
            "recPaidAt": _iso_or_empty(r.get("rec_paid_at")),
            "recStatus": r.get("rec_status") or "",
            "recNetValue": float(r.get("rec_net_value") or 0),
            "recGrossValue": float(r.get("rec_gross_value") or 0),
            "recVpValue": float(r.get("rec_vp_value") or 0),
            "recAntifraudValue": float(r.get("rec_antifraud_value") or 0),
            "recAntecipationValue": str(r.get("rec_antecipation_value") or ""),
            "recAdvanced": bool(r.get("rec_advanced")) if r.get("rec_advanced") is not None else None,
            "recInvoiceUrl": r.get("rec_invoice_url") or "",
            "recTransactionId": r.get("rec_transaction_id") or "",
        })
    return rows


def build(rows: list[dict]) -> dict:
    methods = sorted({r["paymentMethod"] for r in rows if r["paymentMethod"]})
    statuses = sorted({r["recStatus"] for r in rows if r["recStatus"]})
    companies = sorted({r["companyId"] for r in rows if r["companyId"]})

    total_net = sum(r["recNetValue"] for r in rows)
    total_gross = sum(r["recGrossValue"] for r in rows)
    total_vp = sum(r["recVpValue"] for r in rows)
    pagas = sum(1 for r in rows if r["recPaidAt"])
    pendentes = len(rows) - pagas

    return {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
        "paymentMethods": methods,
        "statuses": statuses,
        "companies": companies,
        "resumo": {
            "nRows": len(rows),
            "totalNet": round(total_net, 2),
            "totalGross": round(total_gross, 2),
            "totalVpValue": round(total_vp, 2),
            "nPagas": pagas,
            "nPendentes": pendentes,
        },
    }


def main() -> None:
    with connect() as conn:
        rows = fetch_rows(conn)
    data = build(rows)
    OUT_JS.write_text(
        "window.DADOS = " + json.dumps(data, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    size_kb = OUT_JS.stat().st_size / 1024
    print(f"[write] {OUT_JS.name} ({len(rows)} linhas, {size_kb:.1f}KB)")


if __name__ == "__main__":
    main()
