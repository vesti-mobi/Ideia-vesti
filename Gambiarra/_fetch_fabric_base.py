"""
Atualiza companies_data.json e dashboard_full_data.js usando o Lakehouse
VestiHouse (Microsoft Fabric) como fonte da verdade.

- Empresas ATIVAS: ODBC_Domains WHERE modulos LIKE '%vendas%'
  excluindo partner_id de trial/treino e nomes com 'teste'
- Uma linha por ODBC_Companies (matriz + filiais).
  A mais antiga (ROW_NUMBER=1 por domain ordenado por created_at) = matriz.
- Anjo, canal, integracao: joins com ODBC_Angels/Partners/Integrations
- Plano: iugu_subscriptions via silver_companiesativos_iugu
- Valor mensal: ultima iugu_invoices paga via silver_companiesativos_iugu

Auth: Azure CLI (az) -> token para database.windows.net.
Requer: pyodbc + ODBC Driver 18 for SQL Server.

Rodar:
    py fetch_fabric.py
"""

import json
import os
import re
import struct
import subprocess
import sys
import io
import urllib.parse
import urllib.request
from pathlib import Path

try:
    import pyodbc
except ImportError:
    print("ERRO: pyodbc nao instalado. Rode: py -m pip install pyodbc", file=sys.stderr)
    sys.exit(1)

SQL_COPT_SS_ACCESS_TOKEN = 1256


def get_refresh_token_access() -> str | None:
    """Troca FABRIC_REFRESH_TOKEN por access token. Usado em CI sem az CLI."""
    refresh = os.environ.get("FABRIC_REFRESH_TOKEN", "").strip()
    tenant = os.environ.get("FABRIC_TENANT_ID", "").strip()
    client = os.environ.get("FABRIC_CLIENT_ID", "").strip() or "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
    if not refresh or not tenant:
        return None
    url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    body = urllib.parse.urlencode({
        "client_id": client,
        "scope": "https://database.windows.net/.default offline_access",
        "grant_type": "refresh_token",
        "refresh_token": refresh,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[auth] refresh token flow falhou: {e}", file=sys.stderr)
        return None
    new_refresh = data.get("refresh_token")
    if new_refresh:
        try:
            (Path(__file__).parent / ".new_refresh_token").write_text(new_refresh, encoding="utf-8")
        except Exception:
            pass
    return data.get("access_token")

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "fabric_config.json"
COMPANIES_JSON = ROOT / "companies_data.json"
DASHBOARD_JS = ROOT / "dashboard_full_data.js"

DRIVER = "{ODBC Driver 18 for SQL Server}"


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        print(f"ERRO: {CONFIG_PATH} nao existe.", file=sys.stderr)
        sys.exit(1)
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def get_az_token() -> bytes | None:
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
    except Exception as e:
        print(f"[auth] az token indisponivel ({e}); caindo para Interactive", file=sys.stderr)
        return None


def connect(cfg: dict) -> "pyodbc.Connection":
    print(f"[fabric] conectando em {cfg['sql_endpoint']} / {cfg['database']} ...")
    base_conn = (
        f"Driver={DRIVER};"
        f"Server={cfg['sql_endpoint']},1433;"
        f"Database={cfg['database']};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    # 1) az CLI (local)
    token_struct = get_az_token()
    if token_struct:
        print("[auth] usando access token do az CLI")
        return pyodbc.connect(base_conn, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    # 2) refresh token flow (CI)
    raw = get_refresh_token_access()
    if raw:
        print("[auth] usando FABRIC_REFRESH_TOKEN")
        enc = raw.encode("utf-16-le")
        ts = struct.pack("=i", len(enc)) + enc
        return pyodbc.connect(base_conn, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: ts})
    # 3) interativo
    print("[auth] usando ActiveDirectoryInteractive")
    return pyodbc.connect(base_conn + "Authentication=ActiveDirectoryInteractive;")


SQL = """
WITH active_domains AS (
    SELECT d.id, d.name, d.angel_id, d.integration_id, d.partner_id
    FROM dbo.ODBC_Domains d
    WHERE d.modulos LIKE '%vendas%'
      AND (d.partner_id IS NULL OR d.partner_id NOT IN (
          'ff66c2f1-1f9f-456c-9308-028e48c89582',
          '25fec57c-620c-4ecd-ae7d-cd4fee27b158'
      ))
      AND LOWER(d.name) NOT LIKE '%teste%'
),
ranked_companies AS (
    SELECT
        c.domain_id,
        c.tax_document,
        c.social_name,
        c.company_name,
        c.created_at,
        ROW_NUMBER() OVER (PARTITION BY c.domain_id ORDER BY c.created_at ASC) AS rn
    FROM dbo.ODBC_Companies c
    WHERE c.domain_id IN (SELECT id FROM active_domains)
)
SELECT
    d.id                 AS domain_id,
    d.name               AS domain_name,
    rc.tax_document      AS cnpj,
    rc.social_name       AS razao_social,
    rc.company_name      AS company_name,
    rc.rn                AS row_num,
    a.name               AS angel_name,
    i.name               AS integration_name,
    p.name               AS partner_name,
    sub.plan_name        AS plano,
    inv_last.total_cents AS last_invoice_cents
FROM active_domains d
JOIN ranked_companies rc           ON rc.domain_id = d.id
LEFT JOIN dbo.ODBC_Angels       a  ON a.id = d.angel_id
LEFT JOIN dbo.ODBC_Integrations i  ON i.id = d.integration_id
LEFT JOIN dbo.ODBC_Partners     p  ON p.id = d.partner_id
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
    JOIN (
        SELECT DISTINCT id, customer_id, total_cents, status, created_at_iso_TIMESTAMP
        FROM dbo.iugu_invoices
    ) inv ON inv.customer_id = sc2.Customer_ID_Iugu
    WHERE sc2.domain_id = d.id
      AND inv.status = 'paid'
    ORDER BY inv.created_at_iso_TIMESTAMP DESC
) inv_last
"""


SQL_PEDIDOS_2026 = """
SELECT DISTINCT domainId
FROM dbo.MongoDB_Pedidos_Geral
WHERE settings_createdAt_TIMESTAMP >= '2026-01-01'
"""


def fetch_rows(conn: "pyodbc.Connection") -> list[dict]:
    print("[fabric] rodando query (domains + companies + joins)")
    cur = conn.cursor()
    cur.execute(SQL)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(rows)} linhas retornadas")
    return rows


def fetch_domains_com_pedidos_2026(conn: "pyodbc.Connection") -> set[str]:
    print("[fabric] rodando query (dominios com pedidos em 2026)")
    cur = conn.cursor()
    cur.execute(SQL_PEDIDOS_2026)
    ids = set()
    for r in cur.fetchall():
        v = r[0]
        if v is None or str(v).strip() == "":
            continue
        try:
            ids.add(str(int(v)))
        except (ValueError, TypeError):
            ids.add(str(v).strip())
    print(f"[fabric] {len(ids)} dominios com pedidos em 2026")
    return ids


def _s(v) -> str:
    return "" if v is None else str(v)


def build_companies(rows: list[dict], domains_2026: set[str] | None = None) -> list[dict]:
    merged = []
    stats = {"matriz": 0, "filial": 0, "com_anjo": 0, "com_integ": 0,
             "com_canal": 0, "com_cnpj": 0, "com_plano": 0, "com_valor": 0, "com_ped2026": 0}
    for row in rows:
        did = _s(row["domain_id"])
        rn = int(row.get("row_num") or 1)
        is_filial = rn > 1

        domain_name = _s(row.get("domain_name"))
        company_name = _s(row.get("company_name"))
        anjo = _s(row.get("angel_name"))
        integ = _s(row.get("integration_name"))
        canal = _s(row.get("partner_name"))
        cnpj = _s(row.get("cnpj"))
        razao = _s(row.get("razao_social"))
        plano = _s(row.get("plano"))

        cents = row.get("last_invoice_cents")
        try:
            valor_mensal = float(cents) / 100.0 if cents not in (None, "") else 0.0
        except (TypeError, ValueError):
            valor_mensal = 0.0

        if is_filial:
            name = company_name or f"{domain_name} - Filial {rn}"
            nome_fantasia = company_name or domain_name
            matriz_name = domain_name
            stats["filial"] += 1
        else:
            name = domain_name or company_name
            nome_fantasia = company_name or domain_name
            matriz_name = ""
            stats["matriz"] += 1

        if anjo: stats["com_anjo"] += 1
        if integ: stats["com_integ"] += 1
        if canal: stats["com_canal"] += 1
        if cnpj: stats["com_cnpj"] += 1
        if plano: stats["com_plano"] += 1
        if valor_mensal: stats["com_valor"] += 1
        tem_pedidos_2026 = did in domains_2026 if domains_2026 else True
        if tem_pedidos_2026: stats["com_ped2026"] += 1

        merged.append({
            "domain_id": did,
            "name": name,
            "nome_fantasia": nome_fantasia,
            "cnpj": cnpj,
            "razao_social": razao,
            "anjo": anjo,
            "canal": canal,
            "integracao": integ,
            "plano": plano,
            "valor_mensal": round(valor_mensal, 2),
            "is_filial": is_filial,
            "isMatriz": not is_filial,
            "matriz_name": matriz_name,
            "status": "Ativa",
            "pedidos": 0,
            "matrizId": did if is_filial else "",
            "temPedidos2026": tem_pedidos_2026,
        })
    print(f"[build] {len(merged)} linhas | matriz:{stats['matriz']} filial:{stats['filial']} "
          f"| anjo:{stats['com_anjo']} integ:{stats['com_integ']} "
          f"canal:{stats['com_canal']} cnpj:{stats['com_cnpj']} "
          f"plano:{stats['com_plano']} valor:{stats['com_valor']} ped2026:{stats['com_ped2026']}")
    return merged


def write_companies_json(merged: list[dict]) -> None:
    COMPANIES_JSON.write_text(
        json.dumps(merged, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[write] {COMPANIES_JSON.name} ({len(merged)} linhas)")


def update_dashboard_js(merged: list[dict]) -> None:
    content = DASHBOARD_JS.read_text(encoding="utf-8")
    new_line = "const COMPANIES_DATA = " + json.dumps(merged, ensure_ascii=False) + ";"
    pattern = re.compile(r"^const COMPANIES_DATA\s*=.*?;\s*$", re.MULTILINE)
    if not pattern.search(content):
        raise RuntimeError("Nao encontrei 'const COMPANIES_DATA = ...;' em dashboard_full_data.js")
    new_content = pattern.sub(lambda _m: new_line, content, count=1)
    DASHBOARD_JS.write_text(new_content, encoding="utf-8")
    print(f"[write] {DASHBOARD_JS.name} (COMPANIES_DATA atualizado)")


def main() -> None:
    cfg = load_config()
    with connect(cfg) as conn:
        rows = fetch_rows(conn)
        domains_2026 = fetch_domains_com_pedidos_2026(conn)
    merged = build_companies(rows, domains_2026)
    write_companies_json(merged)
    print("[ok] companies_data.json atualizado. merge_data.py vai compor o dashboard_full_data.js.")


if __name__ == "__main__":
    main()
