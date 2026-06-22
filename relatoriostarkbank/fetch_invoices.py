"""
Puxa faturas/parcelas de cada pedido StarkBank direto da API Vesti.
Fonte 100% API — descobre workspaces e purchases via endpoints novos:
  - GET /workspaces                      lista workspaces (filtra marcas teste)
  - GET /workspace/{ws}/purchases        lista pedidos (paginado por cursor)
  - GET /workspace/{ws}/purchase/{id}    detalhes da purchase com installments

Enriquece com dados.js (Mongo) quando disponivel pra ganhar orderNumber/
isAntecipacao/customerName, mas nao depende dele pra descobrir purchases —
qualquer pedido na Stark aparece, mesmo que o sync Mongo esteja atrasado.

Auth: VESTIAPI_TOKEN como env var (JWT bearer de servico).
"""

import concurrent.futures as cf
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
DADOS_JS = ROOT / "dados.js"
OUT_JS = ROOT / "invoices.js"
API_BASE = "https://apivesti.vesti.mobi/payment/v1/starkbank"
URL_WORKSPACES = f"{API_BASE}/workspaces"
URL_PURCHASES = API_BASE + "/workspace/{ws}/purchases"
URL_PURCHASE = API_BASE + "/workspace/{ws}/purchase/{pur}"
MAX_WORKERS = 16

# Marcas de teste — excluidas do CR.
EXCLUDED_NAMES = {"andressa vesti", "andressa - teste"}

# --- Fabric VestiHouse warehouse (escrita via pyodbc + MERGE) ---
# Reaproveita connect() de fetch_data.py. Se a conexao falhar (ex: ambiente
# sem az CLI / sem FABRIC_REFRESH_TOKEN), ignora a escrita e segue com o JS.

DDL_PURCHASES = """
IF OBJECT_ID('dbo.starkbank_purchases','U') IS NULL
CREATE TABLE dbo.starkbank_purchases (
    purchase_id          VARCHAR(32)   NOT NULL,
    workspace_id         VARCHAR(32),
    order_id             VARCHAR(64),
    order_number         BIGINT,
    company_id           VARCHAR(64),
    nome_fantasia        VARCHAR(256),
    antecipacao_enabled  BIT,
    amount_cents         BIGINT,
    fee_cents            BIGINT,
    currency_code        VARCHAR(8),
    status               VARCHAR(32),
    funding_type         VARCHAR(32),
    network              VARCHAR(32),
    installment_count    INT,
    card_id              VARCHAR(32),
    card_ending          VARCHAR(8),
    holder_id            VARCHAR(64),
    holder_name          VARCHAR(256),
    holder_email         VARCHAR(256),
    holder_phone         VARCHAR(64),
    billing_city         VARCHAR(128),
    billing_state_code   VARCHAR(8),
    billing_country_code VARCHAR(8),
    billing_zip_code     VARCHAR(16),
    billing_street1      VARCHAR(256),
    billing_street2      VARCHAR(256),
    challenge_mode       VARCHAR(32),
    challenge_url        VARCHAR(512),
    end_to_end_id        VARCHAR(64),
    soft_descriptor      VARCHAR(256),
    source               VARCHAR(256),
    tags                 VARCHAR(4000),
    transaction_ids      VARCHAR(4000),
    metadata_json        VARCHAR(4000),
    api_created          DATETIME2(6),
    api_updated          DATETIME2(6),
    snapshot_at          DATETIME2(6)
);
"""

DDL_INSTALLMENTS = """
IF OBJECT_ID('dbo.starkbank_installments','U') IS NULL
CREATE TABLE dbo.starkbank_installments (
    installment_id   VARCHAR(32) NOT NULL,
    purchase_id      VARCHAR(32),
    installment_number INT,
    amount_cents     BIGINT,
    fee_cents        BIGINT,
    funding_type     VARCHAR(32),
    network          VARCHAR(32),
    status           VARCHAR(32),
    due              DATETIME2(6),
    nominal_due      DATETIME2(6),
    is_protected     BIT,
    tags             VARCHAR(4000),
    transaction_ids  VARCHAR(4000),
    api_created      DATETIME2(6),
    api_updated      DATETIME2(6),
    snapshot_at      DATETIME2(6)
);
"""

COLS_PURCHASES = [
    "purchase_id","workspace_id","order_id","order_number","company_id","nome_fantasia",
    "antecipacao_enabled","amount_cents","fee_cents","currency_code","status","funding_type",
    "network","installment_count","card_id","card_ending","holder_id","holder_name",
    "holder_email","holder_phone","billing_city","billing_state_code","billing_country_code",
    "billing_zip_code","billing_street1","billing_street2","challenge_mode","challenge_url",
    "end_to_end_id","soft_descriptor","source","tags","transaction_ids","metadata_json",
    "api_created","api_updated","snapshot_at",
]
COLS_INSTALLMENTS = [
    "installment_id","purchase_id","installment_number","amount_cents","fee_cents",
    "funding_type","network","status","due","nominal_due","is_protected","tags",
    "transaction_ids","api_created","api_updated","snapshot_at",
]


def _parse_dt(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def row_purchase(fat: dict, snap: datetime) -> tuple:
    p = fat["purchase"]
    return (
        p.get("purchaseId") or "", fat.get("workspaceId"), fat.get("orderId"),
        fat.get("orderNumber"), fat.get("companyId"), fat.get("nomeFantasia"),
        bool(fat.get("antecipacaoEnabled")), p.get("amount"), p.get("fee"),
        p.get("currencyCode"), p.get("status"), p.get("fundingType"), p.get("network"),
        p.get("installmentCount"), p.get("cardId"), p.get("cardEnding"),
        p.get("holderId"), p.get("holderName"), p.get("holderEmail"), p.get("holderPhone"),
        p.get("billingCity"), p.get("billingStateCode"), p.get("billingCountryCode"),
        p.get("billingZipCode"), p.get("billingStreetLine1"), p.get("billingStreetLine2"),
        p.get("challengeMode"), p.get("challengeUrl"), p.get("endToEndId"),
        p.get("softDescriptor"), p.get("source"),
        json.dumps(p.get("tags") or [], ensure_ascii=False)[:4000],
        json.dumps(p.get("transactionIds") or [], ensure_ascii=False)[:4000],
        json.dumps(p.get("metadata") or {}, ensure_ascii=False)[:4000],
        _parse_dt(p.get("apiCreated") or p.get("created")),
        _parse_dt(p.get("apiUpdated") or p.get("updated")),
        snap,
    )


def row_installment(i: dict, snap: datetime, num: int) -> tuple:
    return (
        i.get("id") or "", i.get("purchaseId"), num,
        i.get("amount"), i.get("fee"), i.get("fundingType"), i.get("network"),
        i.get("status"), _parse_dt(i.get("due")), _parse_dt(i.get("nominalDue")),
        bool(i.get("isProtected")),
        json.dumps(i.get("tags") or [], ensure_ascii=False)[:4000],
        json.dumps(i.get("transactionIds") or [], ensure_ascii=False)[:4000],
        _parse_dt(i.get("apiCreated") or i.get("created")),
        _parse_dt(i.get("apiUpdated") or i.get("updated")),
        snap,
    )


def upsert(conn, table: str, cols: list[str], rows: list[tuple], key: str) -> None:
    """DELETE pelas keys que vao ser reescritas + INSERT novos. Simples e
    idempotente — a cada run substitui o snapshot. Se quiser history,
    basta nao deletar e adicionar snapshot_at na PK logica."""
    if not rows:
        return
    cur = conn.cursor()
    keys = [r[cols.index(key)] for r in rows]
    # delete em lotes de 500 pra nao estourar limite de parametros
    for i in range(0, len(keys), 500):
        batch = keys[i:i+500]
        ph = ",".join("?" for _ in batch)
        cur.execute(f"DELETE FROM dbo.{table} WHERE {key} IN ({ph})", batch)
    placeholders = ",".join("?" for _ in cols)
    cur.fast_executemany = True
    cur.executemany(
        f"INSERT INTO dbo.{table} ({','.join(cols)}) VALUES ({placeholders})",
        rows,
    )
    conn.commit()


def _wh_connect():
    import struct, subprocess
    try:
        import pyodbc
    except ImportError:
        print("[warehouse] pyodbc nao instalado", file=sys.stderr)
        return None
    SRV = "7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com"
    base = (
        f"Driver={{ODBC Driver 18 for SQL Server}};Server={SRV},1433;"
        f"Database=VestiHouse;Encrypt=yes;TrustServerCertificate=no;"
    )
    # az CLI primeiro
    try:
        out = subprocess.run(
            ["az","account","get-access-token","--resource","https://database.windows.net/","--query","accessToken","-o","tsv"],
            capture_output=True, text=True, check=True, shell=sys.platform.startswith("win"),
        )
        tok = out.stdout.strip().encode("utf-16-le")
        ts = struct.pack("=i", len(tok)) + tok
        return pyodbc.connect(base, attrs_before={1256: ts})
    except Exception:
        pass
    # fallback FABRIC_REFRESH_TOKEN
    refresh = os.environ.get("FABRIC_REFRESH_TOKEN", "").strip()
    tenant = os.environ.get("FABRIC_TENANT_ID", "").strip()
    client = os.environ.get("FABRIC_CLIENT_ID", "").strip() or "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
    if not refresh or not tenant:
        return None
    import urllib.parse
    body = urllib.parse.urlencode({
        "client_id": client, "scope":"https://database.windows.net/.default offline_access",
        "grant_type":"refresh_token", "refresh_token": refresh,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
        data=body, headers={"Content-Type":"application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        tok = json.loads(resp.read().decode("utf-8"))["access_token"].encode("utf-16-le")
    ts = struct.pack("=i", len(tok)) + tok
    return pyodbc.connect(base, attrs_before={1256: ts})


def write_warehouse(faturas: list[dict]) -> None:
    try:
        conn = _wh_connect()
        if conn is None:
            print("[warehouse] skip (sem auth)", file=sys.stderr)
            return
    except Exception as e:
        print(f"[warehouse] skip (connect): {e}", file=sys.stderr)
        return
    # A escrita no warehouse e best-effort (snapshot lateral). Se a capacidade
    # F2 estourar (24801) no meio do MERGE, NAO pode derrubar o fetch_invoices:
    # o invoices.js (CR) ja foi escrito antes desta funcao e precisa ser
    # commitado. Por isso engolimos qualquer erro do Fabric aqui.
    try:
        cur = conn.cursor()
        cur.execute(DDL_PURCHASES)
        cur.execute(DDL_INSTALLMENTS)
        conn.commit()
        snap = datetime.now(timezone.utc)
        p_rows = [row_purchase(f, snap) for f in faturas if f.get("purchase")]
        i_rows = []
        for f in faturas:
            insts = sorted((f.get("purchase") or {}).get("installments") or [],
                           key=lambda x: x.get("due") or "")
            for n, inst in enumerate(insts, start=1):
                i_rows.append(row_installment(inst, snap, n))
        print(f"[warehouse] upsert {len(p_rows)} purchases, {len(i_rows)} installments")
        upsert(conn, "starkbank_purchases", COLS_PURCHASES, p_rows, "purchase_id")
        upsert(conn, "starkbank_installments", COLS_INSTALLMENTS, i_rows, "installment_id")
        conn.close()
        print("[warehouse] ok")
    except Exception as e:
        print(f"[warehouse] skip (escrita falhou, CR nao afetado): {e}", file=sys.stderr)


def load_enrichment_from_dados() -> tuple[dict, dict]:
    """Le dados.js pra obter orderNumber/isAntecipacao/etc por transactionId.
    Retorna (out, ws_antec_default):
      - out: indexado por transactionId (= purchase.id na Stark)
      - ws_antec_default: workspaceId -> bool, "essa workspace eh antecipadora?"
        com base na maioria dos pedidos historicos da marca em dados.js.
        Usado como fallback quando uma compra NOVA nao esta em dados.js ainda
        (lag Mongo→Fabric) — sem isso, defaulta False e marcas 100% antec
        aparecem como fluxo no dashboard ate o sync rodar."""
    if not DADOS_JS.exists():
        return {}, {}
    try:
        text = DADOS_JS.read_text(encoding="utf-8")
        m = re.match(r"window\.DADOS\s*=\s*(.*);\s*$", text.strip(), re.DOTALL)
        if not m:
            return {}, {}
        data = json.loads(m.group(1))
        out: dict[str, dict] = {}
        ws_counts: dict[str, list] = {}  # ws_id -> [n_antec, n_total] (janela recente)
        ws_counts_full: dict[str, list] = {}  # ws_id -> [n_antec, n_total] (historico todo, fallback)
        # Olha so os ultimos 14 dias pra captar migracoes de modalidade
        # (marca que era fluxo e virou antecipacao). Marcas com pouco historico
        # recente caem no fallback de historico completo.
        from datetime import date, timedelta
        cutoff = (date.today() - timedelta(days=14)).isoformat()
        for p in data.get("pedidos", []):
            ws_id = (p.get("workspaceId") or "").strip()
            antec = bool(p.get("antecipacaoEnabled"))
            order_date = (p.get("orderDate") or "")[:10]
            if ws_id:
                cf_ = ws_counts_full.setdefault(ws_id, [0, 0])
                cf_[0] += int(antec); cf_[1] += 1
                if order_date >= cutoff:
                    c = ws_counts.setdefault(ws_id, [0, 0])
                    c[0] += int(antec); c[1] += 1
            for pc in p.get("parcelas") or []:
                tid = (pc.get("transactionId") or "").strip()
                if tid and tid not in out:
                    out[tid] = {
                        "orderNumber":         p.get("orderNumber"),
                        "nomeFantasia":        p.get("nomeFantasia"),
                        "antecipacaoEnabled":  antec,
                        "customerName":        p.get("customerName"),
                        "orderDate":           p.get("orderDate"),
                        "companyId":           p.get("companyId"),
                        "domainId":            p.get("domainId"),
                    }
        # workspace antec=True se >=70% dos pedidos sao antec.
        # Prioridade: janela recente (14d, >=3 amostras). Fallback: historico
        # completo (>=3 amostras). Isso captura migracao de modalidade — marca
        # que era 100% fluxo no passado mas migrou pra antecipacao nas vendas
        # recentes deve ser classificada antec.
        ws_antec_default: dict[str, bool] = {}
        n_recente = n_fallback = 0
        all_ws = set(ws_counts.keys()) | set(ws_counts_full.keys())
        for ws in all_ws:
            rec = ws_counts.get(ws, [0, 0])
            if rec[1] >= 3:
                ws_antec_default[ws] = (rec[0] / rec[1]) >= 0.7
                n_recente += 1
            else:
                full = ws_counts_full.get(ws, [0, 0])
                if full[1] >= 3:
                    ws_antec_default[ws] = (full[0] / full[1]) >= 0.7
                    n_fallback += 1
        n_antec_ws = sum(1 for v in ws_antec_default.values() if v)
        print(f"[enrich] {n_antec_ws}/{len(ws_antec_default)} workspaces antecipadoras "
              f"(janela 14d: {n_recente}, fallback historico: {n_fallback})")
        return out, ws_antec_default
    except Exception as e:
        print(f"[enrich] falha lendo dados.js: {e}", file=sys.stderr)
        return {}, {}


def _api_get(url: str, token: str, retries: int = 3) -> dict | None:
    """GET com retry em falha transiente (timeout / 5xx / 429).
    Retorna None só se TODAS as tentativas falharem — assim oscilacao por
    erro intermitente da API nao derruba purchases silenciosamente."""
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "relatoriostarkbank/1.0",
        },
    )
    last_err = ""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            # 4xx (exceto 429) sao erros permanentes — nao adianta retentar
            if 400 <= e.code < 500 and e.code != 429:
                break
        except Exception as e:
            last_err = str(e)
        if attempt < retries - 1:
            time.sleep(1.5 * (attempt + 1))
    print(f"[api] {url} falhou apos {retries}x: {last_err}", file=sys.stderr)
    return None


def list_workspaces(token: str) -> list[dict]:
    """Lista workspaces Stark (paginado via cursor), filtrando marcas de teste.
    A API limita a 100 workspaces por page — sem paginacao, marcas alem da
    100a sumiam do invoices.js (ex: Alcance Jeans em 14/05/2026)."""
    workspaces: list[dict] = []
    cursor: str | None = None
    pages = 0
    while True:
        url = URL_WORKSPACES + (f"?cursor={cursor}" if cursor else "")
        resp = _api_get(url, token)
        if not resp or not resp.get("success"):
            break
        msg = resp.get("message") or {}
        page = msg.get("workspaces") or []
        workspaces.extend(page)
        cursor = msg.get("cursor") or None
        pages += 1
        if not cursor or pages > 100:
            break
    out = []
    for w in workspaces:
        nome = (w.get("name") or "").strip().lower()
        if nome in EXCLUDED_NAMES:
            continue
        out.append(w)
    return out


def list_purchases(ws_id: str, token: str) -> tuple[list[dict], bool]:
    """Lista todos os pedidos da workspace (paginado via cursor).
    Retorna (purchases, ok) onde ok=False sinaliza que a paginacao foi
    interrompida por erro — main usa pra NAO sobrescrever invoices.js
    com versao incompleta."""
    purchases: list[dict] = []
    cursor: str | None = None
    pages = 0
    while True:
        url = URL_PURCHASES.format(ws=ws_id)
        if cursor:
            url += f"?cursor={cursor}"
        resp = _api_get(url, token)
        if not resp or not resp.get("success"):
            print(f"[api]   workspace {ws_id} paginacao interrompida em page {pages+1}", file=sys.stderr)
            return purchases, False
        msg = resp.get("message") or {}
        page = msg.get("purchases") or []
        purchases.extend(page)
        cursor = msg.get("cursor") or None
        pages += 1
        if not cursor or pages > 200:
            break
    return purchases, True


def fetch_purchase(ws_id: str, pur_id: str, token: str) -> dict | None:
    """Busca detalhes de uma purchase (com installments)."""
    return _api_get(URL_PURCHASE.format(ws=ws_id, pur=pur_id), token)


def parse_purchase_tags(tags: list[str]) -> dict:
    """Extrai companyId / orderId / customerId dos tags."""
    out: dict = {}
    for t in tags or []:
        if t.startswith("company_"):
            out["companyId"] = t[len("company_"):]
        elif t.startswith("order_"):
            out["orderId"] = t[len("order_"):]
        elif t.startswith("customer_"):
            out["customerId"] = t[len("customer_"):]
    return out


def extract(resp: dict) -> dict | None:
    if not resp or not resp.get("success"):
        return None
    pur = (resp.get("message") or {}).get("purchase") or {}
    if not pur:
        return None
    insts = []
    for i in pur.get("installments") or []:
        insts.append({
            "id":             str(i.get("id") or ""),
            "amount":         int(i.get("amount") or 0),
            "fee":            int(i.get("fee") or 0),
            "due":            i.get("due") or "",
            "nominalDue":     i.get("nominalDue") or "",
            "status":         i.get("status") or "",
            "fundingType":    i.get("fundingType") or "",
            "network":        i.get("network") or "",
            "isProtected":    bool(i.get("isProtected")),
            "tags":           i.get("tags") or [],
            "transactionIds": i.get("transactionIds") or [],
            "apiCreated":     i.get("apiCreated") or i.get("created") or "",
            "apiUpdated":     i.get("apiUpdated") or i.get("updated") or "",
        })
    return {
        "purchaseId":         str(pur.get("id") or ""),
        "status":             pur.get("status") or "",
        "amount":             int(pur.get("amount") or 0),
        "fee":                int(pur.get("fee") or 0),
        "currencyCode":       pur.get("currencyCode") or "",
        "installmentCount":   int(pur.get("installmentCount") or 0),
        "fundingType":        pur.get("fundingType") or "",
        "network":            pur.get("network") or "",
        "cardId":             pur.get("cardId") or "",
        "cardEnding":         pur.get("cardEnding") or "",
        "holderId":           pur.get("holderId") or "",
        "holderName":         pur.get("holderName") or "",
        "holderEmail":        pur.get("holderEmail") or "",
        "holderPhone":        pur.get("holderPhone") or "",
        "billingCity":        pur.get("billingCity") or "",
        "billingStateCode":   pur.get("billingStateCode") or "",
        "billingCountryCode": pur.get("billingCountryCode") or "",
        "billingZipCode":     pur.get("billingZipCode") or "",
        "billingStreetLine1": pur.get("billingStreetLine1") or "",
        "billingStreetLine2": pur.get("billingStreetLine2") or "",
        "challengeMode":      pur.get("challengeMode") or "",
        "challengeUrl":       pur.get("challengeUrl") or "",
        "endToEndId":         pur.get("endToEndId") or "",
        "softDescriptor":     pur.get("softDescriptor") or "",
        "source":             pur.get("source") or "",
        "tags":               pur.get("tags") or [],
        "transactionIds":     pur.get("transactionIds") or [],
        "metadata":           pur.get("metadata") or {},
        "created":            pur.get("created") or "",
        "apiCreated":         pur.get("apiCreated") or pur.get("created") or "",
        "apiUpdated":         pur.get("apiUpdated") or pur.get("updated") or "",
        "installments":       insts,
    }


def load_previous_invoices() -> dict[str, dict]:
    """Le invoices.js anterior e indexa por transactionId. Usado pra fazer
    merge: se uma purchase falhou nesta rodada, preserva a versao anterior
    em vez de descartar — eliminando a oscilacao nos KPIs do CR."""
    if not OUT_JS.exists():
        return {}
    try:
        text = OUT_JS.read_text(encoding="utf-8")
        m = re.match(r"window\.INVOICES\s*=\s*(.*);\s*$", text.strip(), re.DOTALL)
        if not m:
            return {}
        data = json.loads(m.group(1))
        return {
            f.get("transactionId"): f
            for f in (data.get("faturas") or [])
            if f.get("transactionId")
        }
    except Exception as e:
        print(f"[merge] falha lendo invoices.js anterior: {e}", file=sys.stderr)
        return {}


def main() -> None:
    token = os.environ.get("VESTIAPI_TOKEN", "").strip()
    if not token:
        print("ERRO: defina VESTIAPI_TOKEN", file=sys.stderr)
        sys.exit(1)

    # 1) lista workspaces (filtrando marcas de teste)
    print("[api] listando workspaces...")
    workspaces = list_workspaces(token)
    print(f"[api] {len(workspaces)} workspaces ativas")

    # 2) lista todas as purchases por workspace (paginadas) — EM PARALELO.
    # A listagem por workspace e independente, entao roda em ThreadPool. Os
    # resultados sao reconstruidos na ORDEM ORIGINAL dos workspaces pra que o
    # invoices.js final fique identico (diff git minimo).
    def _list_ws(ws: dict) -> tuple:
        ws_id = ws.get("id")
        ws_name = ws.get("name") or ""
        purchases, ok = list_purchases(ws_id, token)
        return (ws_id, ws_name, purchases, ok)

    tarefas: list[dict] = []
    ws_pagination_falhou: list[str] = []
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        resultados_ws = list(ex.map(_list_ws, workspaces))
    for ws_id, ws_name, purchases, ok in resultados_ws:
        marker = "" if ok else " [PAGINACAO INCOMPLETA]"
        print(f"[api]   {ws_name}: {len(purchases)} purchases{marker}")
        if not ok:
            ws_pagination_falhou.append(ws_name or ws_id)
        for p in purchases:
            tarefas.append({
                "ws_id": ws_id,
                "ws_name": ws_name,
                "purchase_id": p.get("id"),
                "summary": p,  # preserva tags, holderName, etc. caso enriquecimento Mongo falte
            })
    print(f"[api] total purchases: {len(tarefas)} (workspaces com paginacao incompleta: {len(ws_pagination_falhou)})")

    # 3) carrega enriquecimento Mongo (opcional)
    enrich, ws_antec_default = load_enrichment_from_dados()
    print(f"[enrich] {len(enrich)} entradas em dados.js (orderNumber/isAntec/...)")

    # 4) busca detalhes (com installments) em paralelo (com retry interno em _api_get)
    def _process(t: dict) -> dict | None:
        resp = fetch_purchase(t["ws_id"], t["purchase_id"], token)
        data = extract(resp)
        if not data:
            return None
        summ = t.get("summary") or {}
        tags_parsed = parse_purchase_tags(summ.get("tags", []))
        e = enrich.get(t["purchase_id"], {})
        order_date = e.get("orderDate") or (summ.get("created") or "")[:10]
        # antec: usa o valor do enrichment quando achou; caso contrario, cai
        # no default da workspace (maioria historica). Evita classificar como
        # fluxo compras recentes que ainda nao chegaram em dados.js (lag).
        if "antecipacaoEnabled" in e:
            antec = bool(e["antecipacaoEnabled"])
        else:
            antec = ws_antec_default.get(t["ws_id"], False)
        return {
            "workspaceId":         t["ws_id"],
            "transactionId":       t["purchase_id"],
            "orderId":             e.get("orderId") or tags_parsed.get("orderId", ""),
            "orderNumber":         e.get("orderNumber"),
            "companyId":           e.get("companyId") or tags_parsed.get("companyId", ""),
            "nomeFantasia":        e.get("nomeFantasia") or t["ws_name"],
            "domainId":            e.get("domainId", ""),
            "orderDate":           order_date,
            "customerName":        e.get("customerName") or summ.get("holderName") or "",
            "antecipacaoEnabled":  antec,
            "purchase":            data,
        }

    faturas_novas: list[dict] = []
    tids_falharam: list[str] = []
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for t, r in zip(tarefas, ex.map(_process, tarefas)):
            if r is not None:
                faturas_novas.append(r)
            else:
                tids_falharam.append(t["purchase_id"])
    print(f"[api] {len(faturas_novas)} faturas obtidas (falhas individuais: {len(tids_falharam)})")

    # 5) MERGE com invoices.js anterior. A API `/workspace/{ws}/purchases`
    # so lista compras em aberto/recentes — quando uma parcela e paga, a
    # purchase some da listagem. Se simplesmente preservassemos o anterior,
    # o status ficaria congelado em "created" pra sempre, inflando o CR.
    # Solucao: pra cada transactionId do anterior que NAO veio na rodada,
    # re-buscar individualmente via /workspace/{ws}/purchase/{tid} (esse
    # endpoint sempre devolve status atual). Se a re-busca tb falhar, aí
    # sim preservamos como ultimo fallback.
    previo = load_previous_invoices()
    print(f"[merge] {len(previo)} faturas no invoices.js anterior")
    novo_index = {f["transactionId"]: f for f in faturas_novas}
    faltantes = [(tid, f) for tid, f in previo.items() if tid not in novo_index]
    print(f"[merge] {len(faltantes)} faturas ausentes na rodada — re-buscando individualmente")

    def _refresh(item: tuple) -> tuple:
        tid, antiga = item
        ws_id = antiga.get("workspaceId")
        if not ws_id:
            return (tid, antiga, "sem-workspace")
        resp = fetch_purchase(ws_id, tid, token)
        data = extract(resp)
        if not data:
            return (tid, antiga, "falhou")
        # preserva enriquecimento (orderNumber, nomeFantasia, antec, etc.)
        # e atualiza somente o bloco purchase com status fresco.
        atualizada = dict(antiga)
        atualizada["purchase"] = data
        return (tid, atualizada, "ok")

    refreshed: dict[str, dict] = {}
    n_ok = n_stale = 0
    if faltantes:
        with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for tid, fat, status in ex.map(_refresh, faltantes):
                refreshed[tid] = fat
                if status == "ok":
                    n_ok += 1
                else:
                    n_stale += 1
    print(f"[merge] re-fetch: {n_ok} atualizadas, {n_stale} preservadas como fallback (re-fetch falhou)")

    merged_index = dict(refreshed)       # antigas (atualizadas via re-fetch ou fallback)
    merged_index.update(novo_index)      # sobrescreve com as da rodada principal

    faturas = list(merged_index.values())

    # 5.1) Re-aplica ws_antec_default em TODAS as faturas (inclusive as
    # preservadas do `previo`). Sem isso, faturas que ja estavam no
    # invoices.js anterior com antec=False (porque foram geradas antes
    # do fix de classificacao por workspace, ou estavam fora do enrich
    # naquela rodada) ficam congeladas em False — fazendo a marca
    # aparecer como Fluxo no CR mesmo que a workspace seja antec.
    # So promove False->True (nunca rebaixa) pra nao reclassificar
    # marcas que migraram de antec pra fluxo.
    promovidas = 0
    for f in faturas:
        if f.get("antecipacaoEnabled"):
            continue
        if ws_antec_default.get(f.get("workspaceId")):
            f["antecipacaoEnabled"] = True
            promovidas += 1
    print(f"[antec-fallback] {promovidas} faturas promovidas a antec=True via ws_antec_default")

    # 6) Guard de qualidade: se a rodada degradou demais (perdeu >10% em
    # relacao ao anterior) E houve falhas, aborta a escrita pra nao
    # publicar dashboard pior. Primeira execucao (sem anterior) passa.
    if previo:
        delta = len(faturas) - len(previo)
        if delta < 0 and abs(delta) > 0.10 * len(previo):
            print(
                f"[guard] ABORTADO: total caiu de {len(previo)} pra {len(faturas)} "
                f"({delta}, {abs(delta)/len(previo)*100:.1f}%). Mantendo invoices.js anterior. "
                f"WS com paginacao incompleta: {ws_pagination_falhou}",
                file=sys.stderr,
            )
            sys.exit(2)

    payload = {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "faturas": faturas,
    }
    OUT_JS.write_text(
        "window.INVOICES = " + json.dumps(payload, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    size_kb = OUT_JS.stat().st_size / 1024
    print(f"[write] {OUT_JS.name} ({len(faturas)} faturas, {size_kb:.1f}KB)")
    # escreve tb no VestiHouse (tabelas starkbank_purchases / starkbank_installments)
    write_warehouse(faturas)


if __name__ == "__main__":
    main()
