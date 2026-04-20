"""
Pedidos Onlog: `delivery_provider_provider = 'onLog'` no MongoDB_Pedidos_Geral
(recomendacao do time de dados — esse campo eh o identificador real do provider
e tem historico desde janeiro/2026. `delivery_provider_name` eh o texto
mostrado ao cliente e nem sempre cita Onlog).

Output: onlog_data.json

Formato (consumido pelo template.html via merge_data -> ONLOG_DATA):
{
    "geradoEm": "...",
    "pedidos": [
        {
            "orderNumber": 3992, "dominioId": "1355848",
            "data": "2026-04-09",
            "marca": "...", "cs": "...", "cnpj": "...",
            "provider": "Vesti - OnLog Red - FASTPACK",
            "status": "SENT",
            "valor": 2683.59,
            "comEtiqueta": true,
            "etiquetaUrl": "https://...",
            "trackingCode": null,
            "cidade": "Sao Paulo", "uf": "SP", "cliente": "Joao"
        }
    ],
    "diasList": [...], "csList": [...],
    "resumo": {"nPedidos": N, "nComEtiqueta": N, "nSemEtiqueta": N,
               "valTotal": F, "nEmpresas": N}
}
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from fetch_fabric import connect, load_config

ROOT = Path(__file__).parent
COMPANIES_JSON = ROOT / "companies_data.json"
OUT_JSON = ROOT / "onlog_data.json"

SQL_ONLOG = """
SELECT
    orderNumber,
    domainId,
    companyId,
    settings_createdAt_TIMESTAMP AS data_pedido,
    delivery_provider_name AS provider,
    status_consolidatedOrderStatus AS status,
    summary_total AS valor,
    delivery_tracking_shippingLabel AS etiqueta_url,
    delivery_trackingCode AS tracking_code,
    delivery_address_city_name AS cidade,
    delivery_address_state_initials AS uf,
    customer_name AS cliente,
    status_canceled_isCanceled AS cancelado
FROM dbo.MongoDB_Pedidos_Geral
WHERE LOWER(delivery_provider_provider) = 'onlog'
  AND settings_createdAt_TIMESTAMP IS NOT NULL
ORDER BY settings_createdAt_TIMESTAMP DESC, orderNumber DESC
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
        # Matriz tem prioridade
        if c.get("isMatriz"):
            by_dom[did] = c
        elif did not in by_dom:
            by_dom[did] = c
    return by_dom


def fetch_rows(conn) -> list[dict]:
    print("[fabric] rodando query Onlog")
    cur = conn.cursor()
    cur.execute(SQL_ONLOG)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    print(f"[fabric] {len(rows)} pedidos Onlog")
    return rows


def build(rows: list[dict], companies: dict[str, dict]) -> dict:
    pedidos: list[dict] = []
    dias_set: set[str] = set()
    cs_set: set[str] = set()
    empresas_set: set[str] = set()
    n_com_etiqueta = 0
    n_sem_etiqueta = 0
    val_total = 0.0
    sem_match = 0

    for r in rows:
        dom = str(r.get("domainId") or "").strip()
        if not dom:
            continue
        try:
            dom = str(int(dom))
        except (TypeError, ValueError):
            pass

        data = r.get("data_pedido")
        data_str = ""
        if data is not None:
            if hasattr(data, "isoformat"):
                data_str = data.isoformat()[:10]
            else:
                data_str = str(data)[:10]
        if not data_str:
            continue

        etq = r.get("etiqueta_url")
        com_etiqueta = bool(etq and str(etq).strip())
        if com_etiqueta:
            n_com_etiqueta += 1
        else:
            n_sem_etiqueta += 1

        c = companies.get(dom) or {}
        if not c:
            sem_match += 1

        valor = float(r.get("valor") or 0)
        val_total += valor
        cs = (c.get("anjo") or "") if c else ""

        pedidos.append({
            "orderNumber": int(r.get("orderNumber") or 0),
            "dominioId": dom,
            "data": data_str,
            "marca": (c.get("nome_fantasia") or c.get("name") or "") if c else "",
            "cs": cs,
            "cnpj": (c.get("cnpj") or "") if c else "",
            "provider": r.get("provider") or "",
            "status": r.get("status") or "",
            "valor": round(valor, 2),
            "comEtiqueta": com_etiqueta,
            "etiquetaUrl": str(etq) if com_etiqueta else "",
            "trackingCode": r.get("tracking_code") or "",
            "cidade": r.get("cidade") or "",
            "uf": r.get("uf") or "",
            "cliente": r.get("cliente") or "",
            "cancelado": bool(r.get("cancelado")),
        })
        dias_set.add(data_str)
        empresas_set.add(dom)
        if cs:
            cs_set.add(cs)

    dias_list = sorted(dias_set, reverse=True)
    cs_list = sorted(cs_set, key=lambda s: s.lower())

    print(f"[build] {len(pedidos)} pedidos | com etiqueta: {n_com_etiqueta} | sem: {n_sem_etiqueta} | sem match: {sem_match}")
    print(f"[build] GMV Onlog: R$ {val_total:,.2f}")
    if dias_list:
        print(f"[build] periodo: {dias_list[-1]} -> {dias_list[0]}")

    return {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "pedidos": pedidos,
        "diasList": dias_list,
        "csList": cs_list,
        "resumo": {
            "nPedidos": len(pedidos),
            "nComEtiqueta": n_com_etiqueta,
            "nSemEtiqueta": n_sem_etiqueta,
            "valTotal": round(val_total, 2),
            "nEmpresas": len(empresas_set),
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
