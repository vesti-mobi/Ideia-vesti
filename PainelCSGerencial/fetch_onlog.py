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
WITH fechamento_agg AS (
    -- Um pedido pode ter N linhas de fechamento (divisao em volumes).
    -- Soma ValorPostagem (string BR "29,77" -> float) por CodigoVolume.
    SELECT
        CodigoVolume,
        SUM(TRY_CAST(REPLACE(ValorPostagem, ',', '.') AS FLOAT)) AS postagem_onlog,
        MAX(Operador) AS operador_fech,
        MAX(Modalidade) AS modalidade_fech
    FROM dbo.sheets_onlog_fechamento
    WHERE CodigoVolume IS NOT NULL AND CodigoVolume <> ''
    GROUP BY CodigoVolume
),
descritivo_agg AS (
    -- Fallback: sheets_onlog_descritivo tem Valor_Onlog por pedido (key: domainId+orderNumber).
    -- Usado quando o fechamento nao tem a linha do pedido.
    SELECT
        CONCAT(CAST(domainId AS VARCHAR), CAST(orderNumber AS VARCHAR)) AS key_dom_order,
        MAX(Valor_Onlog) AS valor_onlog_descr
    FROM dbo.sheets_onlog_descritivo
    WHERE domainId > 0 AND orderNumber > 0
    GROUP BY CONCAT(CAST(domainId AS VARCHAR), CAST(orderNumber AS VARCHAR))
)
SELECT
    p.orderNumber,
    p.domainId,
    p.companyId,
    p.settings_createdAt_TIMESTAMP AS data_pedido,
    p.delivery_provider_name AS provider,
    p.status_consolidatedOrderStatus AS status,
    p.summary_total AS valor,
    p.delivery_tracking_shippingLabel AS etiqueta_url,
    p.delivery_trackingCode AS tracking_code,
    p.delivery_address_city_name AS cidade,
    p.delivery_address_state_initials AS uf,
    p.customer_name AS cliente,
    p.status_canceled_isCanceled AS cancelado,
    p.delivery_provider_value AS cotacao_bia,
    f.postagem_onlog,
    f.operador_fech,
    f.modalidade_fech,
    d.valor_onlog_descr
FROM dbo.MongoDB_Pedidos_Geral p
LEFT JOIN fechamento_agg f
    ON f.CodigoVolume = CONCAT(p.domainId, '_', p.orderNumber)
LEFT JOIN descritivo_agg d
    ON d.key_dom_order = CONCAT(CAST(p.domainId AS VARCHAR), CAST(p.orderNumber AS VARCHAR))
WHERE LOWER(p.delivery_provider_provider) = 'onlog'
  AND p.settings_createdAt_TIMESTAMP IS NOT NULL
ORDER BY p.settings_createdAt_TIMESTAMP DESC, p.orderNumber DESC
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

        # Colunas do painel Onlog Descritivo:
        #   Cotacao BIA    = delivery_provider_value
        #   Valor Onlog    = sheets_onlog_descritivo.Valor_Onlog (o que a Onlog cobra)
        #   Valor Postagem = SUM(sheets_onlog_fechamento.ValorPostagem) (custo de postagem real)
        #   Margem Onlog   = Valor Onlog - Valor Postagem
        bia = r.get("cotacao_bia")
        bia_f = float(bia) if bia is not None else None
        post = r.get("postagem_onlog")
        post_f = float(post) if post is not None else None
        post_fonte = "fechamento" if post_f is not None else ""
        vo = r.get("valor_onlog_descr")
        valor_onlog = float(vo) if vo is not None else None
        margem_onlog = (valor_onlog - post_f) if (valor_onlog is not None and post_f is not None) else None

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
            "cotacaoBia": round(bia_f, 2) if bia_f is not None else None,
            "valorOnlog": round(valor_onlog, 2) if valor_onlog is not None else None,
            "valorPostagem": round(post_f, 2) if post_f is not None else None,
            "margemOnlog": round(margem_onlog, 2) if margem_onlog is not None else None,
            "operadorReal": r.get("operador_fech") or "",
            "postagemFonte": post_fonte,
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
