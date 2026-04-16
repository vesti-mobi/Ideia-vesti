"""
Detecta churn de VestiPago e calcula valor perdido (PIX/Cartao) por empresa.

Churn = empresa ativa cujo ultimo pedido VP foi ha pelo menos N meses.
Valor perdido = soma VP no ultimo mes completo antes do churn.
Se o mes anterior ao ultVP nao tinha transacoes, fallback = mes do ultVP.

Saidas:
  - churn_vestipago_data.json  -> lista pronta para CHURN_DATA do dashboard
  - churn_vestipago.csv        -> detalhado (uma linha por empresa)
  - churn_vestipago_pivot.csv  -> agregado por AnoMes x CS

Rodar:
    py fetch_churn_vestipago.py
    py fetch_churn_vestipago.py --meses 2
"""

from __future__ import annotations

import argparse
import csv
import json
import datetime as dt
import sys
from collections import Counter
from pathlib import Path

from fetch_fabric import load_config, connect

ROOT = Path(__file__).parent
OUT_JSON = ROOT / "churn_vestipago_data.json"
OUT_DETAIL = ROOT / "churn_vestipago.csv"
OUT_PIVOT = ROOT / "churn_vestipago_pivot.csv"

START_MONTH = (2026, 1)

# Query 1: detecta empresas em churn (ultimo VP antigo, mas ainda fazendo pedidos)
SQL_CHURN = """
WITH active_domains AS (
    SELECT d.id, d.name, d.angel_id
    FROM dbo.ODBC_Domains d
    WHERE d.modulos LIKE '%%vendas%%'
      AND (d.partner_id IS NULL OR d.partner_id NOT IN (
          'ff66c2f1-1f9f-456c-9308-028e48c89582',
          '25fec57c-620c-4ecd-ae7d-cd4fee27b158'
      ))
      AND LOWER(d.name) NOT LIKE '%%teste%%'
),
last_vp AS (
    SELECT
        p.domainId                              AS domain_id,
        MAX(p.settings_createdAt_TIMESTAMP)     AS last_vp_at,
        COUNT(*)                                AS total_pedidos_vp
    FROM dbo.MongoDB_Pedidos_Geral p
    WHERE p.payment_method IN ('PIX','CREDIT_CARD')
    GROUP BY p.domainId
),
last_any AS (
    SELECT
        p.domainId                              AS domain_id,
        MAX(p.settings_createdAt_TIMESTAMP)     AS last_any_at,
        COUNT(*)                                AS total_pedidos
    FROM dbo.MongoDB_Pedidos_Geral p
    GROUP BY p.domainId
)
SELECT
    d.id           AS domain_id,
    d.name         AS domain_name,
    a.name         AS cs_name,
    lv.last_vp_at,
    lv.total_pedidos_vp,
    la.last_any_at,
    la.total_pedidos
FROM active_domains d
JOIN last_vp lv ON lv.domain_id = d.id
LEFT JOIN last_any la ON la.domain_id = d.id
LEFT JOIN dbo.ODBC_Angels a ON a.id = d.angel_id
"""

# Query 2: valores VP por mes por empresa (para calcular valor perdido com split PIX/Cartao)
SQL_VP_MONTHLY = """
SELECT
    domainId,
    FORMAT(settings_createdAt_TIMESTAMP,'yyyy-MM') AS mes,
    payment_method,
    SUM(summary_total) AS valor,
    COUNT(*) AS qt
FROM dbo.MongoDB_Pedidos_Geral
WHERE domainId IN ({ids})
  AND payment_method IN ('PIX','CREDIT_CARD')
GROUP BY domainId, FORMAT(settings_createdAt_TIMESTAMP,'yyyy-MM'), payment_method
"""


def _to_date(v) -> dt.date | None:
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    s = str(v).strip().replace("Z", "")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s[: len(fmt) + 6 if "%f" in fmt else len(fmt)], fmt).date()
        except ValueError:
            continue
    try:
        return dt.datetime.fromisoformat(s).date()
    except Exception:
        return None


def _add_month(y: int, m: int) -> tuple[int, int]:
    return (y + 1, 1) if m == 12 else (y, m + 1)


def _prev_month(yyyy_mm: str) -> str:
    y, m = int(yyyy_mm[:4]), int(yyyy_mm[5:7])
    m -= 1
    if m < 1:
        m = 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def _fmt_month(yyyy_mm: str) -> str:
    nomes = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
    y, m = int(yyyy_mm[:4]), int(yyyy_mm[5:7])
    return f"{nomes[m-1]}/{y%100:02d}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--meses", type=int, default=2,
                    help="Corte: ultimo pedido VP ha pelo menos N meses (default 2)")
    ap.add_argument("--min-pedidos", type=int, default=3,
                    help="Minimo de pedidos VP historicos (default 3)")
    args = ap.parse_args()

    cfg = load_config()
    with connect(cfg) as conn:
        # === STEP 1: detectar empresas em churn ===
        print("[vp-churn] rodando query de deteccao...")
        cur = conn.cursor()
        cur.execute(SQL_CHURN)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        print(f"[vp-churn] {len(rows)} empresas com historico VestiPago")

        today = dt.date.today()
        cutoff_month = dt.date(today.year, today.month, 1)
        # meses atras
        for _ in range(args.meses):
            m = cutoff_month.month - 1
            y = cutoff_month.year
            if m < 1:
                m = 12
                y -= 1
            cutoff_month = dt.date(y, m, 1)

        start_month = dt.date(*START_MONTH, 1)
        current_month_start = today.replace(day=1)

        churn_rows = []
        for r in rows:
            last_vp = _to_date(r.get("last_vp_at"))
            if last_vp is None:
                continue
            total_vp = int(r.get("total_pedidos_vp") or 0)
            if total_vp < args.min_pedidos:
                continue
            last_vp_month = dt.date(last_vp.year, last_vp.month, 1)
            if last_vp_month >= cutoff_month:
                continue
            cy, cm = _add_month(last_vp.year, last_vp.month)
            churn_month = dt.date(cy, cm, 1)
            if churn_month < start_month or churn_month > current_month_start:
                continue

            last_any = _to_date(r.get("last_any_at"))

            churn_rows.append({
                "mes": f"{cy:04d}-{cm:02d}",
                "cs": (r.get("cs_name") or "Sem CS").strip(),
                "empresa": (r.get("domain_name") or "").strip(),
                "domainId": str(r.get("domain_id") or ""),
                "ultVP": last_vp.isoformat(),
                "ultPed": last_any.isoformat() if last_any else "",
                "totalVP": total_vp,
                "totalPed": int(r.get("total_pedidos") or 0),
            })

        churn_rows.sort(key=lambda x: (x["mes"], x["cs"], x["empresa"]))
        print(f"[vp-churn] {len(churn_rows)} empresas em churn detectadas")

        if not churn_rows:
            print("[vp-churn] nenhum churn, gerando arquivos vazios")
            OUT_JSON.write_text("[]", encoding="utf-8")
            OUT_DETAIL.write_text("", encoding="utf-8")
            OUT_PIVOT.write_text("", encoding="utf-8")
            return

        # === STEP 2: buscar valores VP mensais para calcular valor perdido ===
        domain_ids = list(set(r["domainId"] for r in churn_rows))
        ids_str = ",".join(domain_ids)
        print(f"[vp-churn] buscando valores VP mensais para {len(domain_ids)} empresas...")
        cur.execute(SQL_VP_MONTHLY.format(ids=ids_str))

        # Agrupa por (domainId, mes) -> {pix, cartao}
        vp_data: dict[tuple[str, str], dict] = {}
        for r2 in cur.fetchall():
            did = str(int(r2[0]))
            mes = r2[1]
            pm = r2[2]
            valor = float(r2[3] or 0)
            key = (did, mes)
            if key not in vp_data:
                vp_data[key] = {"pix": 0.0, "cartao": 0.0}
            if pm == "PIX":
                vp_data[key]["pix"] = valor
            else:
                vp_data[key]["cartao"] = valor

    # === STEP 3: calcular valor perdido por empresa ===
    print("[vp-churn] calculando valor perdido (PIX/Cartao)...")
    for r in churn_rows:
        did = r["domainId"]
        ult_vp_month = r["ultVP"][:7]  # yyyy-MM
        preferred = _prev_month(ult_vp_month)  # mes anterior ao ultimo VP

        d = vp_data.get((did, preferred))
        if d and (d["pix"] + d["cartao"]) > 0:
            r["valPix"] = round(d["pix"], 2)
            r["valCartao"] = round(d["cartao"], 2)
            r["valorPerdido"] = round(d["pix"] + d["cartao"], 2)
        else:
            # Fallback: mes do proprio ultimo pedido VP
            d2 = vp_data.get((did, ult_vp_month))
            if d2 and (d2["pix"] + d2["cartao"]) > 0:
                r["valPix"] = round(d2["pix"], 2)
                r["valCartao"] = round(d2["cartao"], 2)
                r["valorPerdido"] = round(d2["pix"] + d2["cartao"], 2)
                r["obs"] = f"Valor ref. {_fmt_month(ult_vp_month)} - mes do ultimo pedido VP (sem atividade em {_fmt_month(preferred)})"
            else:
                r["valPix"] = 0
                r["valCartao"] = 0
                r["valorPerdido"] = 0

    # === STEP 4: salvar JSON para o dashboard ===
    OUT_JSON.write_text(
        json.dumps(churn_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[ok] {OUT_JSON.name} ({len(churn_rows)} empresas)")

    # === STEP 5: salvar CSVs (compatibilidade) ===
    csv_fields = ["mes", "cs", "empresa", "domainId", "ultVP", "ultPed",
                  "totalVP", "totalPed", "valPix", "valCartao", "valorPerdido"]
    with OUT_DETAIL.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(churn_rows)
    print(f"[ok] {OUT_DETAIL.name} ({len(churn_rows)} linhas)")

    pivot: Counter[tuple[str, str]] = Counter()
    for r in churn_rows:
        pivot[(r["mes"], r["cs"])] += 1
    with OUT_PIVOT.open("w", encoding="utf-8-sig", newline="") as f:
        w2 = csv.writer(f)
        w2.writerow(["AnoMes", "CS", "ClientesChurnVestiPago"])
        for k, v in sorted(pivot.items()):
            w2.writerow([*k, v])
    print(f"[ok] {OUT_PIVOT.name} ({len(pivot)} linhas)")

    # Resumo
    total_perdido = sum(r["valorPerdido"] for r in churn_rows)
    total_pix = sum(r["valPix"] for r in churn_rows)
    total_car = sum(r["valCartao"] for r in churn_rows)
    com_obs = sum(1 for r in churn_rows if r.get("obs"))
    print(f"\n[resumo] {len(churn_rows)} empresas em churn | "
          f"PIX R$ {total_pix:,.2f} + Cartao R$ {total_car:,.2f} = Total R$ {total_perdido:,.2f}")
    if com_obs:
        print(f"[resumo] {com_obs} empresa(s) com fallback (valor ref. mes do ultVP)")


if __name__ == "__main__":
    main()
