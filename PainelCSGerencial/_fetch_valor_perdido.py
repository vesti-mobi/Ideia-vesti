"""
Pra cada empresa em churn, soma o valor VP perdido.

Logica: usa o mes ANTERIOR ao ultimo pedido VP (ultimo mes completo antes do churn).
Se esse mes nao tiver transacoes, busca o ultimo mes completo que teve atividade VP.
"""
from fetch_fabric import load_config, connect

# (nome, domain, ultVP yyyy-mm-dd)
ITEMS = [
    ("Viotto",        1482280, "2025-12-11"),
    ("Muna",          1516190, "2025-12-15"),
    ("N-BLOOM",       1687315, "2026-01-28"),
    ("G&B Bros",      1776270, "2026-01-12"),
    ("Bling Bling",   709790,  "2026-01-06"),
    ("Little Star",   1391220, "2026-01-27"),
    ("Sued T-Shirt",  1436061, "2026-02-05"),
    ("Boise",         828876,  "2026-02-20"),
    ("Bem Me Quer",   166943,  "2026-02-10"),
]

# Busca TODOS os meses com atividade VP para cada empresa (para poder fazer fallback)
SQL = """
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


def _prev_month(yyyy_mm):
    """Retorna o mes anterior no formato yyyy-MM."""
    y, m = int(yyyy_mm[:4]), int(yyyy_mm[5:7])
    m -= 1
    if m < 1:
        m = 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def _build_month_data(raw_data, did):
    """Agrupa os dados por mes para um domain, retorna dict {mes: {pix, cartao, ...}}."""
    months = {}
    for (d, mes), vals in raw_data.items():
        if d != did:
            continue
        months[mes] = vals
    return months


def main():
    ids = ",".join(str(d[1]) for d in ITEMS)
    cfg = load_config()
    with connect(cfg) as conn:
        cur = conn.cursor()
        cur.execute(SQL.format(ids=ids))
        data = {}
        for r in cur.fetchall():
            key = (int(r[0]), r[1])
            pm = r[2]
            valor = float(r[3] or 0)
            qt = int(r[4] or 0)
            if key not in data:
                data[key] = {"pix": 0.0, "cartao": 0.0, "qt_pix": 0, "qt_cartao": 0}
            if pm == "PIX":
                data[key]["pix"] = valor
                data[key]["qt_pix"] = qt
            else:
                data[key]["cartao"] = valor
                data[key]["qt_cartao"] = qt

    print(f"{'Empresa':<18}{'Dom':<10}{'Mes ref':<10}{'Nota':<14}{'Qt VP':>8}{'Valor PIX':>16}{'Valor Cartao':>16}{'Total':>16}")
    print("-"*110)
    total_pix = total_car = 0.0
    results = []

    for name, did, ult_vp in ITEMS:
        ult_vp_month = ult_vp[:7]  # yyyy-MM
        preferred = _prev_month(ult_vp_month)  # mes anterior ao ultimo VP

        # Tenta o mes preferido (anterior ao ultimo VP)
        d = data.get((did, preferred))
        mes_usado = preferred
        nota = ""

        if d and (d["pix"] + d["cartao"]) > 0:
            nota = ""
        else:
            # Fallback: mes do proprio ultimo pedido VP (certeza que tem atividade)
            fallback_mes = ult_vp_month
            d = data.get((did, fallback_mes))
            if d and (d["pix"] + d["cartao"]) > 0:
                mes_usado = fallback_mes
                nota = "*"
            else:
                d = {"pix": 0.0, "cartao": 0.0, "qt_pix": 0, "qt_cartao": 0}
                mes_usado = preferred
                nota = "sem historico"

        total = d["pix"] + d["cartao"]
        total_pix += d["pix"]
        total_car += d["cartao"]
        qt = d["qt_pix"] + d["qt_cartao"]
        results.append((name, did, mes_usado, d, nota))

        vpix = f"R$ {d['pix']:,.2f}"
        vcar = f"R$ {d['cartao']:,.2f}"
        vtot = f"R$ {total:,.2f}"
        print(f"{name:<18}{did:<10}{mes_usado:<10}{nota:<14}{qt:>8}{vpix:>16}{vcar:>16}{vtot:>16}")

    print("-"*110)
    spix = f"R$ {total_pix:,.2f}"
    scar = f"R$ {total_car:,.2f}"
    stot = f"R$ {total_pix+total_car:,.2f}"
    print(f"{'TOTAL':<60}{spix:>16}{scar:>16}{stot:>16}")
    print()
    print("JS para CHURN_DATA (copiar valPix e valCartao):")
    for name, did, mes_usado, d, nota in results:
        total = d["pix"] + d["cartao"]
        print(f"  {name}: valPix:{d['pix']:.2f}, valCartao:{d['cartao']:.2f}, valorPerdido:{total:.2f}  (ref: {mes_usado}, {nota})")


if __name__ == "__main__":
    main()
