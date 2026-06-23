"""Diagnostico: grao de Pedidos_Geral (1 linha/pedido ou 1/parcela?) p/ IUGU+STARK."""
import fetch_data

conn = fetch_data.connect()
cur = conn.cursor()

PROV = "('IUGU','Iugu','STARKBANK')"

print("=== linhas vs pedidos distintos (com recebivel, IUGU+STARK) ===", flush=True)
cur.execute(f"""
SELECT COUNT(*) AS linhas, COUNT(DISTINCT _id) AS pedidos
FROM dbo.MongoDB_Pedidos_Geral
WHERE payment_transaction_provider IN {PROV}
  AND payment_receivables_grossValue IS NOT NULL
""")
print(cur.fetchone(), flush=True)

print("\n=== top _id por nro de linhas ===", flush=True)
cur.execute(f"""
SELECT TOP 5 _id, COUNT(*) AS n
FROM dbo.MongoDB_Pedidos_Geral
WHERE payment_transaction_provider IN {PROV}
  AND payment_receivables_grossValue IS NOT NULL
GROUP BY _id ORDER BY COUNT(*) DESC
""")
amostra = cur.fetchall()
for r in amostra:
    print(r, flush=True)

if amostra and amostra[0][1] > 1:
    pid = amostra[0][0]
    print(f"\n=== detalhe das linhas do _id={pid} ===", flush=True)
    cur.execute(f"""
    SELECT payment_receivables_installment, payment_receivables_grossValue,
           payment_receivables_netValue, payment_receivables_dueAt, payment_receivables_paidAt
    FROM dbo.MongoDB_Pedidos_Geral
    WHERE _id = ?
    ORDER BY payment_receivables_installment
    """, pid)
    cols = [d[0] for d in cur.description]
    print(" | ".join(cols), flush=True)
    for r in cur.fetchall():
        print(" | ".join(str(x) for x in r), flush=True)

# total de linhas que entrariam na loan tape: paidAt >= 2025-06-01
print("\n=== linhas elegiveis (paidAt>=2025-06-01, IUGU+STARK) ===", flush=True)
cur.execute(f"""
SELECT COUNT(*) FROM dbo.MongoDB_Pedidos_Geral
WHERE payment_transaction_provider IN {PROV}
  AND payment_receivables_paidAt IS NOT NULL
  AND DATEADD(HOUR,-3,TRY_CAST(payment_receivables_paidAt AS DATETIME2)) >= '2025-06-01'
""")
print(cur.fetchone(), flush=True)
print("\n=== linhas elegiveis desde 2023 (paidAt>=2023-01-01) ===", flush=True)
cur.execute(f"""
SELECT COUNT(*) FROM dbo.MongoDB_Pedidos_Geral
WHERE payment_transaction_provider IN {PROV}
  AND payment_receivables_paidAt IS NOT NULL
  AND DATEADD(HOUR,-3,TRY_CAST(payment_receivables_paidAt AS DATETIME2)) >= '2023-01-01'
""")
print(cur.fetchone(), flush=True)
conn.close()
