"""Diagnostico de cobertura: vestipago_transaction_detail (fonte alternativa)."""
import fetch_data

conn = fetch_data.connect()
cur = conn.cursor()

print("=== vestipago_transaction_detail ===", flush=True)
cur.execute("""
SELECT MIN(paidAt_ts) AS min_paid, MAX(paidAt_ts) AS max_paid,
       MIN(createdAt_ts) AS min_created, MAX(createdAt_ts) AS max_created,
       COUNT(*) AS total
FROM dbo.vestipago_transaction_detail
""")
cols = [d[0] for d in cur.description]
print("RESUMO:", dict(zip(cols, cur.fetchone())), flush=True)

cur.execute("""
SELECT paid_month, COUNT(*) AS qt
FROM dbo.vestipago_transaction_detail
WHERE paid_month IS NOT NULL
GROUP BY paid_month ORDER BY paid_month
""")
print("POR MES (paid_month):", flush=True)
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}", flush=True)

# distintos method
cur.execute("SELECT method, COUNT(*) FROM dbo.vestipago_transaction_detail GROUP BY method")
print("METHODS:", [(r[0], r[1]) for r in cur.fetchall()], flush=True)
conn.close()
