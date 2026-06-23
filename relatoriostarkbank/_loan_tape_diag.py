"""Diagnostico de cobertura de datas em mongodb_pedidos_recebiveis (STARKBANK)."""
import sys
import fetch_data

SQL = """
SELECT
    MIN(DATEADD(HOUR,-3,TRY_CAST(payment_receivables_paidAt AS DATETIME2))) AS min_paid,
    MAX(DATEADD(HOUR,-3,TRY_CAST(payment_receivables_paidAt AS DATETIME2))) AS max_paid,
    MIN(DATEADD(HOUR,-3,TRY_CAST(payment_receivables_dueAt  AS DATETIME2))) AS min_due,
    MAX(DATEADD(HOUR,-3,TRY_CAST(payment_receivables_dueAt  AS DATETIME2))) AS max_due,
    COUNT(*) AS total,
    SUM(CASE WHEN payment_receivables_paidAt IS NULL OR payment_receivables_paidAt='' THEN 1 ELSE 0 END) AS sem_paid
FROM dbo.mongodb_pedidos_recebiveis
WHERE payment_transaction_provider='STARKBANK'
"""

SQL_MES = """
SELECT FORMAT(DATEADD(HOUR,-3,TRY_CAST(payment_receivables_paidAt AS DATETIME2)),'yyyy-MM') AS mes,
       COUNT(*) AS qt
FROM dbo.mongodb_pedidos_recebiveis
WHERE payment_transaction_provider='STARKBANK'
  AND payment_receivables_paidAt IS NOT NULL AND payment_receivables_paidAt<>''
GROUP BY FORMAT(DATEADD(HOUR,-3,TRY_CAST(payment_receivables_paidAt AS DATETIME2)),'yyyy-MM')
ORDER BY mes
"""

conn = fetch_data.connect()
cur = conn.cursor()
cur.execute(SQL)
cols = [d[0] for d in cur.description]
print("RESUMO:", dict(zip(cols, cur.fetchone())), flush=True)
cur.execute(SQL_MES)
print("POR MES (paidAt):", flush=True)
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}", flush=True)
conn.close()
