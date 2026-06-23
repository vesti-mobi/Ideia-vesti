"""Diagnostico: cobertura historica de recebiveis em MongoDB_Pedidos_Geral.

Responde: ate onde (ano) cada provider tem os campos payment_receivables_*
preenchidos, pra saber se da pra montar loan tape desde 2023.
"""
import fetch_data

conn = fetch_data.connect()
cur = conn.cursor()

print("=== Pedidos_Geral: provider x ano x preenchimento de recebivel ===", flush=True)
cur.execute("""
SELECT
  payment_transaction_provider AS provider,
  YEAR(DATEADD(HOUR,-3,TRY_CAST(settings_createdAt_TIMESTAMP AS DATETIME2))) AS ano,
  COUNT(*) AS pedidos,
  SUM(CASE WHEN payment_receivables_grossValue IS NOT NULL AND CAST(payment_receivables_grossValue AS NVARCHAR(50))<>'' THEN 1 ELSE 0 END) AS com_gross,
  SUM(CASE WHEN payment_receivables_paidAt IS NOT NULL AND CAST(payment_receivables_paidAt AS NVARCHAR(50))<>'' THEN 1 ELSE 0 END) AS com_recPaid,
  SUM(CASE WHEN payment_receivables_dueAt IS NOT NULL AND CAST(payment_receivables_dueAt AS NVARCHAR(50))<>'' THEN 1 ELSE 0 END) AS com_due,
  SUM(CASE WHEN payment_paidAt IS NOT NULL AND CAST(payment_paidAt AS NVARCHAR(50))<>'' THEN 1 ELSE 0 END) AS com_orderPaid
FROM dbo.MongoDB_Pedidos_Geral
GROUP BY payment_transaction_provider,
         YEAR(DATEADD(HOUR,-3,TRY_CAST(settings_createdAt_TIMESTAMP AS DATETIME2)))
ORDER BY provider, ano
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
print(" | ".join(cols), flush=True)
for r in rows:
    print(" | ".join(str(x) for x in r), flush=True)

print("\n=== range global settings_createdAt ===", flush=True)
cur.execute("""
SELECT MIN(DATEADD(HOUR,-3,TRY_CAST(settings_createdAt_TIMESTAMP AS DATETIME2))),
       MAX(DATEADD(HOUR,-3,TRY_CAST(settings_createdAt_TIMESTAMP AS DATETIME2))),
       COUNT(*)
FROM dbo.MongoDB_Pedidos_Geral
""")
print(cur.fetchone(), flush=True)

print("\n=== payment_method distribuicao (todos providers) ===", flush=True)
cur.execute("SELECT payment_method, COUNT(*) FROM dbo.MongoDB_Pedidos_Geral GROUP BY payment_method ORDER BY 2 DESC")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}", flush=True)

conn.close()
