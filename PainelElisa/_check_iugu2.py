from _fetch_fabric_base import connect, load_config
with connect(load_config()) as c:
    cur = c.cursor()
    cur.execute("SELECT TOP 3 paid_at, due_date, status, customer_id FROM dbo.iugu_invoices WHERE status='paid' AND paid_at IS NOT NULL")
    for r in cur.fetchall(): print(r)
    print("---")
    cur.execute("""
    SELECT TOP 1
      paid_at, TRY_CAST(paid_at AS DATETIME) AS pd,
      due_date, TRY_CAST(due_date AS DATE) AS dd
    FROM dbo.iugu_invoices WHERE status='paid' AND paid_at IS NOT NULL""")
    for r in cur.fetchall(): print(r)
    print("---total paid:")
    cur.execute("SELECT COUNT(*) FROM dbo.iugu_invoices WHERE status='paid' AND paid_at IS NOT NULL AND due_date IS NOT NULL")
    print(cur.fetchone())
    print("---joined com silver:")
    cur.execute("""
    SELECT COUNT(*)
    FROM dbo.iugu_invoices i
    JOIN dbo.silver_companiesativos_iugu sc ON sc.Customer_ID_Iugu = i.customer_id
    WHERE i.status='paid' AND i.paid_at IS NOT NULL""")
    print(cur.fetchone())
