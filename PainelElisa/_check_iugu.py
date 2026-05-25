from _fetch_fabric_base import connect, load_config
with connect(load_config()) as c:
    cur = c.cursor()
    cur.execute("SELECT TOP 1 * FROM dbo.iugu_invoices")
    cols = [d[0] for d in cur.description]
    print("colunas iugu_invoices:")
    for c2 in cols: print(" ", c2)
