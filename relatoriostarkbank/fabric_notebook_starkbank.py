"""
Fabric Notebook — StarkBank snapshots no Lakehouse VestiLake.

Objetivo:
  - Ler invoices.js publicado no GitHub Pages (mesma fonte do CR).
  - Manter 4 tabelas Delta no schema `starkbank`:
      * starkbank.purchases            (current, MERGE por purchase_id)
      * starkbank.installments         (current, MERGE por installment_id)
      * starkbank.purchases_snapshots  (append, 1 linha por run, com snapshot_at)
      * starkbank.installments_snapshots (append, 1 linha por run, com snapshot_at)

Uso:
  - Cole este conteudo em uma celula de um Fabric Notebook anexado ao Lakehouse VestiLake.
  - Agende via Data Pipeline ou scheduler do notebook (1x/hora, em horarios
    staggered uns 10min apos a hora cheia pra pegar o invoices.js ja atualizado
    pelo GitHub Actions).
"""

import urllib.request, json, re
from datetime import datetime, timezone
from pyspark.sql import functions as F
from delta.tables import DeltaTable

INVOICES_URL = "https://raw.githubusercontent.com/vesti-mobi/Ideia-vesti/main/relatoriostarkbank/invoices.js"
SCHEMA = "starkbank"

# -------- 1) baixa invoices.js e parseia ----------
raw = urllib.request.urlopen(INVOICES_URL, timeout=60).read().decode("utf-8")
m = re.match(r"window\.INVOICES\s*=\s*(.*);\s*$", raw.strip(), re.DOTALL)
if not m:
    raise RuntimeError("invoices.js em formato inesperado")
payload = json.loads(m.group(1))
gerado_em = payload.get("geradoEm")
snap = datetime.now(timezone.utc)

# -------- 2) flatten em rows ----------
purchases, installments = [], []
for f in payload.get("faturas", []):
    p = f.get("purchase") or {}
    purchases.append({
        "purchase_id":         p.get("purchaseId"),
        "workspace_id":        f.get("workspaceId"),
        "transaction_id":      f.get("transactionId"),
        "order_id":            f.get("orderId"),
        "order_number":        f.get("orderNumber"),
        "company_id":          f.get("companyId"),
        "nome_fantasia":       f.get("nomeFantasia"),
        "customer_name":       f.get("customerName"),
        "order_date":          f.get("orderDate"),
        "antecipacao_enabled": bool(f.get("antecipacaoEnabled")),
        "purchase_status":     p.get("status"),
        "amount_cents":        int(p.get("amount") or 0),
        "fee_cents":           int(p.get("fee") or 0),
        "installment_count":   int(p.get("installmentCount") or 0),
        "card_ending":         p.get("cardEnding"),
        "holder_name":         p.get("holderName"),
        "api_created":         p.get("created"),
        "gerado_em":           gerado_em,
        "snapshot_at":         snap,
    })
    insts = sorted(p.get("installments") or [], key=lambda x: x.get("due") or "")
    for n, i in enumerate(insts, start=1):
        installments.append({
            "installment_id":     i.get("id"),
            "purchase_id":        p.get("purchaseId"),
            "installment_number": n,
            "amount_cents":       int(i.get("amount") or 0),
            "fee_cents":          int(i.get("fee") or 0),
            "due":                i.get("due"),
            "status":             i.get("status"),
            "transaction_ids":    json.dumps(i.get("transactionIds") or []),
            "gerado_em":          gerado_em,
            "snapshot_at":        snap,
        })

df_p = spark.createDataFrame(purchases)
df_i = spark.createDataFrame(installments)

# casts de tipo
df_p = (df_p
        .withColumn("api_created", F.to_timestamp("api_created"))
        .withColumn("order_date",  F.to_date("order_date"))
        .withColumn("gerado_em",   F.to_timestamp("gerado_em"))
        .withColumn("snapshot_at", F.to_timestamp("snapshot_at")))
df_i = (df_i
        .withColumn("due",         F.to_timestamp("due"))
        .withColumn("gerado_em",   F.to_timestamp("gerado_em"))
        .withColumn("snapshot_at", F.to_timestamp("snapshot_at")))

# -------- 3) schema + tabelas ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# snapshots (append-only, cada run adiciona uma "foto")
df_p.write.mode("append").format("delta").saveAsTable(f"{SCHEMA}.purchases_snapshots")
df_i.write.mode("append").format("delta").saveAsTable(f"{SCHEMA}.installments_snapshots")

# current (MERGE: mantem so o estado mais recente por id)
def merge_current(df, table, key):
    full = f"{SCHEMA}.{table}"
    if not spark.catalog.tableExists(full):
        df.write.format("delta").saveAsTable(full)
        return
    tgt = DeltaTable.forName(spark, full)
    (tgt.alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

merge_current(df_p, "purchases",    "purchase_id")
merge_current(df_i, "installments", "installment_id")

print(f"[ok] snap={snap.isoformat()} geradoEm={gerado_em} "
      f"purchases={df_p.count()} installments={df_i.count()}")
