# ===========================================================================
#  Fabric Notebook — Métricas Vesti Pago
#  Ingestão da API Vesti  ->  tabela Delta no Lakehouse
#  Endpoint: payment/v1/transaction-detail/orders
#  São 4 células. Cole cada bloco "# CELL N" como uma célula separada (PySpark),
#  com um Lakehouse anexado na notebook.
# ===========================================================================


# CELL 1 — token + parâmetros --------------------------------------------------
import requests, time

BASE  = "https://apivesti.vesti.mobi/payment/v1/transaction-detail/orders"
LIMIT = 500
TABLE = "vestipago_transaction_detail"

# Token da API Vesti (em pedaços pra não quebrar no paste).
# Em produção, troque por leitura do Key Vault e apague esta string.
TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczo"
    "vL2FwaXZlc3RpLWRldi52ZXN0aS5tb2JpL2FwcHZlbmRhcy92MS9sb2d"
    "pbiIsImlhdCI6MTcyMzYzMTU2OCwibmJmIjoxNzIzNjMxNTY4LCJqdGk"
    "iOiI2cGtQMnZITkNZb2cweFdLIiwic3ViIjoiOGEzNGVkNGItMGNlNC0"
    "0YTZhLTliZjYtNjMyYTJiMDkzZGIyIiwicHJ2IjoiOTMzYTE1Y2RmNjU"
    "3Yzg0ODQ5NTk5NzMzYjMwZjA1MDBjYmZhNGVhMyIsImFwcCI6eyJzY2h"
    "lbWVfdXJsIjpudWxsLCJkb21haW5faWQiOjIzNDZ9LCJ1c2VyIjp7Iml"
    "kIjoiOGEzNGVkNGItMGNlNC00YTZhLTliZjYtNjMyYTJiMDkzZGIyIiw"
    "iZG9tYWluX2lkIjoyMzQ2fX0.NgPtuQS7finXMxcqR2NAkJ2WbJLSNco"
    "IdwUTbbjfTj8"
)

TOKEN = "".join(TOKEN.split())                 # tira espaços/quebras do paste
assert len(TOKEN) == 516, f"token errado, len={len(TOKEN)}"
print("token OK, len:", len(TOKEN))

HEADERS = {"Authorization": f"Bearer {TOKEN}", "User-Agent": "fabric-ingest/1.0"}


# CELL 2 — buscar TODAS as páginas ---------------------------------------------
def fetch_all():
    todos, page = [], 1
    while True:
        r = requests.get(BASE, headers=HEADERS,
                         params={"page": page, "limit": LIMIT}, timeout=120)
        if r.status_code == 401:
            raise RuntimeError(f"401: token rejeitado. Resposta: {r.text[:300]}")
        r.raise_for_status()
        body = r.json()
        rows = body.get("data", [])
        todos.extend(rows)
        print(f"  página {page}/{body.get('totalPages','?')}  (+{len(rows)})  acumulado={len(todos)}")
        if not body.get("hasNextPage"):
            break
        page = body.get("nextPage") or page + 1
        time.sleep(0.2)
    print(f"TOTAL coletado: {len(todos)} (totalDocs: {body.get('totalDocs')})")
    return todos

registros = fetch_all()
assert registros, "API não retornou registros."


# CELL 3 — JSON -> DataFrame (converte números no Python + schema) -------------
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType)

# 1) o JSON mistura int e float; converte no Python pra createDataFrame não falhar
num_cols = ["value", "netValue", "antifraudValue", "vestiPagoValue", "vestiValue",
            "providerValue", "mdrCardBrandValue", "mdrVestiValue", "antecipationValue",
            "antecipationProviderFee", "antecipationVestiFee"]
for r in registros:
    for c in num_cols:
        if r.get(c) is not None:
            r[c] = float(r[c])
    if r.get("installments") is not None:
        r["installments"] = int(r["installments"])

# 2) schema explícito
schema = StructType([
    StructField("_id", StringType()),
    StructField("domainId", StringType()),
    StructField("companyId", StringType()),
    StructField("orderId", StringType()),
    StructField("source", StringType()),
    StructField("transactionId", StringType()),
    StructField("method", StringType()),
    StructField("paidAt", StringType()),
    StructField("value", DoubleType()),
    StructField("netValue", DoubleType()),
    StructField("antifraudValue", DoubleType()),
    StructField("vestiPagoValue", DoubleType()),
    StructField("vestiPagoProvider", StringType()),
    StructField("installments", IntegerType()),
    StructField("cardBrand", StringType()),
    StructField("vestiValue", DoubleType()),
    StructField("providerValue", DoubleType()),
    StructField("mdrCardBrandValue", DoubleType()),
    StructField("mdrVestiValue", DoubleType()),
    StructField("antecipationValue", DoubleType()),
    StructField("antecipationProvider", StringType()),
    StructField("antecipationProviderFee", DoubleType()),
    StructField("antecipationVestiFee", DoubleType()),
    StructField("createdAt", StringType()),
    StructField("updatedAt", StringType()),
])

# 3) cria o DataFrame e deriva colunas
df = spark.createDataFrame(registros, schema=schema)
df = (df
    .withColumn("paidAt_ts",    F.to_timestamp("paidAt",    "yyyy-MM-dd HH:mm:ss"))
    .withColumn("createdAt_ts", F.to_timestamp("createdAt", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updatedAt_ts", F.to_timestamp("updatedAt", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("paid_month",   F.date_format("paidAt_ts", "yyyy-MM"))
    .withColumn("ingest_em",    F.current_timestamp())
    .dropDuplicates(["_id"]))

print("linhas no lote:", df.count())
df.select("paid_month").distinct().orderBy("paid_month").show()


# CELL 4 — UPSERT (MERGE) na Delta ---------------------------------------------
from delta.tables import DeltaTable

if not spark.catalog.tableExists(TABLE):
    (df.write.format("delta")
        .partitionBy("paid_month")
        .saveAsTable(TABLE))
    print(f"Tabela {TABLE} criada com {df.count()} linhas.")
else:
    tgt = DeltaTable.forName(spark, TABLE)
    (tgt.alias("t")
        .merge(df.alias("s"), "t._id = s._id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print("MERGE concluído.")

res = spark.table(TABLE)
print("TOTAL na tabela:", res.count())
res.groupBy("paid_month").count().orderBy("paid_month").show()
