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
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               LongType, BooleanType, TimestampType, DateType)
from delta.tables import DeltaTable

INVOICES_URL = "https://raw.githubusercontent.com/vesti-mobi/Ideia-vesti/main/relatoriostarkbank/invoices.js"
# Lakehouse sem schemas custom: tabelas ficam na raiz com prefixo.
# O `dbo/` que aparece na UI eh do SQL Analytics Endpoint, nao do Spark.
PREFIX = "starkbank_"

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
        "purchase_id":          p.get("purchaseId"),
        "workspace_id":         f.get("workspaceId"),
        "transaction_id":       f.get("transactionId"),
        "order_id":             f.get("orderId"),
        "order_number":         f.get("orderNumber"),
        "company_id":           f.get("companyId"),
        "nome_fantasia":        f.get("nomeFantasia"),
        "customer_name":        f.get("customerName"),
        "order_date":           f.get("orderDate"),
        "antecipacao_enabled":  bool(f.get("antecipacaoEnabled")),
        "purchase_status":      p.get("status"),
        "amount_cents":         int(p.get("amount") or 0),
        "fee_cents":            int(p.get("fee") or 0),
        "currency_code":        p.get("currencyCode"),
        "installment_count":    int(p.get("installmentCount") or 0),
        "funding_type":         p.get("fundingType"),
        "network":              p.get("network"),
        "card_id":              p.get("cardId"),
        "card_ending":          p.get("cardEnding"),
        "holder_id":            p.get("holderId"),
        "holder_name":          p.get("holderName"),
        "holder_email":         p.get("holderEmail"),
        "holder_phone":         p.get("holderPhone"),
        "billing_city":         p.get("billingCity"),
        "billing_state_code":   p.get("billingStateCode"),
        "billing_country_code": p.get("billingCountryCode"),
        "billing_zip_code":     p.get("billingZipCode"),
        "billing_street1":      p.get("billingStreetLine1"),
        "billing_street2":      p.get("billingStreetLine2"),
        "challenge_mode":       p.get("challengeMode"),
        "challenge_url":        p.get("challengeUrl"),
        "end_to_end_id":        p.get("endToEndId"),
        "soft_descriptor":      p.get("softDescriptor"),
        "source":               p.get("source"),
        "tags":                 json.dumps(p.get("tags") or []),
        "transaction_ids":      json.dumps(p.get("transactionIds") or []),
        "metadata_json":        json.dumps(p.get("metadata") or {}),
        "created":              p.get("created"),
        "api_created":          p.get("apiCreated") or p.get("created"),
        "api_updated":          p.get("apiUpdated"),
        "gerado_em":            gerado_em,
        "snapshot_at":          snap,
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
            "nominal_due":        i.get("nominalDue"),
            "status":             i.get("status"),
            "funding_type":       i.get("fundingType"),
            "network":            i.get("network"),
            "is_protected":       bool(i.get("isProtected")),
            "tags":               json.dumps(i.get("tags") or []),
            "transaction_ids":    json.dumps(i.get("transactionIds") or []),
            "api_created":        i.get("apiCreated"),
            "api_updated":        i.get("apiUpdated"),
            "gerado_em":          gerado_em,
            "snapshot_at":        snap,
        })

# schema explicito resolve CANNOT_DETERMINE_TYPE quando colunas vem todas vazias
# (ex: holder_email ainda None ate o proximo cron regerar invoices.js).
# Campos de data ficam string aqui e viram timestamp/date via to_timestamp()
# depois, pra aceitar "" ou None sem quebrar o cast.
SCHEMA_P = StructType([
    StructField("purchase_id",          StringType()),
    StructField("workspace_id",         StringType()),
    StructField("transaction_id",       StringType()),
    StructField("order_id",             StringType()),
    StructField("order_number",         LongType()),
    StructField("company_id",           StringType()),
    StructField("nome_fantasia",        StringType()),
    StructField("customer_name",        StringType()),
    StructField("order_date",           StringType()),
    StructField("antecipacao_enabled",  BooleanType()),
    StructField("purchase_status",      StringType()),
    StructField("amount_cents",         LongType()),
    StructField("fee_cents",            LongType()),
    StructField("currency_code",        StringType()),
    StructField("installment_count",    IntegerType()),
    StructField("funding_type",         StringType()),
    StructField("network",              StringType()),
    StructField("card_id",              StringType()),
    StructField("card_ending",          StringType()),
    StructField("holder_id",            StringType()),
    StructField("holder_name",          StringType()),
    StructField("holder_email",         StringType()),
    StructField("holder_phone",         StringType()),
    StructField("billing_city",         StringType()),
    StructField("billing_state_code",   StringType()),
    StructField("billing_country_code", StringType()),
    StructField("billing_zip_code",     StringType()),
    StructField("billing_street1",      StringType()),
    StructField("billing_street2",      StringType()),
    StructField("challenge_mode",       StringType()),
    StructField("challenge_url",        StringType()),
    StructField("end_to_end_id",        StringType()),
    StructField("soft_descriptor",      StringType()),
    StructField("source",               StringType()),
    StructField("tags",                 StringType()),
    StructField("transaction_ids",      StringType()),
    StructField("metadata_json",        StringType()),
    StructField("created",              StringType()),
    StructField("api_created",          StringType()),
    StructField("api_updated",          StringType()),
    StructField("gerado_em",            StringType()),
    StructField("snapshot_at",          TimestampType()),
])
SCHEMA_I = StructType([
    StructField("installment_id",     StringType()),
    StructField("purchase_id",        StringType()),
    StructField("installment_number", IntegerType()),
    StructField("amount_cents",       LongType()),
    StructField("fee_cents",          LongType()),
    StructField("due",                StringType()),
    StructField("nominal_due",        StringType()),
    StructField("status",             StringType()),
    StructField("funding_type",       StringType()),
    StructField("network",            StringType()),
    StructField("is_protected",       BooleanType()),
    StructField("tags",               StringType()),
    StructField("transaction_ids",    StringType()),
    StructField("api_created",        StringType()),
    StructField("api_updated",        StringType()),
    StructField("gerado_em",          StringType()),
    StructField("snapshot_at",        TimestampType()),
])

# normaliza tipos das listas antes de criar DF (int vs None etc.)
def _norm(rows, schema):
    allowed = {fld.name for fld in schema.fields}
    return [{k: v for k, v in r.items() if k in allowed} for r in rows]

df_p = spark.createDataFrame(_norm(purchases,    SCHEMA_P), schema=SCHEMA_P)
df_i = spark.createDataFrame(_norm(installments, SCHEMA_I), schema=SCHEMA_I)

# casts de data/timestamp (feitos agora que o DF ja esta tipado)
# Os timestamps da API Starkbank vem em UTC (T03:00:00+00:00).
# Converte para America/Sao_Paulo (BRT) pra que CAST(due AS DATE) em queries
# no Lakehouse retorne a data correta em BRT (nao em UTC).
_BR = "America/Sao_Paulo"
def _to_brt(col):
    return F.from_utc_timestamp(F.to_timestamp(col), _BR)

df_p = (df_p
        .withColumn("created",     _to_brt("created"))
        .withColumn("api_created", _to_brt("api_created"))
        .withColumn("api_updated", _to_brt("api_updated"))
        .withColumn("order_date",  F.to_date("order_date"))
        .withColumn("gerado_em",   _to_brt("gerado_em")))
df_i = (df_i
        .withColumn("due",         _to_brt("due"))
        .withColumn("nominal_due", _to_brt("nominal_due"))
        .withColumn("api_created", _to_brt("api_created"))
        .withColumn("api_updated", _to_brt("api_updated"))
        .withColumn("gerado_em",   _to_brt("gerado_em")))

# -------- 3) tabelas ----------
# OBS: o schema `starkbank` precisa ser criado antes, pela UI do Lakehouse
# (menu "..." em Schemas > New schema). Fabric nao permite CREATE SCHEMA via spark.sql.

# snapshots (append-only, cada run adiciona uma "foto")
df_p.write.mode("append").format("delta").saveAsTable(f"{PREFIX}purchases_snapshots")
df_i.write.mode("append").format("delta").saveAsTable(f"{PREFIX}installments_snapshots")

# current (MERGE: mantem so o estado mais recente por id)
def merge_current(df, table, key):
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    tgt = DeltaTable.forName(spark, table)
    (tgt.alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

merge_current(df_p, f"{PREFIX}purchases",    "purchase_id")
merge_current(df_i, f"{PREFIX}installments", "installment_id")

print(f"[ok] snap={snap.isoformat()} geradoEm={gerado_em} "
      f"purchases={df_p.count()} installments={df_i.count()}")
